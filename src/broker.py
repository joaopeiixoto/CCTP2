
import socket
import threading
import time
from collections import deque

from config import many, number, one, parse_addr, port
from protocol import (
    EC_DELIVER, EC_PUBLISH, ROLE_PUBLISHER, ROLE_SUBSCRIBER,
    FILTER_LEN, ID_LEN, SC_HELLO, SC_SUBSCRIBE, SC_UNSUBSCRIBE, Ack, Data, ProtocolError,
    Reader, decode_filter, decode_fixed, ec_ack, ec_decode, ec_encode, recv_frame, sc_ack,
    sc_hello_ack, send_frame,
)
from reliability import SendWindow
from topics import covered, matches, valid_filter

TIMER_TICK = 0.02     # s — retransmissão e expulsão verificadas a cada 20 ms


class Session:
    #Estado de um cliente ligado por SubControl.

    def __init__(self, sock, role, ident, udp_addr, now):
        self.sock = sock
        self.role = role
        self.ident = ident
        self.udp_addr = udp_addr      # (ip da ligação TCP, porta UDP anunciada no HELLO)
        self.alive = True
        # Só para subscritores:
        self.filters = {}             # filtro -> qos
        self.window = SendWindow()    # DELIVER enviados e por confirmar (hop_seq próprio)
        self.queue = deque()          # entregas à espera de lugar na janela
        self.last_ack = now           # instante do último ACK recebido
        self.pending_since = None     # desde quando há algo por entregar (None: nada)

    def has_pending(self):
        return bool(self.queue) or len(self.window) > 0


class Broker:
    def __init__(self, _ident, cfg):
        bind = one(cfg, "bind")
        self.control_port = port(one(cfg, "control_port"), "control_port")
        self.data_port = port(one(cfg, "data_port"), "data_port")
        self.duration = number(one(cfg, "duration"), "duration")
        self.evict_after = number(one(cfg, "evict_after"), "evict_after")
        self.peers = [parse_addr(p) for p, _ in many(cfg, "peer")]   # fase 4: ainda não usados

        self.lock = threading.Lock()
        self.sessions = {}            # socket TCP -> Session
        self.by_addr = {}             # endereço UDP do cliente -> Session

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((bind, self.data_port))

        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind((bind, self.control_port))
        self.tcp.listen(64)

    def run(self):
        deadline = time.monotonic() + self.duration
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._timer_loop, daemon=True).start()
        self.udp.settimeout(0.2)
        while time.monotonic() < deadline:
            try:
                data, addr = self.udp.recvfrom(65535)
            except OSError:          # inclui socket.timeout
                continue
            self._on_datagram(data, addr)

    # TCP

    def _accept_loop(self):
        while True:
            try:
                conn, _ = self.tcp.accept()
            except OSError:
                time.sleep(0.05)                    # ex.: descritores esgotados; não matar o accept
                continue
            threading.Thread(target=self._serve_client, args=(conn,), daemon=True).start()

    def _serve_client(self, conn):
        session = None
        try:
            while True:
                r = Reader(recv_frame(conn))
                mtype = r.u8()

                if mtype == SC_HELLO:
                    if session is not None:
                        raise ProtocolError("HELLO repetido")
                    role, udp_port, ident = r.u8(), r.u16(), decode_fixed(r.take(ID_LEN))
                    r.end()
                    if role not in (ROLE_PUBLISHER, ROLE_SUBSCRIBER) or udp_port == 0 or not ident:
                        raise ProtocolError("HELLO inválido")
                    addr = (conn.getpeername()[0], udp_port)
                    session = Session(conn, role, ident, addr, time.monotonic())
                    with self.lock:
                        self.sessions[conn] = session
                        self.by_addr[addr] = session
                    # Responde antes de qualquer datagrama: só depois deste HELLO_ACK o cliente
                    # conhece a porta de dados.
                    send_frame(conn, sc_hello_ack(self.data_port))
                    continue

                if session is None or session.role != ROLE_SUBSCRIBER:
                    raise ProtocolError("pedido fora de sessão ou de papel errado")

                if mtype == SC_SUBSCRIBE:
                    qos, filt = r.u8(), decode_filter(r.take(FILTER_LEN))
                    r.end()
                    if qos not in (1, 2) or not valid_filter(filt):
                        raise ProtocolError("subscrição inválida")
                    with self.lock:
                        session.filters[filt] = qos
                elif mtype == SC_UNSUBSCRIBE:
                    filt = decode_filter(r.take(FILTER_LEN))
                    r.end()
                    with self.lock:
                        session.filters.pop(filt, None)
                        self._purge_uncovered(session, time.monotonic())
                else:
                    raise ProtocolError("tipo inesperado vindo de um cliente")

                send_frame(conn, sc_ack())
        except (OSError, ProtocolError, ValueError):
            pass   # sessão terminada pelo cliente, expulsa, ou mensagem inválida: fecha
        finally:
            with self.lock:
                self._forget(conn)
            conn.close()

    def _forget(self, conn):
        #Retira a sessão do estado e descarta o que tinha em fila. Idempotente; com lock.
        session = self.sessions.pop(conn, None)
        if session is None:
            return None
        session.alive = False
        if self.by_addr.get(session.udp_addr) is session:
            del self.by_addr[session.udp_addr]
        session.queue.clear()
        session.window.clear()
        return session

    def _purge_uncovered(self, session, now):
        #Após um cancelamento, nada do que já não corresponde a um filtro volta a sair:
        #nem o que está em fila, nem retransmissões do que estava em trânsito.
        keep = lambda topic: covered(session.filters, topic)
        session.queue = deque(item for item in session.queue if keep(item[-2]))
        session.window.discard(lambda topic: not keep(topic))
        if not session.has_pending():
            session.pending_since = None

    # plano de dados (UDP) 

    def _on_datagram(self, data, addr):
        try:
            msg = ec_decode(data)
        except ProtocolError:
            return # inválido ou truncado é descartado
        out = []
        now = time.monotonic()
        with self.lock:
            session = self.by_addr.get(addr)
            if session is None:
                return        # sem sessão SubControl: ignora
            if isinstance(msg, Data) and msg.kind == EC_PUBLISH and session.role == ROLE_PUBLISHER:
                # Em G1 não se deduplica- uma retransmissão do publicador é distribuída outra vez.
                out.append((ec_ack(msg.hop_seq), addr))
                self._fan_out(msg, now, out)
            elif isinstance(msg, Ack) and session.role == ROLE_SUBSCRIBER:
                session.last_ack = now
                session.window.ack(msg.hop_seq)
                self._fill_window(session, now, out)
            self._send_all(out)

    def _fan_out(self, msg, now, out):
        for s in self.by_addr.values():
            if s.role != ROLE_SUBSCRIBER:
                continue
            matching = [q for f, q in s.filters.items() if matches(f, msg.topic)]
            if not matching:
                continue
            # Uma cópia por subscritor, mesmo com filtros sobrepostos.
            qos = min(msg.qos, max(matching))
            if not s.has_pending():
                s.pending_since = now
            s.queue.append((qos, msg.pub_id, msg.fluxo, msg.seq, msg.topic, msg.payload))
            self._fill_window(s, now, out)

    def _fill_window(self, s, now, out):
        #Passa entregas da fila para a janela enquanto houver lugar
        while s.queue and s.window.has_room():
            qos, pub_id, fluxo, seq, topic, payload = s.queue.popleft()
            pkt = s.window.add(
                lambda hop: ec_encode(EC_DELIVER, qos, hop, pub_id, fluxo, seq, topic, payload),
                now, tag=topic)
            out.append((pkt, s.udp_addr))
        if not s.has_pending():
            s.pending_since = None

    # ----- temporizadores: retransmissão e expulsão -----

    def _timer_loop(self):
        while True:
            time.sleep(TIMER_TICK)
            out, evicted = [], []
            now = time.monotonic()
            with self.lock:
                for conn, s in list(self.sessions.items()):
                    if s.role != ROLE_SUBSCRIBER or not s.has_pending():
                        continue
                    # evict_after conta desde o último ACK ou desde que passou a haver
                    # algo por entregar, o que for mais recente.
                    silent_since = max(s.last_ack, s.pending_since)
                    if now - silent_since >= self.evict_after:
                        self._forget(conn)       # descarta fila e pendentes: nada mais sai
                        evicted.append(conn)
                        continue
                    out.extend((pkt, s.udp_addr) for pkt in s.window.expired(now))
                self._send_all(out)
            for conn in evicted:
                # Fecha a sessão TCP (FIN). A thread do cliente acorda do recv e fecha o socket.
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def _send_all(self, out):
        for pkt, addr in out:
            try:
                self.udp.sendto(pkt, addr)
            except OSError:
                pass        # destino inalcançável: conta como perda; o temporizador trata