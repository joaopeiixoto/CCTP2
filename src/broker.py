"""Broker, fase 1: um broker, SubControl em TCP, EventCast em UDP.
Pontos de extensão: fase 2 (eviction, fiabilidade por subscritor),
fase 3 (G2 e fan-out com estado por subscritor), fase 4 (peers)."""
import socket
import threading
import time

from config import one
from protocol import (
    EC_DELIVER, EC_PUBLISH, FILTER_LEN, ROLE_PUBLISHER, ROLE_SUBSCRIBER,
    SC_ACK, SC_HELLO, SC_HELLO_ACK, SC_SUBSCRIBE, SC_UNSUBSCRIBE,
    ProtocolError, Reader, decode_fixed, decode_filter, ec_decode, ec_encode, recv_frame, send_frame,
)
from topics import matches, valid_filter


class Session:
    """Estado de um cliente ligado por SubControl."""

    def __init__(self, sock, role, ident, udp_addr):
        self.sock = sock
        self.role = role
        self.ident = ident
        self.udp_addr = udp_addr      # (ip do TCP, porta de dados do cliente)
        self.filters = {}             # filtro -> qos


class Broker:
    def __init__(self, _ident, cfg):
        bind = one(cfg, "bind")
        self.control_port = int(one(cfg, "control_port"))
        self.data_port = int(one(cfg, "data_port"))
        self.duration = float(one(cfg, "duration"))
        self.evict_after = float(one(cfg, "evict_after"))

        self.lock = threading.Lock()
        self.sessions = {}            # socket TCP -> Session

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind((bind, self.data_port))

        self.tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp.bind((bind, self.control_port))
        self.tcp.listen(64)

    def run(self):
        threading.Thread(target=self._accept_loop, daemon=True).start()
        self.udp.settimeout(0.2)
        deadline = time.monotonic() + self.duration
        while time.monotonic() < deadline:
            try:
                data, _ = self.udp.recvfrom(65535)
            except socket.timeout:
                continue
            self._route(data)

    # ----- plano de controlo (TCP) -----

    def _accept_loop(self):
        while True:
            conn, _ = self.tcp.accept()
            threading.Thread(target=self._serve_client, args=(conn,), daemon=True).start()

    def _serve_client(self, conn):
        session = None
        try:
            while True:
                r = Reader(recv_frame(conn))
                mtype = r.u8()

                if mtype == SC_HELLO:
                    role = r.u8()
                    udp_port = r.u16()
                    ident = decode_fixed(r.take(16))
                    if role not in (ROLE_PUBLISHER, ROLE_SUBSCRIBER):
                        raise ProtocolError("papel desconhecido")
                    session = Session(conn, role, ident, (conn.getpeername()[0], udp_port))
                    with self.lock:
                        self.sessions[conn] = session
                    send_frame(conn, bytes([SC_HELLO_ACK]) + self.data_port.to_bytes(2, "big"))
                    continue

                if session is None:
                    raise ProtocolError("HELLO em falta")

                if mtype == SC_SUBSCRIBE:
                    qos = r.u8()
                    filt = decode_filter(r.take(FILTER_LEN))
                    if qos not in (1, 2) or not valid_filter(filt):
                        raise ProtocolError("subscrição inválida")
                    with self.lock:
                        session.filters[filt] = qos
                elif mtype == SC_UNSUBSCRIBE:
                    filt = decode_filter(r.take(FILTER_LEN))
                    with self.lock:
                        session.filters.pop(filt, None)
                else:
                    raise ProtocolError("tipo desconhecido")

                send_frame(conn, bytes([SC_ACK]))
        except (OSError, ProtocolError, ValueError):
            pass   # sessão caída ou mensagem inválida: fecha
        finally:
            with self.lock:
                self.sessions.pop(conn, None)
            conn.close()

    # ----- plano de dados (UDP) -----

    def _route(self, data):
        try:
            kind, qos, pub_id, fluxo, seq, topic, payload = ec_decode(data)
        except ProtocolError:
            return                                  # cabeçalho inválido: descarta
        if kind != EC_PUBLISH or qos not in (1, 2):
            return

        with self.lock:
            snapshot = [(s, dict(s.filters)) for s in self.sessions.values()
                        if s.role == ROLE_SUBSCRIBER]

        for session, filters in snapshot:
            matching = [q for f, q in filters.items() if matches(f, topic)]
            if not matching:
                continue
            # Uma cópia por subscritor, mesmo com filtros sobrepostos.
            # Qos da entrega: o mais forte dos filtros, sem passar do qos do fluxo.
            deliver_qos = min(qos, max(matching))
            self.udp.sendto(ec_encode(EC_DELIVER, deliver_qos, pub_id, fluxo, seq, topic, payload),
                            session.udp_addr)