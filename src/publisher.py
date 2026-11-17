
import random
import socket
import threading
import time

from config import ConfigError, many, number, one, opt, parse_addr, qos_of
from protocol import (EC_PUBLISH, ROLE_PUBLISHER, Ack, ProtocolError, SEQ_MOD,
                      ec_decode, ec_encode, message_id, open_session)
from reliability import SendWindow
from runlog import RunLog
from topics import valid_topic

IO_TICK = 0.02      # s — período máximo entre verificações de temporizadores


class Publisher:
    def __init__(self, ident, cfg):
        self.ident = ident
        self.host, self.port = parse_addr(one(cfg, "broker_control"))
        self.start_after = number(one(cfg, "start_after"), "start_after")
        self.drain = number(one(cfg, "drain"), "drain")

        self.flows = []   # (tópico, qos, count, rate)
        for topic, opts in many(cfg, "publish"):
            if not valid_topic(topic):
                raise ConfigError(f"tópico inválido: {topic}")
            try:
                count = int(opt(opts, "count"))
            except ValueError:
                raise ConfigError(f"count inválido: {topic}")
            rate = number(opt(opts, "rate"), "rate")
            if not 0 <= count <= SEQ_MOD or rate <= 0:
                raise ConfigError(f"count/rate inválidos: {topic}")
            self.flows.append((topic, qos_of(opts), count, rate))
        if len(self.flows) > 0x10000:
            raise ConfigError("mais fluxos do que cabem no campo fluxo (uint16)")

        self.log = RunLog(one(cfg, "log"), ident)

        self.window = SendWindow()
        self.cond = threading.Condition()     # protege a janela; acorda quem espera por espaço
        self.stop = threading.Event()

        # Socket UDP aberto antes do HELLO: a porta vai no HELLO e é para lá que vêm os ACKs.
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("", 0))
        self.udp_port = self.udp.getsockname()[1]

    def run(self):
        self.tcp, self.data_addr = open_session(self.host, self.port, ROLE_PUBLISHER,
                                                self.ident, self.udp_port)
        io = threading.Thread(target=self._io_loop, daemon=True)
        try:
            io.start()
            time.sleep(self.start_after)   # os subscritores estabelecem a sessão em paralelo

            threads = [threading.Thread(target=self._flow, args=(i, *f), daemon=True)
                       for i, f in enumerate(self.flows)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # drain: continuar a retransmitir até tudo confirmado, no máximo 'drain' segundos.
            deadline = time.monotonic() + self.drain
            with self.cond:
                while len(self.window) and time.monotonic() < deadline:
                    self.cond.wait(min(IO_TICK, max(0.0, deadline - time.monotonic())))
        finally:
            self.stop.set()
            if io.is_alive():
                io.join(timeout=1.0)
            self.tcp.close()
            self.log.close()

    def _flow(self, idx, topic, qos, count, rate):
        interval = 1.0 / rate
        next_at = time.monotonic()
        for n in range(count):
            payload = f"{random.uniform(0, 100):.2f}".encode()
            with self.cond:
                while not self.window.has_room():          # janela cheia: espera por ACKs
                    self.cond.wait(IO_TICK)
                pkt = self.window.add(
                    lambda hop: ec_encode(EC_PUBLISH, qos, hop, self.ident, idx, n, topic, payload),
                    time.monotonic())
            # A linha de registo é escrita uma vez, na primeira transmissão, antes do envio.
            self.log.write(topic, message_id(self.ident, idx, n))
            self._send(pkt)
            # Ritmo: um atraso (janela cheia) não é compensado com uma rajada.
            next_at = max(next_at + interval, time.monotonic())
            time.sleep(max(0.0, next_at - time.monotonic()))

    def _io_loop(self):
        """Recebe ACKs e retransmite o que expirou."""
        self.udp.settimeout(IO_TICK)
        while not self.stop.is_set():
            try:
                data, addr = self.udp.recvfrom(65535)
            except socket.timeout:
                data = None
            except OSError:
                data = None
            if data is not None and addr == self.data_addr:
                try:
                    msg = ec_decode(data)
                except ProtocolError:
                    msg = None                      # datagrama inválido: descarta
                if isinstance(msg, Ack):
                    with self.cond:
                        if self.window.ack(msg.hop_seq):
                            self.cond.notify_all()
            with self.cond:
                due = self.window.expired(time.monotonic())
            for pkt in due:
                self._send(pkt)

    def _send(self, pkt):
        try:
            self.udp.sendto(pkt, self.data_addr)
        except OSError:
            pass          # ex.: rede ainda a convergir; o temporizador trata como perda