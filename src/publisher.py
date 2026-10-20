"""Publicador: gera publicações sintéticas nos fluxos da configuração.
Fase 2: aqui passa a esperar por ACKs até 'drain' (retransmissão)."""
import random
import socket
import threading
import time

from config import one, many, opt, qos_of, parse_addr, ConfigError
from protocol import EC_PUBLISH, ROLE_PUBLISHER, ec_encode, open_session
from runlog import RunLog
from topics import valid_topic


class Publisher:
    def __init__(self, ident, cfg):
        self.ident = ident
        self.log = RunLog(one(cfg, "log"), ident)
        self.host, self.port = parse_addr(one(cfg, "broker_control"))
        self.start_after = float(one(cfg, "start_after"))
        self.drain = float(one(cfg, "drain"))

        self.flows = []   # (tópico, qos, count, rate)
        for topic, opts in many(cfg, "publish"):
            if not valid_topic(topic):
                raise ConfigError(f"tópico inválido: {topic}")
            self.flows.append((topic, qos_of(opts),
                               int(opt(opts, "count")), float(opt(opts, "rate"))))

    def run(self):
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.tcp, self.data_addr = open_session(self.host, self.port,
                                                ROLE_PUBLISHER, self.ident, 0)
        time.sleep(self.start_after)   # os subscritores estabelecem a sessão em paralelo

        threads = [threading.Thread(target=self._flow, args=(i, *f))
                   for i, f in enumerate(self.flows)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Fase 1: sem confirmações, não há nada a esperar.
        self.tcp.close()
        self.log.close()

    def _flow(self, idx, topic, qos, count, rate):
        interval = 1.0 / rate
        next_at = time.monotonic()
        for n in range(count):
            msg_id = f"{self.ident}-{idx}-{n}"
            payload = f"{random.uniform(0, 100):.2f}".encode()
            self.udp.sendto(ec_encode(EC_PUBLISH, qos, msg_id, topic, payload), self.data_addr)
            self.log.write(topic, msg_id)
            next_at += interval
            time.sleep(max(0.0, next_at - time.monotonic()))