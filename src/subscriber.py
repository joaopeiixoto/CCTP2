"""Subscritor: subscreve no arranque e nas ações 'at'; uma linha de log por entrega.
Fase 3: G2 com dedup já está aqui de forma mínima (o broker ainda não retransmite)."""
import socket
import time

from config import one, many, opt, qos_of, parse_addr, ConfigError
from protocol import (EC_DELIVER, ROLE_SUBSCRIBER, ProtocolError, ec_decode,
                      open_session, request, sc_subscribe, sc_unsubscribe)
from runlog import RunLog
from topics import matches, valid_filter


class Subscriber:
    def __init__(self, ident, cfg):
        self.started = time.monotonic()        # 'at' conta a partir do arranque do processo
        self.ident = ident
        if not 0 < len(ident.encode("utf-8")) <= 16:
            raise ConfigError("--id tem de ter 1 a 16 bytes")
        self.log = RunLog(one(cfg, "log"), ident)
        self.host, self.port = parse_addr(one(cfg, "broker_control"))
        self.duration = float(one(cfg, "duration"))

        # Ações: (segundos, "sub"|"unsub", filtro, qos)
        self.actions = []
        for filt, opts in many(cfg, "subscribe"):          # subscrição do arranque
            self.actions.append((0.0, "sub", filt, qos_of(opts)))
        for t, opts in many(cfg, "at"):
            when = float(t)
            if "subscribe" in opts:
                self.actions.append((when, "sub", opts["subscribe"], qos_of(opts)))
            elif "unsubscribe" in opts:
                self.actions.append((when, "unsub", opts["unsubscribe"], None))
            else:
                raise ConfigError("ação 'at' sem subscribe= nem unsubscribe=")
        for _, _, filt, _ in self.actions:
            if not valid_filter(filt):
                raise ConfigError(f"filtro inválido: {filt}")
        self.actions.sort(key=lambda a: a[0])

        self.active = {}          # filtros ativos -> qos
        self.seen = set()         # G2: (tópico, message_id) já registados

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.bind(("", 0))
        self.udp_port = self.udp.getsockname()[1]

    def run(self):
        self.tcp, self.data_addr = open_session(self.host, self.port,
                                                ROLE_SUBSCRIBER, self.ident, self.udp_port)
        pending = list(self.actions)
        end = self.started + self.duration
        self.udp.settimeout(0.05)

        try:
            while time.monotonic() < end:
                now = time.monotonic()
                while pending and self.started + pending[0][0] <= now:
                    self._do_action(pending.pop(0))
                try:
                    data, _ = self.udp.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    continue
                self._on_deliver(data)
        finally:
            self.tcp.close()
            self.log.close()

    def _do_action(self, action):
        _, kind, filt, qos = action
        if kind == "sub":
            request(self.tcp, sc_subscribe(filt, qos))
            self.active[filt] = qos
        else:
            request(self.tcp, sc_unsubscribe(filt))
            self.active.pop(filt, None)

    def _on_deliver(self, data):
        try:
            kind, qos, pub_id, fluxo, seq, topic, _ = ec_decode(data)
        except ProtocolError:
            return
        if kind != EC_DELIVER:
            return
        # Datagramas que chegam depois de um cancelamento são ignorados.
        if not any(matches(f, topic) for f in self.active):
            return
        msg_id = f"{pub_id}-{fluxo}-{seq}"
        if qos == 2:
            key = (topic, msg_id)
            if key in self.seen:
                return                      # G2: nunca registar duas vezes
            self.seen.add(key)
        self.log.write(topic, msg_id)       # G1: registar tudo o que chega