"""Subscritor: subscreve no arranque e nas ações 'at'; confirma cada DELIVER e escreve
uma linha de registo por entrega. Em G1 regista tudo o que chega (duplicados incluídos);
em G2 consulta a tabela de duplicados."""
import select
import socket
import time

from config import ConfigError, many, number, one, parse_addr, qos_of
from protocol import (EC_DELIVER, ROLE_SUBSCRIBER, Data, ProtocolError, ec_ack, ec_decode,
                      open_session, request, sc_subscribe, sc_unsubscribe)
from runlog import RunLog
from topics import covered, valid_filter

LOOP_TICK = 0.05      # s — granularidade das ações 'at' e do fim por 'duration'
RCVBUF = 4 * 1024 * 1024   # aguenta o que chega enquanto o processo está parado (SIGSTOP)


class Subscriber:
    def __init__(self, ident, cfg):
        self.started = time.monotonic()        # 'at' conta a partir do arranque do processo
        self.ident = ident
        self.host, self.port = parse_addr(one(cfg, "broker_control"))
        self.duration = number(one(cfg, "duration"), "duration")

        # Ações: (segundos, "sub"|"unsub", filtro, qos)
        self.actions = []
        for filt, opts in many(cfg, "subscribe"):          # subscrição do arranque
            self.actions.append((0.0, "sub", filt, qos_of(opts)))
        for t, opts in many(cfg, "at"):
            when = number(t, "at")
            if "subscribe" in opts:
                self.actions.append((when, "sub", opts["subscribe"], qos_of(opts)))
            elif "unsubscribe" in opts:
                self.actions.append((when, "unsub", opts["unsubscribe"], None))
            else:
                raise ConfigError("ação 'at' sem subscribe= nem unsubscribe=")
        for _, _, filt, _ in self.actions:
            if not valid_filter(filt):
                raise ConfigError(f"filtro inválido: {filt}")
        self.actions.sort(key=lambda a: a[0])               # estável: mantém a ordem do ficheiro

        self.log = RunLog(one(cfg, "log"), ident)

        self.active = {}          # filtros ativos -> qos
        self.seen = set()         # G2: (tópico, message_id) já registados
        self.session_open = False

        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF)
        except OSError:
            pass                  # o kernel limita ao rmem_max; não é fatal
        self.udp.bind(("", 0))
        self.udp_port = self.udp.getsockname()[1]

    def run(self):
        self.tcp, self.data_addr = open_session(self.host, self.port,
                                                ROLE_SUBSCRIBER, self.ident, self.udp_port)
        self.session_open = True
        pending = list(self.actions)
        end = self.started + self.duration
        self.udp.setblocking(False)

        try:
            while True:
                now = time.monotonic()
                if now >= end:
                    break
                while self.session_open and pending and self.started + pending[0][0] <= now:
                    self._do_action(pending.pop(0))

                watch = [self.udp] + ([self.tcp] if self.session_open else [])
                timeout = min(LOOP_TICK, max(0.0, end - now))
                readable, _, _ = select.select(watch, [], [], timeout)
                # O TCP primeiro: se o broker nos expulsou, nada do que está no buffer UDP
                # é entregue — a sessão já fechou.
                if self.tcp in readable:
                    self._on_control_readable()
                if self.udp in readable:
                    self._drain_udp()
        finally:
            self.tcp.close()
            self.log.close()

    # ----- plano de controlo -----

    def _do_action(self, action):
        _, kind, filt, qos = action
        try:
            if kind == "sub":
                request(self.tcp, sc_subscribe(filt, qos))
                self.active[filt] = qos
            else:
                request(self.tcp, sc_unsubscribe(filt))
                self.active.pop(filt, None)
        except (OSError, ProtocolError):
            self._session_closed()

    def _on_control_readable(self):
        """Fora de um pedido o broker não envia nada: o TCP ficar legível significa que
        fechou (recv devolve b"") ou que o broker violou o protocolo. Em ambos os casos
        a sessão acabou."""
        self._session_closed()

    def _session_closed(self):
        """Expulso, ou broker terminou: deixa de confirmar e de entregar. O processo
        continua até 'duration', como manda o contrato, mas já sem sessão."""
        self.session_open = False
        self.active.clear()

    # ----- plano de dados -----

    def _drain_udp(self):
        while True:
            try:
                data, addr = self.udp.recvfrom(65535)
            except (BlockingIOError, InterruptedError):
                return
            except OSError:
                return
            if self.session_open and addr == self.data_addr:
                self._on_deliver(data)

    def _on_deliver(self, data):
        try:
            msg = ec_decode(data)
        except ProtocolError:
            return                              # inválido ou truncado: descarta, sem ACK
        if not isinstance(msg, Data) or msg.kind != EC_DELIVER:
            return
        # Confirma tudo o que chega bem formado — duplicados e tópicos já cancelados
        # incluídos. É o ACK que pára as retransmissões do broker.
        try:
            self.udp.sendto(ec_ack(msg.hop_seq), self.data_addr)
        except OSError:
            pass                                # perde-se como qualquer datagrama
        # Em trânsito no momento do cancelamento: confirmado, mas não entregue.
        if not covered(self.active, msg.topic):
            return
        if msg.qos == 2:                        # a linha que distingue G1 de G2
            key = (msg.topic, msg.msg_id)
            if key in self.seen:
                return
            self.seen.add(key)
        self.log.write(msg.topic, msg.msg_id)