
SEQ_MOD = 2 ** 32

RTO_INIT = 0.2      # s — primeiro prazo de retransmissão; RTT no CORE é da ordem de 1 ms
RTO_MAX = 1.0       # s — teto do backoff; um subscritor parado continua a ser sondado
WINDOW = 4096       # hop_seq na janela: ritmo (500/s) x recuperação de 5 perdas seguidas (~3.4 s)


class _Pending:
    __slots__ = ("pkt", "rto", "deadline", "tag")

    def __init__(self, pkt, now, tag):
        self.pkt = pkt
        self.rto = RTO_INIT
        self.deadline = now + RTO_INIT
        self.tag = tag


class SendWindow:
    def __init__(self, size=WINDOW):
        self.size = size
        self.next_seq = 0            # próximo hop_seq a atribuir
        # hop_seq -> _Pending. Os hop_seq são inseridos por ordem crescente e uma
        # retransmissão não reinsere, por isso a primeira chave é sempre a base.
        self.pending = {}

    def __len__(self):
        return len(self.pending)

    @property
    def base(self):
        #hop_seq mais antigo por confirmar (ou next_seq, se não há nenhum)
        return next(iter(self.pending), self.next_seq)

    def has_room(self):
        #O próximo hop_seq cabe em [base, base + size)?
        return (self.next_seq - self.base) % SEQ_MOD < self.size

    def add(self, build, now, tag=None):
        #Atribui o próximo hop_seq, constrói o datagrama com build(hop_seq) e guarda-o como pendente e devolve o datagrama, pronto a enviar.
        hop = self.next_seq
        self.next_seq = (hop + 1) % SEQ_MOD
        pkt = build(hop)
        self.pending[hop] = _Pending(pkt, now, tag)
        return pkt

    def ack(self, hop_seq):
        """Retira o pendente confirmado (a base desliza sozinha, se era ela).
        Devolve True se existia; ACKs duplicados ou atrasados não fazem nada."""
        return self.pending.pop(hop_seq, None) is not None

    def expired(self, now):
        #Datagramas cujo temporizador expirou, já com o novo prazo, que é o backoff
        out = []
        for p in self.pending.values():
            if p.deadline <= now:
                p.rto = min(p.rto * 2, RTO_MAX)
                p.deadline = now + p.rto
                out.append(p.pkt)
        return out

    def discard(self, predicate):
        #Esquece os pendentes cuja tag satisfaz predicate, por exemplo subscrição cancelada
        for hop in [h for h, p in self.pending.items() if predicate(p.tag)]:
            del self.pending[hop]

    def clear(self):
        self.pending.clear()