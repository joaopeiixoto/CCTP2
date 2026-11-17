"""Codificação dos dois protocolos, byte a byte (sem bibliotecas de serialização).
Todos os inteiros são big-endian. Os textos são UTF-8 em campos de tamanho fixo,
preenchidos com 0x00 à direita.

SubControl (TCP), cada mensagem é um quadro:
    comprimento(1B) | tipo(1B) | campos...

EventCast (UDP), cabeçalho fixo de 78 bytes:
    PUBLISH/DELIVER: kind(1B) | qos(1B) | hop_seq(4B) | pub_id(16B) | fluxo(2B) | seq(4B)
                     | topico(48B) | tam_payload(2B) | payload
    ACK (5 bytes):   kind(1B) | hop_seq(4B)

Dois identificadores com dois âmbitos (secção 3.5 do enunciado):
    hop_seq            — número da transmissão nesta ligação; é o que a janela usa e o ACK
                         confirma. Cada emissor tem o seu (publicador; broker por subscritor).
    pub_id/fluxo/seq   — identificam a publicação de ponta a ponta (message_id);
                         o broker copia-os sem os alterar.

Tópicos e filtros viajam sem barras: três campos de 16 bytes, regiao | estacao | metrica.
Num filtro, um campo vazio (só 0x00) é o '*'.
"""
import socket
import time
from typing import NamedTuple

from topics import LEVEL_MAX, valid_filter, valid_topic


class ProtocolError(Exception):
    """Mensagem mal formada ou cabeçalho truncado."""


SEQ_MOD = 2 ** 32             # hop_seq e seq são uint32
ID_LEN = 16                   # --id / pub_id
TOPIC_LEN = 3 * LEVEL_MAX     # 48: regiao(16) estacao(16) metrica(16)
FILTER_LEN = TOPIC_LEN


# ---------- texto em campos fixos ----------

def encode_fixed(text: str, length: int) -> bytes:
    """UTF-8 com tamanho fixo, preenchido com 0x00 à direita."""
    raw = text.encode("utf-8")
    if len(raw) > length:
        raise ValueError(f"campo excede {length} bytes: {text}")
    return raw.ljust(length, b"\x00")


def decode_fixed(raw: bytes) -> str:
    """Retira o preenchimento 0x00 à direita e descodifica UTF-8. Um 0x00 no meio do
    texto não é preenchimento e torna o campo inválido."""
    text = raw.rstrip(b"\x00")
    if b"\x00" in text:
        raise ProtocolError("0x00 no meio de um campo de texto")
    try:
        return text.decode("utf-8")
    except UnicodeDecodeError:
        raise ProtocolError("texto inválido (não é UTF-8)")


def _levels(raw: bytes):
    return [decode_fixed(raw[i:i + LEVEL_MAX]) for i in range(0, TOPIC_LEN, LEVEL_MAX)]


def encode_topic(topic: str) -> bytes:
    """'north/station7/temperature' -> 48 bytes. As barras não viajam: separam os campos."""
    parts = topic.split("/")
    if len(parts) != 3:
        raise ValueError(f"tópico inválido (requer 3 níveis): {topic}")
    return b"".join(encode_fixed(p, LEVEL_MAX) for p in parts)


def decode_topic(raw: bytes) -> str:
    """48 bytes -> 'regiao/estacao/metrica'. Os três níveis são obrigatórios."""
    if len(raw) != TOPIC_LEN:
        raise ProtocolError("tópico com tamanho inválido")
    topic = "/".join(_levels(raw))
    if not valid_topic(topic):
        raise ProtocolError("tópico inválido")
    return topic


def encode_filter(filt: str) -> bytes:
    """Filtro -> 48 bytes. Cada '*' (só pode estar no fim) vira campos vazios:
    'north/*' -> north | 0x00*16 | 0x00*16;  'north/station7/*' -> north | station7 | 0x00*16."""
    parts = filt.split("/")
    if parts[-1] == "*":
        parts = parts[:-1]
    if not 1 <= len(parts) <= 3:
        raise ValueError(f"filtro inválido: {filt}")
    parts += [""] * (3 - len(parts))
    return b"".join(encode_fixed(p, LEVEL_MAX) for p in parts)


def decode_filter(raw: bytes) -> str:
    """48 bytes -> filtro canónico. Depois de um campo vazio, os seguintes também têm de
    estar vazios: 'north | 0x00 | temperature' (o '*' no meio) é rejeitado."""
    if len(raw) != FILTER_LEN:
        raise ProtocolError("filtro com tamanho inválido")
    reg, est, met = _levels(raw)
    if not est and met:
        raise ProtocolError("filtro com '*' no meio")
    if not est:
        filt = f"{reg}/*"
    elif not met:
        filt = f"{reg}/{est}/*"
    else:
        filt = f"{reg}/{est}/{met}"
    if not valid_filter(filt):
        raise ProtocolError("filtro inválido")
    return filt


class Reader:
    """Lê campos com verificação de limites: um cabeçalho truncado vira ProtocolError."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise ProtocolError("cabeçalho truncado")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return int.from_bytes(self.take(2), "big")

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "big")

    def end(self):
        """Bytes a mais depois do último campo também são erro."""
        if self.pos != len(self.data):
            raise ProtocolError("bytes a mais no fim da mensagem")


# ---------- SubControl (TCP): enquadramento ----------

SC_HELLO = 1          # tipo(1B) | role(1B) | udp_port(2B) | id(16B)
SC_HELLO_ACK = 2      # tipo(1B) | data_port(2B)
SC_SUBSCRIBE = 3      # tipo(1B) | qos(1B) | filtro(48B)
SC_UNSUBSCRIBE = 4    # tipo(1B) | filtro(48B)
SC_ACK = 5            # tipo(1B)

# Cada tipo tem um tamanho fixo: o comprimento do quadro tem de bater certo com o tipo.
SC_SIZE = {SC_HELLO: 20, SC_HELLO_ACK: 3, SC_SUBSCRIBE: 50, SC_UNSUBSCRIBE: 49, SC_ACK: 1}

ROLE_PUBLISHER = 1
ROLE_SUBSCRIBER = 2


def recv_exact(sock, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("ligação fechada pelo peer")
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock, body: bytes):
    if not 1 <= len(body) <= 255:
        raise ValueError("quadro com tamanho inválido")
    sock.sendall(bytes([len(body)]) + body)


def recv_frame(sock) -> bytes:
    n = recv_exact(sock, 1)[0]
    if n < 1:
        raise ProtocolError("comprimento inválido")
    body = recv_exact(sock, n)
    if SC_SIZE.get(body[0]) != n:
        raise ProtocolError("tipo desconhecido ou comprimento incompatível")
    return body


# ---------- SubControl: construção ----------

def sc_hello(role: int, udp_port: int, ident: str) -> bytes:
    return bytes([SC_HELLO, role]) + udp_port.to_bytes(2, "big") + encode_fixed(ident, ID_LEN)


def sc_hello_ack(data_port: int) -> bytes:
    return bytes([SC_HELLO_ACK]) + data_port.to_bytes(2, "big")


def sc_subscribe(filt: str, qos: int) -> bytes:
    return bytes([SC_SUBSCRIBE, qos]) + encode_filter(filt)


def sc_unsubscribe(filt: str) -> bytes:
    return bytes([SC_UNSUBSCRIBE]) + encode_filter(filt)


def sc_ack() -> bytes:
    return bytes([SC_ACK])


# ---------- SubControl: lado do cliente ----------

CONNECT_RETRY_S = 10.0     # os três papéis arrancam em simultâneo: o broker pode ainda não escutar
CONNECT_STEP_S = 0.1
SC_REPLY_TIMEOUT_S = 5.0   # um pedido SubControl sem resposta neste prazo dá a sessão por perdida


def request(sock, body: bytes):
    """Envia um pedido e espera o ACK. Timeout ou outra resposta é erro."""
    sock.settimeout(SC_REPLY_TIMEOUT_S)
    try:
        send_frame(sock, body)
        resp = recv_frame(sock)
    finally:
        sock.settimeout(None)
    if resp != sc_ack():
        raise ProtocolError("resposta inesperada (esperava ACK)")


def connect_with_retry(host: str, port: int):
    """Liga ao broker, repetindo enquanto a ligação for recusada ou o destino inalcançável."""
    deadline = time.monotonic() + CONNECT_RETRY_S
    while True:
        try:
            return socket.create_connection((host, port), timeout=SC_REPLY_TIMEOUT_S)
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(CONNECT_STEP_S)


def open_session(host: str, port: int, role: int, ident: str, udp_port: int):
    """Abre a sessão SubControl e devolve (socket TCP, endereço do plano de dados do broker).
    Nenhum datagrama é enviado antes de este HELLO_ACK chegar."""
    sock = connect_with_retry(host, port)
    try:
        send_frame(sock, sc_hello(role, udp_port, ident))
        r = Reader(recv_frame(sock))
        if r.u8() != SC_HELLO_ACK:
            raise ProtocolError("esperava HELLO_ACK")
        data_port = r.u16()
        r.end()
    except BaseException:
        sock.close()
        raise
    sock.settimeout(None)
    return sock, (host, data_port)


# ---------- EventCast (UDP) ----------

EC_PUBLISH = 1
EC_DELIVER = 2
EC_ACK = 3

EC_HEADER_LEN = 78            # 1+1+4+16+2+4+48+2
EC_ACK_LEN = 5                # 1+4
MAX_PAYLOAD = 1394            # 1500 (MTU) - 20 (IP) - 8 (UDP) - 78: nenhum datagrama é fragmentado


class Data(NamedTuple):
    """Um PUBLISH ou DELIVER descodificado."""
    kind: int
    qos: int
    hop_seq: int
    pub_id: str
    fluxo: int
    seq: int
    topic: str
    payload: bytes

    @property
    def msg_id(self) -> str:
        return message_id(self.pub_id, self.fluxo, self.seq)


class Ack(NamedTuple):
    hop_seq: int


def message_id(pub_id: str, fluxo: int, seq: int) -> str:
    """Identificador de ponta a ponta, igual no registo do publicador e no do subscritor."""
    return f"{pub_id}-{fluxo}-{seq}"


def ec_encode(kind: int, qos: int, hop_seq: int, pub_id: str, fluxo: int, seq: int,
              topic: str, payload: bytes) -> bytes:
    """PUBLISH ou DELIVER: 78 bytes de cabeçalho + payload."""
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(f"payload com mais de {MAX_PAYLOAD} bytes")
    return (bytes([kind, qos]) +
            hop_seq.to_bytes(4, "big") +
            encode_fixed(pub_id, ID_LEN) +
            fluxo.to_bytes(2, "big") +
            seq.to_bytes(4, "big") +
            encode_topic(topic) +
            len(payload).to_bytes(2, "big") +
            payload)


def ec_ack(hop_seq: int) -> bytes:
    return bytes([EC_ACK]) + hop_seq.to_bytes(4, "big")


def ec_decode(data: bytes):
    #Devolve Data ou Ack. Qualquer datagrama mal formado levanta ProtocolError.
    r = Reader(data)
    kind = r.u8()
    if kind == EC_ACK:
        hop_seq = r.u32()
        r.end()                       # um ACK tem exatamente 5 bytes
        return Ack(hop_seq)
    if kind not in (EC_PUBLISH, EC_DELIVER):
        raise ProtocolError("kind desconhecido")
    qos = r.u8()
    if qos not in (1, 2):
        raise ProtocolError("qos inválido")
    hop_seq = r.u32()
    pub_id = decode_fixed(r.take(ID_LEN))
    fluxo = r.u16()
    seq = r.u32()
    topic = decode_topic(r.take(TOPIC_LEN))
    tam_payload = r.u16()
    if tam_payload > MAX_PAYLOAD:
        raise ProtocolError("payload acima do máximo")
    payload = r.take(tam_payload)     # menos bytes do que o declarado: truncado
    r.end()                           # mais bytes do que o declarado: inválido
    if not pub_id:
        raise ProtocolError("pub_id vazio")
    return Data(kind, qos, hop_seq, pub_id, fluxo, seq, topic, payload)