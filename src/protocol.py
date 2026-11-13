"""Codificação dos dois protocolos, byte a byte (sem bibliotecas de serialização),
estritamente de acordo com as especificações do relatório.

SubControl (TCP), cada mensagem é um quadro:
    comprimento(1B) | tipo(1B) | campos...

EventCast (UDP), cada datagrama:
    kind(1B) | qos(1B) | pub_id(16B) | fluxo(2B) | seq(4B) | topico(48B) | tam_payload(2B) | payload
"""
import socket


class ProtocolError(Exception):
    """Mensagem mal formada ou cabeçalho truncado."""


def encode_fixed(text: str, length: int) -> bytes:
    """Codifica texto em UTF-8 com tamanho fixo, preenchido com 0x00 à direita."""
    raw = text.encode("utf-8")
    if len(raw) > length:
        raise ValueError(f"campo excede {length} bytes: {text}")
    return raw.ljust(length, b"\x00")


def decode_fixed(raw: bytes) -> str:
    """Descodifica texto em UTF-8 removendo o preenchimento de 0x00 à direita."""
    try:
        return raw.rstrip(b"\x00").decode("utf-8")
    except UnicodeDecodeError:
        raise ProtocolError("texto inválido (não é UTF-8)")


def encode_topic(topic: str) -> bytes:
    """Codifica um tópico em 48 bytes: regiao (16B) + estacao (16B) + metrica (16B)."""
    parts = topic.split("/")
    if len(parts) != 3:
        raise ValueError(f"tópico inválido (requer 3 níveis): {topic}")
    return (encode_fixed(parts[0], 16) +
            encode_fixed(parts[1], 16) +
            encode_fixed(parts[2], 16))


def decode_topic(raw: bytes) -> str:
    """Descodifica 48 bytes no formato regiao/estacao/metrica."""
    if len(raw) != 48:
        raise ProtocolError("tópico com tamanho inválido (esperado 48 bytes)")
    reg = decode_fixed(raw[:16])
    est = decode_fixed(raw[16:32])
    met = decode_fixed(raw[32:48])
    if not reg or not est or not met:
        raise ProtocolError("tópico com níveis vazios")
    return f"{reg}/{est}/{met}"


def encode_filter(filt: str) -> bytes:
    """Codifica um filtro em 48 bytes sem barras: regiao(16B) + estacao(16B) + metrica(16B) com '*'."""
    if filt.endswith("/*"):
        prefix = filt[:-2].split("/")
        if len(prefix) == 1:
            return (encode_fixed(prefix[0], 16) +
                    encode_fixed("*", 16) +
                    encode_fixed("", 16))
        elif len(prefix) == 2:
            return (encode_fixed(prefix[0], 16) +
                    encode_fixed(prefix[1], 16) +
                    encode_fixed("*", 16))
        else:
            raise ValueError(f"filtro inválido: {filt}")
    parts = filt.split("/")
    if len(parts) != 3:
        raise ValueError(f"filtro inválido: {filt}")
    return (encode_fixed(parts[0], 16) +
            encode_fixed(parts[1], 16) +
            encode_fixed(parts[2], 16))


def decode_filter(raw: bytes) -> str:
    """Descodifica 48 bytes no formato canónico do filtro (com '*' se aplicável)."""
    if len(raw) != 48:
        raise ProtocolError("filtro com tamanho inválido (esperado 48 bytes)")
    reg = decode_fixed(raw[:16])
    est = decode_fixed(raw[16:32])
    met = decode_fixed(raw[32:48])
    if not reg:
        raise ProtocolError("filtro com região vazia")
    if est == "*":
        return f"{reg}/*"
    if met == "*":
        if not est:
            raise ProtocolError("filtro com estação vazia antes de '*'")
        return f"{reg}/{est}/*"
    if not est or not met:
        raise ProtocolError("filtro incompleto")
    return f"{reg}/{est}/{met}"


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

    def rest(self) -> bytes:
        out = self.data[self.pos:]
        self.pos = len(self.data)
        return out


# ---------- Enquadramento SubControl (TCP) ----------
# Conforme relatório: comprimento(1B) | tipo(1B) | campos...

def recv_exact(sock, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("ligação fechada pelo peer")
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock, body: bytes):
    if len(body) > 255:
        raise ValueError("quadro demasiado grande (> 255 bytes)")
    sock.sendall(bytes([len(body)]) + body)


def recv_frame(sock) -> bytes:
    n = recv_exact(sock, 1)[0]
    if n < 1:
        raise ProtocolError("comprimento inválido")
    body = recv_exact(sock, n)
    if SC_SIZE.get(body[0]) != n:
        raise ProtocolError("tipo desconhecido ou comprimento incompatível")
    return body


# ---------- SubControl: tipos e construção ----------

SC_HELLO = 1          # tipo(1B) | role(1B) | udp_port(2B) | id(16B)
SC_HELLO_ACK = 2      # tipo(1B) | data_port(2B)
SC_SUBSCRIBE = 3      # tipo(1B) | qos(1B) | filtro(48B: regiao, estacao, metrica)
SC_UNSUBSCRIBE = 4    # tipo(1B) | filtro(48B: regiao, estacao, metrica)
SC_ACK = 5            # tipo(1B)

SC_SIZE = {SC_HELLO: 20, SC_HELLO_ACK: 3, SC_SUBSCRIBE: 50, SC_UNSUBSCRIBE: 49, SC_ACK: 1}

ROLE_PUBLISHER = 1
ROLE_SUBSCRIBER = 2
FILTER_LEN = 48


def sc_hello(role: int, udp_port: int, ident: str) -> bytes:
    return bytes([SC_HELLO, role]) + udp_port.to_bytes(2, "big") + encode_fixed(ident, 16)


def sc_hello_ack(data_port: int) -> bytes:
    return bytes([SC_HELLO_ACK]) + data_port.to_bytes(2, "big")


def sc_subscribe(filt: str, qos: int) -> bytes:
    return bytes([SC_SUBSCRIBE, qos]) + encode_filter(filt)


def sc_unsubscribe(filt: str) -> bytes:
    return bytes([SC_UNSUBSCRIBE]) + encode_filter(filt)


def request(sock, body: bytes):
    """Envia um pedido e espera o ACK. Qualquer outra resposta é erro."""
    send_frame(sock, body)
    resp = recv_frame(sock)
    if Reader(resp).u8() != SC_ACK:
        raise ProtocolError("resposta inesperada (esperava SC_ACK)")


def open_session(host: str, port: int, role: int, ident: str, udp_port: int):
    """Abre a sessão SubControl e devolve (socket TCP, endereço do plano de dados)."""
    sock = socket.create_connection((host, port), timeout=10)
    send_frame(sock, sc_hello(role, udp_port, ident))
    r = Reader(recv_frame(sock))
    if r.u8() != SC_HELLO_ACK:
        raise ProtocolError("esperava HELLO_ACK")
    data_port = r.u16()
    sock.settimeout(None)
    return sock, (host, data_port)


# ---------- EventCast (UDP) ----------
# kind(1B) | qos(1B) | pub_id(16B) | fluxo(2B) | seq(4B) | topico(48B) | tam_payload(2B) | payload

EC_PUBLISH = 1
EC_DELIVER = 2
EC_ACK = 3


def ec_encode(kind: int, qos: int, pub_id: str, fluxo: int, seq: int, topic: str, payload: bytes) -> bytes:
    """Codifica o datagrama EventCast conforme a Tabela 2 do relatório."""
    top_bytes = encode_topic(topic)
    return (bytes([kind, qos]) +
            encode_fixed(pub_id, 16) +
            fluxo.to_bytes(2, "big") +
            seq.to_bytes(4, "big") +
            top_bytes +
            len(payload).to_bytes(2, "big") +
            payload)


def ec_decode(data: bytes):
    """Descodifica o datagrama EventCast com verificação de limites."""
    r = Reader(data)
    kind = r.u8()
    qos = r.u8()
    pub_id = decode_fixed(r.take(16))
    fluxo = r.u16()
    seq = r.u32()
    topic = decode_topic(r.take(48))
    tam_payload = r.u16()
    payload = r.take(tam_payload)
    if r.pos != len(data):
        raise ProtocolError("bytes a mais")
    return kind, qos, pub_id, fluxo, seq, topic, payload
