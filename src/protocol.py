"""Codificação dos dois protocolos, byte a byte (sem bibliotecas de serialização).

SubControl (TCP), cada mensagem é um quadro:
    comprimento(2B, big-endian) | tipo(1B) | campos...

EventCast (UDP), cada datagrama:
    kind(1B) | qos(1B) | len(1B) + message_id | len(1B) + topic | payload (resto do datagrama)
"""
import socket


class ProtocolError(Exception):
    """Mensagem mal formada ou cabeçalho truncado."""


def lp(data: bytes) -> bytes:
    """Campo de comprimento variável: 1 byte de tamanho + dados (máx. 255)."""
    if len(data) > 255:
        raise ValueError("campo com mais de 255 bytes")
    return bytes([len(data)]) + data


class Reader:
    """Lê campos com verificação de limites: um cabeçalho truncado vira ProtocolError."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def take(self, n):
        if self.pos + n > len(self.data):
            raise ProtocolError("cabeçalho truncado")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def u8(self):
        return self.take(1)[0]

    def u16(self):
        return int.from_bytes(self.take(2), "big")

    def lp(self):
        return self.take(self.u8())

    def text(self):
        try:
            return self.lp().decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError("texto inválido")

    def rest(self):
        out = self.data[self.pos:]
        self.pos = len(self.data)
        return out


# ---------- enquadramento SubControl (TCP) ----------

def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("ligação fechada pelo peer")
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock, body: bytes):
    if len(body) > 0xFFFF:
        raise ValueError("quadro demasiado grande")
    sock.sendall(len(body).to_bytes(2, "big") + body)


def recv_frame(sock) -> bytes:
    n = int.from_bytes(recv_exact(sock, 2), "big")
    return recv_exact(sock, n)


# ---------- SubControl: tipos e construção ----------

SC_HELLO = 1          # role(1B) | udp_port(2B) | lp(id)
SC_HELLO_ACK = 2      # data_port(2B)
SC_SUBSCRIBE = 3      # qos(1B) | lp(filtro)
SC_UNSUBSCRIBE = 4    # lp(filtro)
SC_ACK = 5            # resposta a SUBSCRIBE / UNSUBSCRIBE

ROLE_PUBLISHER = 1
ROLE_SUBSCRIBER = 2


def sc_hello(role, udp_port, ident):
    return bytes([SC_HELLO, role]) + udp_port.to_bytes(2, "big") + lp(ident.encode())


def sc_subscribe(filt, qos):
    return bytes([SC_SUBSCRIBE, qos]) + lp(filt.encode())


def sc_unsubscribe(filt):
    return bytes([SC_UNSUBSCRIBE]) + lp(filt.encode())


def request(sock, body):
    """Envia um pedido e espera o ACK. Qualquer outra resposta é erro."""
    send_frame(sock, body)
    if Reader(recv_frame(sock)).u8() != SC_ACK:
        raise ProtocolError("resposta inesperada")


def open_session(host, port, role, ident, udp_port):
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

EC_PUBLISH = 1
EC_DELIVER = 2


def ec_encode(kind, qos, msg_id, topic, payload: bytes):
    return bytes([kind, qos]) + lp(msg_id.encode()) + lp(topic.encode()) + payload


def ec_decode(data: bytes):
    r = Reader(data)
    kind = r.u8()
    qos = r.u8()
    msg_id = r.text()
    topic = r.text()
    payload = r.rest()
    return kind, qos, msg_id, topic, payload