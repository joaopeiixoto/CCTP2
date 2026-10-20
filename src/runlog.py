"""Registo: <timestamp_ms> <id> <topico> <message_id>, com flush em cada linha."""
import threading
import time


class RunLog:
    def __init__(self, path, ident):
        self.f = open(path, "w", encoding="utf-8")
        self.ident = ident
        self.lock = threading.Lock()

    def write(self, topic, msg_id):
        ts = time.time_ns() // 1_000_000          # relógio do sistema, em ms
        line = f"{ts} {self.ident} {topic} {msg_id}\n"
        with self.lock:
            self.f.write(line)
            self.f.flush()                          # sobrevive a SIGKILL

    def close(self):
        with self.lock:
            self.f.close()