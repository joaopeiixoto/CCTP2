"""Configuração: 'chave = termo [opção=valor ...]'. '#' inicia comentário.
Não há valores por omissão: quem usa uma chave pede-a explicitamente."""


class ConfigError(ValueError):
    pass


def parse_config(path):
    #Devolve {chave: [(termo, {opções}), ...]} pela ordem do ficheiro.
    entries = {}
    with open(path, encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if "=" not in line:
                raise ConfigError(f"{path}:{lineno}: falta '='")
            key, value = (part.strip() for part in line.split("=", 1))
            tokens = value.split()
            if not key or not tokens:
                raise ConfigError(f"{path}:{lineno}: valor vazio")
            term, opts = tokens[0], {}
            for tok in tokens[1:]:
                if "=" not in tok:
                    raise ConfigError(f"{path}:{lineno}: opção sem '=': {tok}")
                name, val = tok.split("=", 1)
                opts[name] = val
            entries.setdefault(key, []).append((term, opts))
    return entries


def one(cfg, key):
    """Chave que aparece exatamente uma vez. Devolve o termo."""
    vals = cfg.get(key, [])
    if len(vals) != 1:
        raise ConfigError(f"a chave '{key}' tem de aparecer exatamente uma vez")
    return vals[0][0]


def many(cfg, key):
    """Chave que pode aparecer zero ou mais vezes. Devolve [(termo, opções)]."""
    return cfg.get(key, [])


def opt(opts, name):
    if name not in opts:
        raise ConfigError(f"falta a opção '{name}='")
    return opts[name]


def number(text, what, minimum=0.0):
    """Número de segundos (ou ritmo) não negativo."""
    try:
        v = float(text)
    except ValueError:
        raise ConfigError(f"{what}: não é um número: {text}")
    if v < minimum:
        raise ConfigError(f"{what}: tem de ser >= {minimum}")
    return v


def qos_of(opts):
    q = opt(opts, "qos")
    if q not in ("1", "2"):
        raise ConfigError("qos tem de ser 1 ou 2")
    return int(q)


def port(text, what):
    try:
        p = int(text)
    except ValueError:
        raise ConfigError(f"{what}: porta inválida: {text}")
    if not 1 <= p <= 65535:
        raise ConfigError(f"{what}: porta fora de 1..65535")
    return p


def parse_addr(s):
    """'a.b.c.d:porta' -> (host, porta). Endereços IPv4, sem resolução de nomes."""
    if ":" not in s:
        raise ConfigError(f"endereço sem porta: {s}")
    host, p = s.rsplit(":", 1)
    if not host:
        raise ConfigError(f"endereço sem host: {s}")
    return host, port(p, s)