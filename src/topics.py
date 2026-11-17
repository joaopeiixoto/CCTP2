#Tópicos: regiao/estacao/metrica (3 níveis, sem '*').
#Filtros: tópico completo, ou prefixo terminado em /* (north/* ou north/station7/*).
#Cada nível ocupa um campo de 16 bytes na rede, por isso tem de 1 a 16 bytes UTF-8.

LEVEL_MAX = 16


def _level_ok(level):
    return (bool(level) and len(level.encode("utf-8")) <= LEVEL_MAX
            and "*" not in level and "\x00" not in level
            and not any(c.isspace() for c in level))


def valid_topic(topic):
    parts = topic.split("/")
    return len(parts) == 3 and all(_level_ok(p) for p in parts)


def valid_filter(filt):
    if "*" not in filt:
        return valid_topic(filt)
    if not filt.endswith("/*"): # north/*/temperature não é válido
        return False
    prefix = filt[:-2].split("/")
    return 1 <= len(prefix) <= 2 and all(_level_ok(p) for p in prefix)


def matches(filt, topic):
    if filt.endswith("/*"):
        return topic.startswith(filt[:-1])   # "north/*" -> "north/"; "north" não corresponde
    return filt == topic


def covered(filters, topic):
    #Algum dos filtros corresponde ao tópico?
    return any(matches(f, topic) for f in filters)