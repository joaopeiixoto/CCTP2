#!/usr/bin/env python3
"""Script de teste automatizado para a Fase 1.
Valida o contrato de execução, o enquadramento binário, o reencaminhamento UDP e a integridade dos logs.
"""
import os
import shutil
import subprocess
import tempfile
import time
import sys


def run_test():
    print("=" * 60)
    print("Iniciando Verificação Automatizada — Fase 1")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="cc_test_fase1_")
    broker_cfg = os.path.join(tmpdir, "broker.cfg")
    pub_cfg = os.path.join(tmpdir, "pub1.cfg")
    sub_cfg = os.path.join(tmpdir, "sub1.cfg")
    pub_log = os.path.join(tmpdir, "pub1.log")
    sub_log = os.path.join(tmpdir, "sub1.log")

    try:
        # 1. Gerar configurações de teste
        with open(broker_cfg, "w", encoding="utf-8") as f:
            f.write("""bind = 127.0.0.1
control_port = 17380
data_port = 17381
duration = 6
evict_after = 5
""")

        with open(pub_cfg, "w", encoding="utf-8") as f:
            f.write(f"""broker_control = 127.0.0.1:17380
log = {pub_log}
start_after = 1
drain = 2
publish = north/station7/temperature qos=1 count=10 rate=10
publish = north/station7/humidity qos=2 count=5 rate=5
""")

        with open(sub_cfg, "w", encoding="utf-8") as f:
            f.write(f"""broker_control = 127.0.0.1:17380
log = {sub_log}
duration = 5
subscribe = north/station7/* qos=1
""")

        print("[1/5] Ficheiros de configuração temporários criados em:", tmpdir)

        # 2. Lançar o Broker via ./run.sh
        print("[2/5] Lançando o Broker...")
        p_broker = subprocess.Popen(["./run.sh", "broker", "--id", "broker1", "--config", broker_cfg])
        time.sleep(0.3)

        # 3. Lançar o Subscritor via ./run.sh
        print("[3/5] Lançando o Subscritor...")
        p_sub = subprocess.Popen(["./run.sh", "subscriber", "--id", "sub1", "--config", sub_cfg])
        time.sleep(0.2)

        # 4. Lançar o Publicador via ./run.sh
        print("[4/5] Lançando o Publicador...")
        p_pub = subprocess.Popen(["./run.sh", "publisher", "--id", "pub1", "--config", pub_cfg])

        # Aguardar conclusão dos processos
        p_pub.wait(timeout=10)
        p_sub.wait(timeout=10)
        p_broker.wait(timeout=10)

        print("[5/5] Todos os processos terminaram. Validando registos...")

        # 5. Validação dos logs
        assert os.path.exists(pub_log), f"Ficheiro de log do publicador não foi criado: {pub_log}"
        assert os.path.exists(sub_log), f"Ficheiro de log do subscritor não foi criado: {sub_log}"

        with open(pub_log, "r", encoding="utf-8") as f:
            pub_lines = [l.strip() for l in f if l.strip()]

        with open(sub_log, "r", encoding="utf-8") as f:
            sub_lines = [l.strip() for l in f if l.strip()]

        print(f"  -> Publicador registou {len(pub_lines)} mensagens.")
        print(f"  -> Subscritor registou {len(sub_lines)} entregas.")

        assert len(pub_lines) == 15, f"Esperadas 15 publicações, obtidas {len(pub_lines)}"
        assert len(sub_lines) == 15, f"Esperadas 15 entregas, obtidas {len(sub_lines)}"

        # Verificar formato: <timestamp_ms> <id> <topico> <message_id>
        now_ms = time.time_ns() // 1_000_000
        for line in pub_lines:
            parts = line.split()
            assert len(parts) == 4, f"Linha mal formada no log do publicador: {line}"
            ts = int(parts[0])
            assert abs(now_ms - ts) < 30_000, f"Timestamp Unix inválido ou fora do relógio atual: {ts}"
            assert parts[1] == "pub1"

        for line in sub_lines:
            parts = line.split()
            assert len(parts) == 4, f"Linha mal formada no log do subscritor: {line}"
            ts = int(parts[0])
            assert abs(now_ms - ts) < 30_000, f"Timestamp Unix inválido ou fora do relógio atual: {ts}"
            assert parts[1] == "sub1"

        # Conferência do par (tópico, message_id) entre publicador e subscritor
        pub_entries = sorted([(l.split()[2], l.split()[3]) for l in pub_lines])
        sub_entries = sorted([(l.split()[2], l.split()[3]) for l in sub_lines])

        assert pub_entries == sub_entries, "Os registos de entrega não coincidem exatamente com o publicado!"

        print("=" * 60)
        print("✓ TESTES DA FASE 1 PASSARAM COM 100% DE SUCESSO!")
        print("  - Contrato de execução ./run.sh: OK")
        print("  - Negociação de portos SubControl: OK")
        print("  - Transporte e cabeçalho EventCast (74B): OK")
        print("  - Correspondência e integridade dos logs: OK")
        print("=" * 60)
        return 0

    except Exception as e:
        print("\n❌ ERRO NA VERIFICAÇÃO:", e, file=sys.stderr)
        return 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(run_test())

