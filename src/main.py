"""Ponto de entrada: ./run.sh <papel> --id <id> --config <ficheiro>"""
import argparse
import sys

from config import parse_config
from broker import Broker
from publisher import Publisher
from subscriber import Subscriber

ROLES = {"broker": Broker, "publisher": Publisher, "subscriber": Subscriber}


def main(argv=None):
    parser = argparse.ArgumentParser(prog="run.sh")
    parser.add_argument("role", choices=sorted(ROLES))
    parser.add_argument("--id", required=True)
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)

    if not args.id or len(args.id.encode()) > 16 or any(c.isspace() for c in args.id):
        print("erro: --id tem de ter 1 a 16 bytes e não ter espaços", file=sys.stderr)
        return 2

    try:
        cfg = parse_config(args.config)
        node = ROLES[args.role](args.id, cfg)
    except (OSError, ValueError) as e:      # ConfigError herda de ValueError
        print(f"erro: {e}", file=sys.stderr)
        return 2

    node.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())