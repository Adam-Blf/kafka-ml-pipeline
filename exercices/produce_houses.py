"""Exercice 11 : producteur qui envoie des maisons a predire.

Lancement : python exercices/produce_houses.py [--nombre 5]
"""

import argparse
import json
import random
import sys
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_MAISONS  # noqa: E402


def maison_au_hasard() -> dict:
    return {
        "size": round(random.uniform(40, 260), 2),
        "nb_rooms": random.randint(1, 5),
        "garden": random.randint(0, 1),
    }


def main() -> None:
    parseur = argparse.ArgumentParser(description=__doc__)
    parseur.add_argument("--nombre", type=int, default=5)
    args = parseur.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for _ in range(args.nombre):
        maison = maison_au_hasard()
        producer.send(TOPIC_MAISONS, maison)
        print(f"envoye sur {TOPIC_MAISONS} : {maison}")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
