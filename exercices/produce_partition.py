"""Session 2, exercice 3 : producteur qui choisit sa partition au hasard.

Sans cle et sans partition explicite, Kafka repartit tout seul. Ici on force la
partition pour voir chaque message atterrir chez l'un ou l'autre consommateur.

Lancement : python exercices/produce_partition.py --nombre 6
"""

import argparse
import random
import sys
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, NOM  # noqa: E402


def main() -> None:
    parseur = argparse.ArgumentParser(description=__doc__)
    parseur.add_argument("--nombre", type=int, default=6)
    args = parseur.parse_args()

    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    for i in range(1, args.nombre + 1):
        partition = random.randint(0, 1)
        message = f"message {i}"
        producer.send(NOM, message.encode("utf-8"), partition=partition)
        print(f"envoye sur {NOM} partition {partition} : {message}")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
