"""Exercice 2, partie 1 : producteur JSON sur un canal nomme d'apres le nom.

Lancement : python exercices/produce_json.py
"""

import sys
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_DONNEES  # noqa: E402
from serialisation import JsonSerializer  # noqa: E402

CHARGE = {"data": [[1, 2], [3, 4]]}


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        # Kafka ne transporte que des octets : le JSON s'encode a l'emission.
        value_serializer=JsonSerializer(),
    )
    producer.send(TOPIC_DONNEES, CHARGE)
    producer.flush()
    producer.close()
    print(f"Envoye sur {TOPIC_DONNEES} : {CHARGE}")


if __name__ == "__main__":
    main()
