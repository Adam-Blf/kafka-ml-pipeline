"""Exercice 1, partie 2 : producteur simple sur le canal exo1.

Lancement : python exercices/produce.py
"""

import sys
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, NOM, TOPIC_EXO1  # noqa: E402


def main() -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    message = f"coucou {NOM.capitalize()}"
    producer.send(TOPIC_EXO1, message.encode("utf-8"))
    # send est asynchrone. Sans flush, un script court se termine avant que le
    # message parte, et rien n'arrive sans qu'aucune erreur ne soit levee.
    producer.flush()
    producer.close()
    print(f"Envoye sur {TOPIC_EXO1} : {message}")


if __name__ == "__main__":
    main()
