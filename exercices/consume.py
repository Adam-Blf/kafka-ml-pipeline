"""Exercice 1, partie 1 : consommateur simple sur le canal exo1.

Lancement : python exercices/consume.py
Le script ne rend jamais la main, c'est le comportement attendu d'un
consommateur. S'il se termine tout de suite, l'adresse du broker est fausse.
"""

import sys
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_EXO1  # noqa: E402


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_EXO1,
        bootstrap_servers=BOOTSTRAP,
        # Sans ce reglage, un consommateur qui demarre ne lit que ce qui arrive
        # APRES lui : on croit le canal vide alors qu'il contient tout l'historique.
        auto_offset_reset="earliest",
    )
    print(f"A l'ecoute de {TOPIC_EXO1} sur {BOOTSTRAP}. Ctrl+C pour arreter.")
    for message in consumer:
        print(f"[{message.partition}:{message.offset}] {message.value.decode('utf-8')}")


if __name__ == "__main__":
    main()
