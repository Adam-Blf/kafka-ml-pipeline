"""Exercice 12, etape 10 : verifier le nombre de partitions d'un canal.

Lancement : python exercices/compter_partitions.py [nom_du_topic]
"""

import sys
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_MAISONS  # noqa: E402


def main() -> None:
    topic = sys.argv[1] if len(sys.argv) > 1 else TOPIC_MAISONS
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    # Forcer une lecture des metadonnees : sur un client qui vient de se
    # connecter, partitions_for_topic peut rendre un ensemble vide simplement
    # parce que le catalogue n'a pas encore ete recupere.
    consumer.topics()
    partitions = consumer.partitions_for_topic(topic)
    consumer.close()
    if not partitions:
        print(f"{topic} : canal inconnu du broker {BOOTSTRAP}")
        return
    print(f"{topic} : {len(partitions)} partition(s) -> {sorted(partitions)}")
    print("Le parallelisme d'un groupe est borne par ce nombre : au-dela, "
          "les consommateurs supplementaires restent inactifs.")


if __name__ == "__main__":
    main()
