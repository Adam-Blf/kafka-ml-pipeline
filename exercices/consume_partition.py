"""Session 2, exercice 3 : un consommateur assigne a UNE partition precise.

Difference avec l'abonnement : subscribe() laisse Kafka repartir les partitions
entre les membres du groupe et rebalancer quand quelqu'un entre ou sort.
assign() fige le choix, le consommateur ne lit que la partition demandee et ne
participe a aucun rebalancement.

Lancement :
    python exercices/consume_partition.py --partition 0
    python exercices/consume_partition.py --partition 1
"""

import argparse
import sys
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, NOM  # noqa: E402


def main() -> None:
    parseur = argparse.ArgumentParser(description=__doc__)
    parseur.add_argument("--partition", type=int, required=True)
    parseur.add_argument("--groupe", default="grp1")
    args = parseur.parse_args()

    # Pas de nom de topic dans le constructeur : l'assignation se fait plus bas.
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=args.groupe,
        auto_offset_reset="earliest",
    )
    tp = TopicPartition(NOM, args.partition)
    consumer.assign([tp])

    print(f"[p{args.partition}] assigne a {NOM} partition {args.partition}, groupe {args.groupe}")
    for message in consumer:
        print(f"[p{args.partition}] offset {message.offset} : {message.value.decode('utf-8')}")


if __name__ == "__main__":
    main()
