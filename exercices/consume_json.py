"""Exercice 2, partie 2 : consommateur JSON qui somme un tableau numpy.

Lancement : python exercices/consume_json.py [--groupe NOM]

L'option --groupe repond a la question posee par le sujet. Sans groupe commun,
deux consommateurs identiques recoivent TOUS LES DEUX chaque message. Avec un
meme group_id, Kafka assigne chaque partition a un seul consommateur du groupe,
donc un message n'est traite qu'une fois.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_DONNEES  # noqa: E402
from serialisation import JsonDeserializer  # noqa: E402


def main() -> None:
    parseur = argparse.ArgumentParser(description=__doc__)
    parseur.add_argument("--groupe", default=None, help="group_id partage entre consommateurs")
    parseur.add_argument("--etiquette", default="c1", help="nom affiche, pour distinguer deux instances")
    args = parseur.parse_args()

    consumer = KafkaConsumer(
        TOPIC_DONNEES,
        bootstrap_servers=BOOTSTRAP,
        group_id=args.groupe,
        auto_offset_reset="earliest",
        value_deserializer=JsonDeserializer(),
    )
    mode = f"groupe {args.groupe}" if args.groupe else "sans groupe, diffusion a tous"
    print(f"[{args.etiquette}] a l'ecoute de {TOPIC_DONNEES} ({mode})")

    for message in consumer:
        tableau = np.array(message.value["data"])
        print(f"[{args.etiquette}] partition {message.partition} "
              f"tableau {tableau.tolist()} somme {tableau.sum()}")


if __name__ == "__main__":
    main()
