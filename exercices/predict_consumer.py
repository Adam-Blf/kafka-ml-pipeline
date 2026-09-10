"""Exercice 11 : consommateur qui predit et republie la prediction.

Couvre les etapes 5, 6, 7 et 9 du sujet.

Lancement :
    python exercices/predict_consumer.py                      # affiche seulement
    python exercices/predict_consumer.py --publier            # publie aussi
    python exercices/predict_consumer.py --publier --groupe predicteurs --etiquette p1

Le groupe est ce qui permet de paralleliser sans traiter deux fois. Sans
group_id, deux instances recoivent chacune tous les messages et predisent deux
fois la meme maison. Avec un group_id commun, Kafka assigne chaque partition a
exactement un consommateur du groupe.
"""

import argparse
import sys
from pathlib import Path

from kafka import KafkaConsumer, KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_MAISONS, TOPIC_PREDICTION  # noqa: E402
from serialisation import JsonDeserializer, JsonSerializer  # noqa: E402
from modele.chargement import charger_modele, predire  # noqa: E402


def main() -> None:
    parseur = argparse.ArgumentParser(description=__doc__)
    parseur.add_argument("--publier", action="store_true", help="republier sur le canal de predictions")
    parseur.add_argument("--groupe", default=None)
    parseur.add_argument("--etiquette", default="p1")
    args = parseur.parse_args()

    # Le modele est charge UNE fois, avant la boucle.
    charger_modele()

    consumer = KafkaConsumer(
        TOPIC_MAISONS,
        bootstrap_servers=BOOTSTRAP,
        group_id=args.groupe,
        auto_offset_reset="earliest",
        value_deserializer=JsonDeserializer(),
    )
    producer = None
    if args.publier:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            value_serializer=JsonSerializer(),
        )

    mode = f"groupe {args.groupe}" if args.groupe else "sans groupe"
    cible = f" -> {TOPIC_PREDICTION}" if args.publier else ""
    print(f"[{args.etiquette}] {TOPIC_MAISONS} ({mode}){cible}")

    for message in consumer:
        maison = message.value
        prix = predire(maison["size"], maison["nb_rooms"], maison["garden"])
        print(f"[{args.etiquette}] partition {message.partition} {maison} -> {prix:,.2f}".replace(",", " "))
        if producer:
            producer.send(TOPIC_PREDICTION, {**maison, "y_pred": prix})
            producer.flush()


if __name__ == "__main__":
    main()
