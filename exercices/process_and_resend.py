"""Exercice 3 : consommateur ET producteur dans le meme programme.

Lit un JSON contenant un tableau, en calcule la somme, republie le resultat sur
le canal processed. C'est le motif de base d'une chaine de traitement en flux :
chaque maillon lit un canal, fait une chose, ecrit sur le suivant, sans jamais
connaitre les autres maillons.

Lancement : python exercices/process_and_resend.py
"""

import json
import sys
from pathlib import Path

import numpy as np
from kafka import KafkaConsumer, KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_DONNEES, TOPIC_TRAITE  # noqa: E402


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_DONNEES,
        bootstrap_servers=BOOTSTRAP,
        group_id="processeur",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"{TOPIC_DONNEES} -> somme -> {TOPIC_TRAITE}")

    for message in consumer:
        tableau = np.array(message.value["data"])
        resultat = {"somme": float(tableau.sum()), "forme": list(tableau.shape)}
        producer.send(TOPIC_TRAITE, resultat)
        producer.flush()
        print(f"recu {tableau.tolist()} -> publie {resultat}")


if __name__ == "__main__":
    main()
