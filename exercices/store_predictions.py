"""Exercice 11, bonus : stocker chaque prediction recue dans une base SQLite.

SQLite plutot qu'un serveur : la base tient dans un fichier, elle est dans la
bibliotheque standard, et l'exercice porte sur le flux, pas sur l'exploitation
d'une base.

Lancement : python exercices/store_predictions.py
"""

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, TOPIC_PREDICTION  # noqa: E402
from serialisation import JsonDeserializer  # noqa: E402

BASE = Path(__file__).resolve().parent.parent / "predictions.db"


def ouvrir_base() -> sqlite3.Connection:
    cnx = sqlite3.connect(BASE)
    cnx.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            recue_le  TEXT    NOT NULL,
            size      REAL    NOT NULL,
            nb_rooms  INTEGER NOT NULL,
            garden    INTEGER NOT NULL,
            y_pred    REAL    NOT NULL
        )
        """
    )
    cnx.commit()
    return cnx


def main() -> None:
    cnx = ouvrir_base()
    consumer = KafkaConsumer(
        TOPIC_PREDICTION,
        bootstrap_servers=BOOTSTRAP,
        group_id="archivage",
        auto_offset_reset="earliest",
        value_deserializer=JsonDeserializer(),
    )
    print(f"{TOPIC_PREDICTION} -> {BASE.name}")

    for message in consumer:
        p = message.value
        cnx.execute(
            "INSERT INTO predictions (recue_le, size, nb_rooms, garden, y_pred) VALUES (?,?,?,?,?)",
            (datetime.now(timezone.utc).isoformat(timespec="seconds"),
             p["size"], p["nb_rooms"], p["garden"], p["y_pred"]),
        )
        # Valider a chaque message : un traitement long qui n'ecrit qu'a la fin
        # transforme le moindre incident en perte totale.
        cnx.commit()
        total = cnx.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
        print(f"stocke {p} (total en base : {total})")


if __name__ == "__main__":
    main()
