"""Chargement du modele et calcul d'une prediction.

Le sujet demande explicitement une fonction qui charge le fichier du modele et
le retourne. Elle est mise en cache : dans un consommateur, le modele se charge
une fois au demarrage et non a chaque message recu, sans quoi on paie une
lecture disque et une deserialisation par prediction.
"""

from functools import lru_cache
from pathlib import Path

import joblib
import pandas as pd

RACINE = Path(__file__).resolve().parent.parent
CHEMIN_MODELE = RACINE / "regression.joblib"

# L'ordre des colonnes doit etre celui de l'entrainement. Une inversion ne leve
# rien : elle rend des prix faux et parfaitement plausibles.
COLONNES = ["size", "nb_rooms", "garden"]


class ModeleIndisponible(RuntimeError):
    """Le fichier du modele est absent : lancer train_model.py."""


@lru_cache(maxsize=1)
def charger_modele():
    """Charge le modele depuis regression.joblib et le retourne."""
    if not CHEMIN_MODELE.exists():
        raise ModeleIndisponible(
            f"Modele introuvable : {CHEMIN_MODELE}. Lancer `python train_model.py`."
        )
    return joblib.load(CHEMIN_MODELE)


def predire(size: float, nb_rooms: int, garden: int) -> float:
    """Rend le prix estime pour une maison."""
    entree = pd.DataFrame([[size, nb_rooms, garden]], columns=COLONNES)
    return float(charger_modele().predict(entree)[0])
