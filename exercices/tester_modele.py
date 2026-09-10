"""Exercice 11, etape 4 : verifier que le modele rend bien une prediction.

Lancement : python exercices/tester_modele.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from modele.chargement import charger_modele, predire  # noqa: E402

CAS = [
    {"size": 120, "nb_rooms": 3, "garden": 1},
    {"size": 60, "nb_rooms": 1, "garden": 0},
    {"size": 240, "nb_rooms": 5, "garden": 1},
]


def main() -> None:
    modele = charger_modele()
    print(f"Modele charge : {type(modele).__name__}")
    print(f"Colonnes attendues : {list(getattr(modele, 'feature_names_in_', []))}\n")

    prix = []
    for cas in CAS:
        p = predire(**cas)
        prix.append(p)
        print(f"{cas} -> {p:,.2f}".replace(",", " "))

    # Controle de coherence : a nombre de chambres et jardin comparables, le
    # prix doit croitre avec la surface. Un simple "ca rend un nombre" resterait
    # vrai avec les colonnes chargees a l'envers.
    assert prix[2] > prix[1], "le prix devrait croitre avec la surface"
    print("\nControle de coherence : le prix croit bien avec la surface.")


if __name__ == "__main__":
    main()
