"""Reglages partages par tous les scripts.

L'adresse du broker vient de l'environnement. En cours, c'est le broker commun
`nowledgeable.com:9092` ; en local, le docker-compose de ce depot ecoute sur
localhost:9092. Ecrire l'adresse en dur dans dix scripts oblige a dix
corrections le jour ou elle change.

Le defaut reste local : un script lance par erreur sans variable d'environnement
touche sa propre machine, pas le broker partage par toute la promotion.
"""

import os

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

# Le sujet demande un canal nomme d'apres le nom de famille.
NOM = os.environ.get("KAFKA_NOM", "beloucif")

TOPIC_EXO1 = "exo1"
TOPIC_DONNEES = NOM
TOPIC_MAISONS = f"maisons_{NOM}"
TOPIC_TRAITE = "processed"
TOPIC_PREDICTION = f"prediction_{NOM}"
