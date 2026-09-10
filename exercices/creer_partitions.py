"""Session 2, exercice 2 : creer les partitions d'un topic.

Un topic ne gagne pas de partitions tout seul : soit il nait avec le nombre par
defaut du broker (num.partitions), soit on l'augmente explicitement. C'est ce
que fait KafkaAdminClient.

On ne peut qu'AUGMENTER le nombre de partitions, jamais le reduire : baisser
casserait la garantie d'ordre par cle, puisque des messages deja ranges par
hash se retrouveraient ailleurs.

Lancement : python exercices/creer_partitions.py
"""

import sys
from pathlib import Path

from kafka.admin import KafkaAdminClient, NewPartitions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import BOOTSTRAP, NOM  # noqa: E402

TOTAL = 2


def main() -> None:
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    topic = NOM

    topic_partitions = {topic: NewPartitions(total_count=TOTAL)}

    try:
        admin_client.create_partitions(topic_partitions)
        print(f"{topic} porte maintenant {TOTAL} partitions")
    except Exception as erreur:
        # Rejouer le script sur un topic deja a 2 partitions leve une erreur.
        # Ce n'est pas un echec : l'etat vise est atteint.
        print(f"{topic} : {type(erreur).__name__} - {erreur}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
