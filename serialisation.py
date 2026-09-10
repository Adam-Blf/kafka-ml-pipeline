"""Serialisation JSON pour Kafka.

kafka-python 3 attend des classes qui implementent les interfaces Serializer et
Deserializer. Passer une lambda marche encore mais emet un avertissement de
depreciation a chaque creation de client, ce qui noie les vraies erreurs.

Kafka ne transporte que des octets : c'est ici que le JSON devient des octets a
l'emission, et redevient un dictionnaire a la reception.
"""

import json

from kafka.serializer import Deserializer, Serializer


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        if data is None:
            return None
        return json.dumps(data).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, headers, data):
        if data is None:
            return None
        return json.loads(data.decode("utf-8"))
