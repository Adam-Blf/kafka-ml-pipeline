# Changelog

All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), versions follow [SemVer](https://semver.org/).

## [0.1.0] - 2026-10-07

First tagged release. Latest changes:

- docs: add colors to mermaid diagrams (#7)
- chore: add the MIT licence
- docs: name the shared class broker and keep the local default
- chore: make the local Kafka compose shareable and self-diagnosing
- feat: create partitions and pin one consumer per partition
- fix: replace lambda serializers with the classes kafka-python expects
- docs: document the chain, the exercises and the two traps
- test: prove every exercise end to end against a real broker
- feat: store predictions in SQLite and count topic partitions
- feat: serve the model over Kafka and republish predictions
- feat: load the model once and expose a predict helper
- feat: add the exercise 3 consumer and producer in one program
- feat: add the exercise 2 JSON pair and answer the group question
- feat: add the exercise 1 consumer and producer
- chore: add the provided dataset and training script
