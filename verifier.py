"""Preuve executee de chaque exercice, contre le broker configure.

Lance les consommateurs en arriere-plan, envoie les messages, coupe, puis lit
ce que chacun a reellement affiche. Un exercice n'est pas fait parce que le
script existe : il est fait quand on a vu le message arriver.

Lancement : python verifier.py
"""

import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

RACINE = Path(__file__).resolve().parent
PY = str(RACINE / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
SORTIES = Path(tempfile.mkdtemp(prefix="kafka-tp-"))

# Un suffixe unique par execution : sans cela, un consommateur "earliest" relit
# les messages des essais precedents et le decompte devient faux.
MARQUE = uuid.uuid4().hex[:8]

VERT, ROUGE, GRIS, FIN = "\033[32m", "\033[31m", "\033[90m", "\033[0m"
resultats = []


def annoncer(titre: str) -> None:
    print(f"\n{'=' * 70}\n{titre}\n{'=' * 70}")


def verifier(nom: str, condition: bool, detail: str = "") -> None:
    resultats.append((nom, condition))
    marque = f"{VERT}OK{FIN}" if condition else f"{ROUGE}ECHEC{FIN}"
    print(f"  [{marque}] {nom}" + (f"  {GRIS}{detail}{FIN}" if detail else ""))


def lancer_fond(args: list[str], etiquette: str) -> tuple[subprocess.Popen, Path]:
    sortie = SORTIES / f"{etiquette}.log"
    fh = open(sortie, "w", encoding="utf-8")
    p = subprocess.Popen([PY, "-u", *args], cwd=RACINE, stdout=fh, stderr=subprocess.STDOUT)
    return p, sortie


def couper(procs) -> None:
    for p, _ in procs:
        p.terminate()
    for p, _ in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


def lire(chemin: Path) -> str:
    for _ in range(10):
        try:
            return chemin.read_text(encoding="utf-8", errors="replace")
        except PermissionError:
            time.sleep(0.3)
    return ""


def env_topic(**kv):
    e = dict(os.environ)
    e.update(kv)
    return e


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    print(f"Broker : {bootstrap}")
    print(f"Marque d'execution : {MARQUE}   Journaux : {SORTIES}")

    # --- Exercice 1 : un producteur, un consommateur ---------------------
    annoncer("Exercice 1 : consommateur et producteur simples")
    os.environ["KAFKA_NOM"] = f"beloucif{MARQUE}"
    c1 = lancer_fond(["exercices/consume.py"], "exo1_consumer")
    time.sleep(6)
    envoi = subprocess.run([PY, "exercices/produce.py"], cwd=RACINE,
                           capture_output=True, text=True, timeout=60)
    time.sleep(5)
    couper([c1])
    recu = lire(c1[1])
    verifier("le producteur confirme l'envoi", "Envoye sur exo1" in envoi.stdout,
             envoi.stdout.strip().splitlines()[-1] if envoi.stdout.strip() else "")
    verifier("le consommateur recoit le message", "coucou" in recu.lower(),
             [l for l in recu.splitlines() if "coucou" in l.lower()][:1])

    # --- Exercice 2 : diffusion contre groupe ----------------------------
    annoncer("Exercice 2 : JSON, et la question des deux consommateurs")
    topic = f"beloucif{MARQUE}"
    e = env_topic(KAFKA_NOM=topic)

    print(f"\n{GRIS}-- sans group_id : chacun doit recevoir le message{FIN}")
    a = lancer_fond(["exercices/consume_json.py", "--etiquette", "a"], "exo2_libre_a")
    b = lancer_fond(["exercices/consume_json.py", "--etiquette", "b"], "exo2_libre_b")
    time.sleep(7)
    subprocess.run([PY, "exercices/produce_json.py"], cwd=RACINE, env=e,
                   capture_output=True, text=True, timeout=60)
    time.sleep(5)
    couper([a, b])
    ra, rb = lire(a[1]), lire(b[1])
    verifier("consommateur a recoit et somme a 10", "somme 10" in ra)
    verifier("consommateur b recoit aussi", "somme 10" in rb)

    print(f"\n{GRIS}-- avec un group_id commun : un seul doit recevoir{FIN}")
    topic2 = f"grp{MARQUE}"
    e2 = env_topic(KAFKA_NOM=topic2)
    ga = lancer_fond(["exercices/consume_json.py", "--groupe", "gtest", "--etiquette", "ga"], "exo2_grp_a")
    gb = lancer_fond(["exercices/consume_json.py", "--groupe", "gtest", "--etiquette", "gb"], "exo2_grp_b")
    time.sleep(10)  # laisser le groupe s'equilibrer avant de produire
    subprocess.run([PY, "exercices/produce_json.py"], cwd=RACINE, env=e2,
                   capture_output=True, text=True, timeout=60)
    time.sleep(6)
    couper([ga, gb])
    rga, rgb = lire(ga[1]), lire(gb[1])
    recus = ("somme 10" in rga) + ("somme 10" in rgb)
    verifier("un seul consommateur du groupe traite le message", recus == 1,
             f"a={'oui' if 'somme 10' in rga else 'non'} b={'oui' if 'somme 10' in rgb else 'non'}")

    # --- Exercice 3 : consommateur ET producteur -------------------------
    annoncer("Exercice 3 : process_and_resend, somme republiee sur processed")
    topic3 = f"chaine{MARQUE}"
    e3 = env_topic(KAFKA_NOM=topic3)
    proc = subprocess.Popen([PY, "-u", "exercices/process_and_resend.py"], cwd=RACINE, env=e3,
                            stdout=open(SORTIES / "exo3_proc.log", "w", encoding="utf-8"),
                            stderr=subprocess.STDOUT)
    aval = subprocess.Popen([PY, "-u", "-c",
        "import json,os,sys;from kafka import KafkaConsumer;"
        "c=KafkaConsumer('processed',bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP','localhost:9092'),"
        "auto_offset_reset='latest',value_deserializer=lambda v: json.loads(v.decode()));"
        "print('aval pret',flush=True);"
        "[print('AVAL',m.value,flush=True) for m in c]"], cwd=RACINE, env=e3,
        stdout=open(SORTIES / "exo3_aval.log", "w", encoding="utf-8"), stderr=subprocess.STDOUT)
    time.sleep(9)
    subprocess.run([PY, "exercices/produce_json.py"], cwd=RACINE, env=e3,
                   capture_output=True, text=True, timeout=60)
    time.sleep(6)
    couper([(proc, None), (aval, None)])
    rproc, raval = lire(SORTIES / "exo3_proc.log"), lire(SORTIES / "exo3_aval.log")
    verifier("le maillon calcule la somme", "publie" in rproc and "10" in rproc)
    verifier("un tiers lit le resultat sur processed", "AVAL" in raval and "10" in raval,
             [l for l in raval.splitlines() if l.startswith("AVAL")][:1])

    # --- Exercice 11 : le modele en flux ---------------------------------
    annoncer("Exercice 11 : prediction en flux et republication")
    topic11 = f"ml{MARQUE}"
    e11 = env_topic(KAFKA_NOM=topic11)
    pred = subprocess.Popen([PY, "-u", "exercices/predict_consumer.py", "--publier", "--etiquette", "p1"],
                            cwd=RACINE, env=e11,
                            stdout=open(SORTIES / "exo11_pred.log", "w", encoding="utf-8"),
                            stderr=subprocess.STDOUT)
    store = subprocess.Popen([PY, "-u", "exercices/store_predictions.py"], cwd=RACINE, env=e11,
                             stdout=open(SORTIES / "exo11_store.log", "w", encoding="utf-8"),
                             stderr=subprocess.STDOUT)
    time.sleep(10)
    envoi11 = subprocess.run([PY, "exercices/produce_houses.py", "--nombre", "3"],
                             cwd=RACINE, env=e11, capture_output=True, text=True, timeout=60)
    time.sleep(8)
    couper([(pred, None), (store, None)])
    rpred, rstore = lire(SORTIES / "exo11_pred.log"), lire(SORTIES / "exo11_store.log")
    n_pred = rpred.count("->")
    verifier("le consommateur predit chaque maison recue", n_pred >= 3, f"{n_pred} predictions")
    verifier("les predictions sont stockees en base", "total en base" in rstore,
             [l for l in rstore.splitlines() if "total en base" in l][-1:])

    # --- Exercice 12 : partitions ----------------------------------------
    annoncer("Exercice 12 : nombre de partitions du canal")
    part = subprocess.run([PY, "exercices/compter_partitions.py", f"maisons_{topic11}"],
                          cwd=RACINE, capture_output=True, text=True, timeout=60)
    verifier("partitions lues via partitions_for_topic", "partition(s)" in part.stdout,
             part.stdout.strip().splitlines()[0] if part.stdout.strip() else part.stderr[:80])

    # --- Bilan ------------------------------------------------------------
    annoncer("Bilan")
    ok = sum(1 for _, c in resultats if c)
    for nom, c in resultats:
        print(f"  {(VERT + 'OK' + FIN) if c else (ROUGE + 'ECHEC' + FIN)}  {nom}")
    print(f"\n{ok}/{len(resultats)} verifications au vert")
    print(f"Journaux complets : {SORTIES}")
    sys.exit(0 if ok == len(resultats) else 1)


if __name__ == "__main__":
    main()
