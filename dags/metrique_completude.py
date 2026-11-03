import csv
import os
from contextlib import contextmanager
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "completude")
SORTIE = os.path.join(OUTPUT_DIR, "audit_completude.csv")

SOURCES = {
    "S1_Consommation": ("NB_KW_Jour", "ID_Adr"),
    "S2_Consommation": ("NB_KW_Jour", "ID_Adr"),
    "S1_Population": ("Adresse",),
    "S2_Population": ("Adresse",),
}

COLONNES_EXPORT = [
    "type_mesure",
    "categorie",
    "table_auditee",
    "attribut_audite",
    "taux_completude_pct",
    "taux_valeurs_manquantes_pct",
]


@contextmanager
def session_spark(nom):
    spark = SparkSession.builder.appName(nom).master("local[*]").getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def verifier_source(source):
    """Tâche 1 : vérifier le fichier et son en-tête, sans démarrer Spark."""
    chemin = os.path.join(DATA_DIR, source + ".csv")
    if not os.path.isfile(chemin):
        raise FileNotFoundError(f"Fichier source introuvable : {chemin}")
    with open(chemin, "r", newline="", encoding="utf-8-sig") as fichier:
        entete = next(csv.reader(fichier), [])
    absentes = [c for c in SOURCES[source] if c not in entete]
    if absentes:
        raise ValueError(f"Colonnes absentes de {source} : {absentes}. Présentes : {entete}")
    print(f"Source vérifiée : {source} ({', '.join(SOURCES[source])})")


def mesurer_completude(source):
    """Tâche 2 : lecture, détection et agrégation en UNE session Spark.

    Seul un petit objet Python (1 ou 2 lignes de métriques) est retourné en XCom.
    """
    chemin = os.path.join(DATA_DIR, source + ".csv")
    attributs = SOURCES[source]
    with session_spark("DQ_Completude_" + source) as spark:
        donnees = spark.read.option("header", True).option("inferSchema", False).csv(chemin)
        absentes = [c for c in attributs if c not in donnees.columns]
        if absentes:
            raise ValueError(f"Colonnes absentes de {source} : {absentes}")

        agregations = [F.count(F.lit(1)).alias("total")]
        for attribut in attributs:
            texte = F.trim(F.col(attribut).cast("string"))
            manquant = (
                F.col(attribut).isNull()
                | (texte == "")
                | F.lower(texte).isin("nan", "null")
            )
            agregations.append(
                F.sum(F.when(manquant, F.lit(1)).otherwise(F.lit(0))).alias("manquants_" + attribut)
            )
        resultat = donnees.agg(*agregations).first()

    total = int(resultat["total"])
    categorie = "consommation" if "Consommation" in source else "population"
    lignes = []
    for attribut in attributs:
        nb_manquants = int(resultat["manquants_" + attribut] or 0)
        lignes.append({
            "type_mesure": "Valeurs manquantes métiers" if attribut == "NB_KW_Jour" else "Clés primaires manquantes",
            "categorie": categorie,
            "table_auditee": source,
            "attribut_audite": attribut,
            "taux_completude_pct": round(100 * (total - nb_manquants) / total, 2) if total else None,
            "taux_valeurs_manquantes_pct": round(100 * nb_manquants / total, 2) if total else None,
        })
    return lignes


def valider_resultats(source, **context):
    """Tâche 3 : contrôle des valeurs calculées, sans Spark ni fichier temporaire."""
    ti = context["ti"]
    lignes = ti.xcom_pull(task_ids=f"calculer_completude_{source.lower()}")
    if not isinstance(lignes, list) or len(lignes) != len(SOURCES[source]):
        raise ValueError(f"Résultat incomplet ou absent pour {source}")
    for ligne in lignes:
        if ligne["table_auditee"] != source or ligne["attribut_audite"] not in SOURCES[source]:
            raise ValueError(f"Ligne incohérente pour {source} : {ligne}")
        completude = ligne["taux_completude_pct"]
        manquantes = ligne["taux_valeurs_manquantes_pct"]
        if completude is not None:
            if manquantes is None or not (0 <= completude <= 100 and 0 <= manquantes <= 100):
                raise ValueError(f"Taux hors bornes : {ligne}")
            if abs(completude + manquantes - 100) > 0.02:
                raise ValueError(f"Taux incohérents : {ligne}")
    return lignes


def exporter_metriques(**context):
    """Tâche finale : fichier unique pour les dashboards, sans dossier temporaire."""
    ti = context["ti"]
    lignes = []
    for source in SOURCES:
        mesures = ti.xcom_pull(task_ids=f"valider_resultats_{source.lower()}")
        if not isinstance(mesures, list) or len(mesures) != len(SOURCES[source]):
            raise ValueError(f"Mesures manquantes pour {source}")
        lignes.extend(mesures)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # L'écriture directe évite tout fichier ou dossier temporaire.
    with open(SORTIE, "w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=COLONNES_EXPORT)
        writer.writeheader()
        writer.writerows(lignes)
    print(f"Export de {len(lignes)} mesures : {SORTIE}")


with DAG(
    dag_id="metrique_completude",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["datascale", "qualite", "completude"],
    description="Mesure de la complétude S1/S2, sans fichiers intermédiaires",
) as dag:

    fins = []
    for source in SOURCES:
        identifiant = source.lower()
        verification = PythonOperator(
            task_id=f"verifier_source_{identifiant}",
            python_callable=verifier_source,
            op_kwargs={"source": source},
        )
        calcul = PythonOperator(
            task_id=f"calculer_completude_{identifiant}",
            python_callable=mesurer_completude,
            op_kwargs={"source": source},
        )
        validation = PythonOperator(
            task_id=f"valider_resultats_{identifiant}",
            python_callable=valider_resultats,
            op_kwargs={"source": source},
        )
        verification >> calcul >> validation
        fins.append(validation)

    export = PythonOperator(
        task_id="exporter_csv_metriques",
        python_callable=exporter_metriques,
    )
    fins >> export