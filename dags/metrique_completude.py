import csv
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim


DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "dq_results", "completude")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "audit_completude.csv")

SOURCES = {
    "S1_Consommation": (
        os.path.join(DATA_DIR, "S1_Consommation.csv"),
        ["ID_Adr", "Nom_Rue", "Code_Postal", "NB_KW_Jour"],
    ),
    "S2_Consommation": (
        os.path.join(DATA_DIR, "S2_Consommation.csv"),
        ["ID_Adr", "Nom_Rue", "Code_Postal", "NB_KW_Jour"],
    ),
    "S1_Population": (
        os.path.join(DATA_DIR, "S1_Population.csv"),
        ["Adresse", "CSP"],
    ),
    "S2_Population": (
        os.path.join(DATA_DIR, "S2_Population.csv"),
        ["Adresse", "CSP"],
    ),
    "S3_CSP": (
        os.path.join(DATA_DIR, "S3_CSP.csv"),
        ["ID_CSP", "Desc", "Salaire_Moyen"],
    ),
    "S4_IRIS": (
        os.path.join(DATA_DIR, "S4_IRIS.csv"),
        ["ID_Rue", "ID_Ville", "ID_Iris"],
    ),
}


def est_renseignee(nom_colonne):
    valeur = trim(col(nom_colonne).cast("string"))
    return (
        col(nom_colonne).isNotNull()
        & (valeur != "")
        & (lower(valeur) != "nan")
    )


def mesurer_completude():
    spark = (
        SparkSession.builder.appName("DQ_Completude")
        .master("local[*]")
        .getOrCreate()
    )
    resultats = []

    try:
        for source, (chemin, colonnes) in SOURCES.items():
            df = spark.read.csv(chemin, header=True, inferSchema=True)
            total = df.count()

            for nom_colonne in colonnes:
                renseignees = df.filter(est_renseignee(nom_colonne)).count()
                manquantes = total - renseignees
                taux = round(100 * renseignees / total, 2) if total else 0.0
                resultats.append(
                    {
                        "Nom_Mesure": "Taux_Completude",
                        "Dimension": "Complétude",
                        "Table_Testee": source,
                        "Colonne_Testee": nom_colonne,
                        "Nb_Lignes_Total": total,
                        "Nb_Valeurs_Renseignees": renseignees,
                        "Nb_Valeurs_Manquantes": manquantes,
                        "Taux_Completude_Pct": taux,
                    }
                )
    finally:
        spark.stop()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    colonnes_sortie = [
        "Nom_Mesure",
        "Dimension",
        "Table_Testee",
        "Colonne_Testee",
        "Nb_Lignes_Total",
        "Nb_Valeurs_Renseignees",
        "Nb_Valeurs_Manquantes",
        "Taux_Completude_Pct",
    ]
    temporaire = OUTPUT_FILE + ".tmp"
    with open(temporaire, "w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=colonnes_sortie)
        writer.writeheader()
        writer.writerows(resultats)
    os.replace(temporaire, OUTPUT_FILE)


with DAG(
    dag_id="metrique_completude",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    tags=["datascale", "qualite", "completude"],
    description="Mesure le taux de valeurs renseignées des fichiers sources",
) as dag:
    mesurer = PythonOperator(
        task_id="mesurer_completude",
        python_callable=mesurer_completude,
    )
