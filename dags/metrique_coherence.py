import csv
import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "coherence")
METRICS_FILE = os.path.join(OUTPUT_DIR, "audit_coherence.csv")
ANOMALIES_DIR = os.path.join(OUTPUT_DIR, "anomalies")

SOURCES = ("S1", "S2")
METRIC_COLUMNS = [
    "Nom_Mesure",
    "Table_Testee",
    "Nb_Lignes_Total",
    "Nb_Anomalies",
    "Taux_Coherence_Pct",
]


def spark_session(name):
    return (
        SparkSession.builder.appName(name)
        .master("local[2]")
        .getOrCreate()
    )


def source_dataframe(spark, source):
    consommation = spark.read.csv(
        os.path.join(DATA_DIR, f"{source}_Consommation.csv"),
        header=True,
        inferSchema=True,
    )
    population = spark.read.csv(
        os.path.join(DATA_DIR, f"{source}_Population.csv"),
        header=True,
        inferSchema=True,
    )
    iris = spark.read.csv(
        os.path.join(DATA_DIR, "S4_IRIS.csv"),
        header=True,
        inferSchema=True,
    )

    population = population.select(
        F.trim(F.col("Adresse").cast("string")).alias("_adresse"),
        F.trim(F.col("CSP").cast("string")).alias("_csp"),
    ).dropDuplicates(["_adresse"])

    iris = iris.select(
        F.upper(F.trim(F.col("ID_Rue").cast("string"))).alias("_rue"),
        F.trim(F.col("ID_Ville").cast("string")).alias("_ville"),
        F.col("ID_Iris").alias("_iris"),
    ).dropDuplicates(["_rue", "_ville"])

    return (
        consommation.withColumn(
            "_adresse", F.trim(F.col("ID_Adr").cast("string"))
        )
        .withColumn("_rue", F.upper(F.trim(F.col("Nom_Rue").cast("string"))))
        .withColumn("_ville", F.trim(F.col("Code_Postal").cast("string")))
        .join(population, "_adresse", "left")
        .join(iris, ["_rue", "_ville"], "left")
        .withColumn("_kw", F.col("NB_KW_Jour").cast("double"))
    )


def ecrire_csv_unique(dataframe, chemin):
    temporaire = chemin + ".tmp"
    shutil.rmtree(temporaire, ignore_errors=True)
    dataframe.coalesce(1).write.mode("overwrite").option("header", True).csv(
        temporaire
    )
    parties = [
        nom for nom in os.listdir(temporaire)
        if nom.startswith("part-") and nom.endswith(".csv")
    ]
    if len(parties) != 1:
        raise RuntimeError(f"Sortie Spark inattendue : {temporaire}")
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    os.replace(os.path.join(temporaire, parties[0]), chemin)
    shutil.rmtree(temporaire, ignore_errors=True)


def calculer_coherence():
    metrics = []
    spark = spark_session("DQ_Coherence")
    try:
        for source in SOURCES:
            data = source_dataframe(spark, source).cache()
            total = data.count()

            segments = data.filter(
                F.col("_kw").isNotNull() & F.col("_csp").isNotNull()
            )
            statistiques = segments.groupBy("_csp").agg(
                F.avg("_kw").alias("_moyenne"),
                F.stddev_pop("_kw").alias("_ecart_type"),
            )
            anomalies_csp = (
                segments.join(statistiques, "_csp")
                .withColumn(
                    "_zscore",
                    F.when(
                        F.col("_ecart_type") > 0,
                        F.abs(F.col("_kw") - F.col("_moyenne"))
                        / F.col("_ecart_type"),
                    ).otherwise(F.lit(0.0)),
                )
                .filter(F.col("_zscore") > 3)
                .drop("_moyenne", "_ecart_type", "_zscore")
            )
            anomalies_csp = anomalies_csp.dropDuplicates(["ID_Adr"])
            nb_csp = anomalies_csp.count()
            ecrire_csv_unique(
                anomalies_csp.select(
                    "ID_Adr", "Nom_Rue", "Code_Postal", "NB_KW_Jour", "_csp"
                ),
                os.path.join(ANOMALIES_DIR, f"{source}_csp.csv"),
            )
            metrics.append(
                {
                    "Nom_Mesure": "Cohérence_Consommation_CSP",
                    "Table_Testee": f"{source}_Consommation",
                    "Nb_Lignes_Total": total,
                    "Nb_Anomalies": nb_csp,
                    "Taux_Coherence_Pct": round(
                        100 * (total - nb_csp) / total, 2
                    ) if total else 0.0,
                }
            )

            anomalies_geo = data.filter(
                F.col("_iris").isNull()
            ).dropDuplicates(["ID_Adr"])
            nb_geo = anomalies_geo.count()
            ecrire_csv_unique(
                anomalies_geo.select(
                    "ID_Adr", "Nom_Rue", "Code_Postal", "_csp"
                ),
                os.path.join(ANOMALIES_DIR, f"{source}_geographie.csv"),
            )
            metrics.append(
                {
                    "Nom_Mesure": "Cohérence_Adresse_IRIS",
                    "Table_Testee": f"{source}_Consommation",
                    "Nb_Lignes_Total": total,
                    "Nb_Anomalies": nb_geo,
                    "Taux_Coherence_Pct": round(
                        100 * (total - nb_geo) / total, 2
                    ) if total else 0.0,
                }
            )
            data.unpersist()
    finally:
        spark.stop()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    temporaire = METRICS_FILE + ".tmp"
    with open(temporaire, "w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=METRIC_COLUMNS)
        writer.writeheader()
        writer.writerows(metrics)
    os.replace(temporaire, METRICS_FILE)


with DAG(
    dag_id="metrique_coherence",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    tags=["datascale", "qualite", "coherence"],
    description="Mesure la coherence des consommations avec les CSP et le referentiel IRIS",
) as dag:
    mesurer = PythonOperator(
        task_id="mesurer_coherence",
        python_callable=calculer_coherence,
    )
