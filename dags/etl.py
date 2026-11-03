from datetime import datetime
import glob
import os
import shutil

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lower, round as spark_round, trim

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

SOURCES = {
    "paris": ["S1_Consommation.csv", "S4_IRIS.csv"],
    "evry": ["S2_Consommation.csv", "S4_IRIS.csv"],
    "csp": [
        "S1_Population.csv", "S1_Consommation.csv",
        "S2_Population.csv", "S2_Consommation.csv", "S3_CSP.csv",
    ],
}

FICHIERS_CIBLES = {
    "paris": "Consommation_IRIS_Paris.csv",
    "evry": "Consommation_IRIS_Evry.csv",
    "csp": "Consommation_CSP.csv",
}


def verifier_sources(branche):
    """Verification rapide, sans lancer Spark."""
    for fichier in SOURCES[branche]:
        chemin = os.path.join(DATA_DIR, fichier)
        if not os.path.isfile(chemin):
            raise FileNotFoundError(f"Source introuvable : {chemin}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def transformer_iris(spark, branche):
    nom_source = SOURCES[branche][0]
    consommation = spark.read.csv(
        os.path.join(DATA_DIR, nom_source), header=True, inferSchema=True
    )
    iris = spark.read.csv(
        os.path.join(DATA_DIR, "S4_IRIS.csv"), header=True, inferSchema=True
    )

    # Condition identique a celle du DAG initial : Code_Postal = ID_Ville.
    # A verifier sur les valeurs de S4_IRIS, sans la modifier lors du refactoring.
    jointure = consommation.join(
        iris,
        (lower(trim(consommation.Nom_Rue)) == lower(trim(iris.ID_Rue)))
        & (trim(consommation.Code_Postal) == trim(iris.ID_Ville)),
        "inner",
    )
    return jointure.groupBy("ID_Iris").agg(
        spark_round(avg(col("NB_KW_Jour")) * 365, 2).alias("Conso_moyenne_annuelle")
    ).select(
        col("ID_Iris").alias("ID_IRIS"), col("Conso_moyenne_annuelle")
    )


def transformer_csp(spark):
    s1_pop = spark.read.csv(os.path.join(DATA_DIR, "S1_Population.csv"), header=True, inferSchema=True)
    s1_conso = spark.read.csv(os.path.join(DATA_DIR, "S1_Consommation.csv"), header=True, inferSchema=True)
    s2_pop = spark.read.csv(os.path.join(DATA_DIR, "S2_Population.csv"), header=True, inferSchema=True)
    s2_conso = spark.read.csv(os.path.join(DATA_DIR, "S2_Consommation.csv"), header=True, inferSchema=True)
    csp = spark.read.csv(os.path.join(DATA_DIR, "S3_CSP.csv"), header=True, inferSchema=True)

    s1 = s1_pop.join(
        s1_conso, lower(trim(s1_pop.Adresse)) == lower(trim(s1_conso.ID_Adr)), "inner"
    ).select(s1_pop.CSP.alias("CSP"), s1_conso.NB_KW_Jour.alias("NB_KW_Jour"))
    s2 = s2_pop.join(
        s2_conso, lower(trim(s2_pop.Adresse)) == lower(trim(s2_conso.ID_Adr)), "inner"
    ).select(s2_pop.CSP.alias("CSP"), s2_conso.NB_KW_Jour.alias("NB_KW_Jour"))

    toutes = s1.unionByName(s2)
    jointure = toutes.join(
        csp, lower(trim(toutes.CSP)) == lower(trim(csp.ID_CSP)), "inner"
    )
    return jointure.groupBy(csp.ID_CSP, csp.Salaire_Moyen).agg(
        spark_round(avg(col("NB_KW_Jour")) * 365, 2).alias("Conso_moyenne_annuelle")
    ).select(
        col("ID_CSP").alias("ID_CSP"),
        col("Conso_moyenne_annuelle"),
        col("Salaire_Moyen"),
    )


def transformer_et_ecrire(branche, **context):
    """Une seule session Spark par branche et par execution."""
    # Repertoire isole par execution : evite la collision entre DAG runs.
    run_id = context["run_id"].replace("/", "_").replace(":", "_")
    dossier_temp = os.path.join(OUTPUT_DIR, ".tmp_etl", run_id, branche)
    os.makedirs(os.path.dirname(dossier_temp), exist_ok=True)

    spark = SparkSession.builder.appName(f"ETL_{branche}_Spark").master("local[*]").getOrCreate()
    try:
        resultat = transformer_csp(spark) if branche == "csp" else transformer_iris(spark, branche)
        resultat.coalesce(1).write.mode("overwrite").csv(dossier_temp, header=True)
    finally:
        spark.stop()


def publier_csv(branche, **context):
    """Renommer le fichier Spark sans relancer Spark."""
    run_id = context["run_id"].replace("/", "_").replace(":", "_")
    dossier_temp = os.path.join(OUTPUT_DIR, ".tmp_etl", run_id, branche)
    fichiers = glob.glob(os.path.join(dossier_temp, "part-*.csv"))
    if len(fichiers) != 1:
        raise RuntimeError(f"Un seul fichier CSV attendu dans {dossier_temp}, trouve : {len(fichiers)}")

    destination = os.path.join(OUTPUT_DIR, FICHIERS_CIBLES[branche])
    os.replace(fichiers[0], destination)
    shutil.rmtree(dossier_temp)
    # Supprimer uniquement les parents vides, sans toucher aux autres branches.
    for parent in (os.path.dirname(dossier_temp), os.path.dirname(os.path.dirname(dossier_temp))):
        try:
            os.rmdir(parent)
        except OSError:
            pass
    print(f"CSV publie : {destination}")


with DAG(
    dag_id="etl_consommation_villes",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,  # Evite de lancer trois sessions Spark lourdes en meme temps.
    tags=["datascale", "etl"],
) as dag:
    for branche in ("paris", "evry", "csp"):
        with TaskGroup(group_id=f"etl_{branche}"):
            verifier = PythonOperator(
                task_id="verifier_sources",
                python_callable=verifier_sources,
                op_kwargs={"branche": branche},
            )
            transformer = PythonOperator(
                task_id="transformer_et_agreger",
                python_callable=transformer_et_ecrire,
                op_kwargs={"branche": branche},
            )
            publier = PythonOperator(
                task_id="publier_csv",
                python_callable=publier_csv,
                op_kwargs={"branche": branche},
            )
            verifier >> transformer >> publier
