from datetime import datetime
import csv
import os
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "heterogeneite")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "audit_heterogeneite.csv")

SOURCES = {
    "S1_Consommation": "S1_Consommation.csv",
    "S2_Consommation": "S2_Consommation.csv",
}
COLONNES_CSV = [
    "Nom_Mesure",
    "Dimension",
    "Table_Testee",
    "Nb_Lignes_Total",
    "Nb_Erreurs",
    "Taux_Conformite_Pct",
]


def verifier_source(table):
    """Verification legere, sans demarrer Spark."""
    path = os.path.join(DATA_DIR, SOURCES[table])
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        entete = next(csv.reader(f), [])
    if "NB_KW_Jour" not in entete:
        raise ValueError(f"Colonne NB_KW_Jour absente de {table}: {entete}")
    print(f"Source verifiee : {table} ({path})")


def calculer_heterogeneite(table):
    """Une session Spark par table et une seule aggregation pour limiter les scans."""
    path = os.path.join(DATA_DIR, SOURCES[table])
    spark = (
        SparkSession.builder
        .appName(f"DQ_Heterogeneite_{table}")
        .master("local[2]")
        .getOrCreate()
    )
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        kw = F.col("NB_KW_Jour").cast("double")
        anomalie = (kw < F.lit(0)) | (kw > F.lit(100000))
        stats = df.agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(anomalie, 1).otherwise(0)).alias("erreurs"),
        ).first()
        return {"table": table, "total": int(stats["total"]), "erreurs": int(stats["erreurs"] or 0)}
    finally:
        spark.stop()


def valider_et_calculer_taux(table, task_id_calcul, **context):
    """Controle les effectifs et calcule le pourcentage, sans demarrer Spark."""
    stats = context["ti"].xcom_pull(task_ids=task_id_calcul)
    if not stats or stats.get("table") != table:
        raise ValueError(f"Resultat absent ou incorrect pour {table}")
    total = stats["total"]
    erreurs = stats["erreurs"]
    if not (0 <= erreurs <= total):
        raise ValueError(f"Nombre d'erreurs incoherent pour {table}: {erreurs}/{total}")
    taux = round(100 * (total - erreurs) / total, 2) if total else 0.0
    print(f"{table} : {taux}% de valeurs conformes ({erreurs}/{total} anomalies)")
    return {
        "Nom_Mesure": "Heterogeneite_Echelle_KW",
        "Dimension": "Heterogeneite",
        "Table_Testee": table,
        "Nb_Lignes_Total": total,
        "Nb_Erreurs": erreurs,
        "Taux_Conformite_Pct": taux,
    }


def exporter_csv(task_ids_validation, **context):
    """Une seule tache ecrit le CSV, sans risque d'ecriture concurrente."""
    lignes = []
    for task_id in task_ids_validation:
        ligne = context["ti"].xcom_pull(task_ids=task_id)
        if not ligne:
            raise ValueError(f"Mesure manquante : {task_id}")
        lignes.append(ligne)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fichier_tmp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=OUTPUT_DIR,
            prefix=".audit_heterogeneite_", suffix=".csv", delete=False,
        ) as f:
            fichier_tmp = f.name
            writer = csv.DictWriter(f, fieldnames=COLONNES_CSV)
            writer.writeheader()
            writer.writerows(lignes)
        os.replace(fichier_tmp, OUTPUT_CSV)
    finally:
        if fichier_tmp and os.path.exists(fichier_tmp):
            os.remove(fichier_tmp)
    print(f"CSV final : {OUTPUT_CSV}")


with DAG(
    dag_id="mesure_heterogeneite",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["datascale", "heterogeneite"],
    description="Mesure de l'heterogeneite d'echelle dans S1/S2 Consommation",
) as dag:
    validations = []
    for table, groupe in [
        ("S1_Consommation", "consommation_s1"),
        ("S2_Consommation", "consommation_s2"),
    ]:
        with TaskGroup(group_id=groupe) as tg:
            verifier = PythonOperator(
                task_id="verifier_source",
                python_callable=verifier_source,
                op_kwargs={"table": table},
            )
            calculer = PythonOperator(
                task_id="calculer_heterogeneite_echelle",
                python_callable=calculer_heterogeneite,
                op_kwargs={"table": table},
            )
            valider = PythonOperator(
                task_id="valider_et_calculer_taux",
                python_callable=valider_et_calculer_taux,
                op_kwargs={
                    "table": table,
                    "task_id_calcul": f"{groupe}.calculer_heterogeneite_echelle",
                },
            )
            verifier >> calculer >> valider
        validations.append(valider)

    exporter = PythonOperator(
        task_id="exporter_csv_heterogeneite",
        python_callable=exporter_csv,
        op_kwargs={
            "task_ids_validation": [
                "consommation_s1.valider_et_calculer_taux",
                "consommation_s2.valider_et_calculer_taux",
            ],
        },
    )
    for valider in validations:
        valider >> exporter
