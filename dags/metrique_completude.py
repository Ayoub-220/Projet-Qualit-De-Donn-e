import csv
import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "metrique_completude")
SORTIE = os.path.join(OUTPUT_DIR, "audit_completude.csv")
TMP_ROOT = os.path.join(OUTPUT_DIR, ".tmp_metrique_completude")

# Perimetre exact de la section 3.3 du rapport (Paris/Evry Conso et Population).
SOURCES = {
    "Consommation_Paris": ("NB_KW_Jour", "ID_Adr"),
    "Consommation_Evry": ("NB_KW_Jour", "ID_Adr"),
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


def dossier_execution(context):
    """Chemin propre a chaque execution du DAG, commun a toutes les taches."""
    identifiant = hashlib.sha256(context["run_id"].encode("utf-8")).hexdigest()[:24]
    return os.path.join(TMP_ROOT, identifiant)


@contextmanager
def session_spark(nom):
    spark = SparkSession.builder.appName(nom).master("local[*]").getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def charger_source(source, **context):
    """Etape 1 : lire le CSV brut sans inference de types pour preserver les manquants."""
    fichier = os.path.join(DATA_DIR, source + ".csv")
    if not os.path.isfile(fichier):
        raise FileNotFoundError(f"Fichier source introuvable : {fichier}")
    colonnes_attendues = SOURCES[source]
    sortie = os.path.join(dossier_execution(context), source, "01_sources")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with session_spark("Completude_charger_" + source) as spark:
        donnees = spark.read.option("header", True).option("inferSchema", False).csv(fichier)
        absentes = [c for c in colonnes_attendues if c not in donnees.columns]
        if absentes:
            raise ValueError(f"Colonnes absentes de {source} : {absentes}. Disponibles : {donnees.columns}")
        # Conserver le nombre de lignes, y compris celles entierement nulles.
        donnees.select(*colonnes_attendues).write.mode("overwrite").parquet(sortie)


def preparer_indicateurs(source, **context):
    """Etape 2 : signaler NULL, chaine vide et NaN, sans filtrer de lignes."""
    base = os.path.join(dossier_execution(context), source)
    with session_spark("Completude_preparer_" + source) as spark:
        donnees = spark.read.parquet(os.path.join(base, "01_sources"))
        for attribut in SOURCES[source]:
            # CSV sans schema infere : tout est texte, 'NaN' est une valeur manquante.
            texte = F.trim(F.col(attribut).cast("string"))
            manquant = (
                F.col(attribut).isNull()
                | (texte == "")
                | F.lower(texte).isin("nan", "null")
            )
            donnees = donnees.withColumn("manquant_" + attribut, manquant)
        donnees.select(*("manquant_" + c for c in SOURCES[source])) \
            .write.mode("overwrite").parquet(os.path.join(base, "02_indicateurs"))


def calculer_taux(source, **context):
    """Etape 3 : mesurer les taux sur toutes les lignes, sans rejeter les incompletes."""
    base = os.path.join(dossier_execution(context), source)
    with session_spark("Completude_calculer_" + source) as spark:
        donnees = spark.read.parquet(os.path.join(base, "02_indicateurs"))
        agregations = [F.count(F.lit(1)).alias("total")]
        for attribut in SOURCES[source]:
            agregations.append(
                F.sum(F.when(F.col("manquant_" + attribut), 1).otherwise(0))
                .alias("manquants_" + attribut)
            )
        resultats = donnees.agg(*agregations).first()
        total = resultats["total"]
        categorie = "consommation" if "Consommation" in source else "population"
        mesures = []
        for attribut in SOURCES[source]:
            manquants = resultats["manquants_" + attribut] or 0
            # Aucune ligne = taux non defini (pas artificiellement 100 %).
            completude = round(100 * (total - manquants) / total, 2) if total else None
            manquants_pct = round(100 * manquants / total, 2) if total else None
            mesures.append({
                "type_mesure": (
                    "Valeurs manquantes métiers" if attribut == "NB_KW_Jour"
                    else "Clés primaires manquantes"
                ),
                "categorie": categorie,
                "table_auditee": source,
                "attribut_audite": attribut,
                "taux_completude_pct": completude,
                "taux_valeurs_manquantes_pct": manquants_pct,
            })
    os.makedirs(base, exist_ok=True)
    with open(os.path.join(base, "03_metriques.json"), "w", encoding="utf-8") as f:
        json.dump(mesures, f, ensure_ascii=False)


def exporter_metriques(**context):
    """Etape 4 : exporter un seul CSV, lisible directement par un dashboard."""
    base = dossier_execution(context)
    lignes = []
    for source in SOURCES:
        chemin = os.path.join(base, source, "03_metriques.json")
        with open(chemin, encoding="utf-8") as f:
            for mesure in json.load(f):
                lignes.append(mesure)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    temporaire = os.path.join(base, "metriques_completude.csv.tmp")
    with open(temporaire, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_EXPORT)
        writer.writeheader()
        writer.writerows(lignes)
    os.replace(temporaire, SORTIE)


def nettoyer_intermediaires(**context):
    """Etape 5 : supprimer uniquement les fichiers temporaires de cette execution."""
    shutil.rmtree(dossier_execution(context), ignore_errors=True)

with DAG(
    dag_id="metrique_completude",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["datascale", "qualite", "completude"],
    description="Mesure par table et attribut des taux de completude des sources Paris/Evry",
) as dag:

    def pipeline_source(source):
        chargement = PythonOperator(
            task_id=f"charger_{source.lower()}",
            python_callable=charger_source,
            op_kwargs={"source": source},
        )
        preparation = PythonOperator(
            task_id=f"identifier_manquants_{source.lower()}",
            python_callable=preparer_indicateurs,
            op_kwargs={"source": source},
        )
        calcul = PythonOperator(
            task_id=f"calculer_completude_{source.lower()}",
            python_callable=calculer_taux,
            op_kwargs={"source": source},
        )
        chargement >> preparation >> calcul
        return calcul
    fins = [pipeline_source(source) for source in SOURCES]
    
    export = PythonOperator(
        task_id="exporter_csv_metriques",
        python_callable=exporter_metriques,
    )
    nettoyage = PythonOperator(
        task_id="nettoyer_fichiers_temporaires",
        python_callable=nettoyer_intermediaires,
    )
    fins >> export >> nettoyage
