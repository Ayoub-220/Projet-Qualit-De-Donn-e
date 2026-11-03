"""Airflow 2.10.2 + PySpark : mesure des doublons semantiques.
"""

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
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "metrique_doublon")
SORTIE = os.path.join(OUTPUT_DIR, "metriques_doublons.csv")
TMP_ROOT = os.path.join(OUTPUT_DIR, ".tmp_metrique_doublons")
SEUIL = 0.85  # Strictement superieur, conformement au rapport.

SOURCES = {
    "consommation": ("S1_Consommation", "S2_Consommation"),
    "population": ("S1_Population", "S2_Population"),
}
SCHEMAS = {
    "consommation": {"Nom_Rue": ("Nom_Rue",), "Code_Postal": ("Code_Postal",)},
    "population": {
        "Nom": ("Nom",),
        "Prenom": ("Prenom", "Prénom"),
        "Adresse": ("Adresse",),
    },
}


def dossier_execution(context):
    """Chemin partage par toutes les taches d'une meme execution Airflow."""
    run_id = context["run_id"]
    identifiant = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]
    return os.path.join(TMP_ROOT, identifiant)


@contextmanager
def session_spark(nom):
    spark = SparkSession.builder.appName(nom).master("local[*]").getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def lire_csv(spark, nom_source, colonnes):
    chemin = os.path.join(DATA_DIR, nom_source + ".csv")
    if not os.path.isfile(chemin):
        raise FileNotFoundError(f"Source introuvable : {chemin}")
    df = spark.read.option("header", True).option("inferSchema", False).csv(chemin)
    for canonique, alias in colonnes.items():
        colonne = next((x for x in alias if x in df.columns), None)
        if colonne is None:
            raise ValueError(f"Colonne {canonique!r} absente de {nom_source}: {df.columns}")
        if colonne != canonique:
            df = df.withColumnRenamed(colonne, canonique)
    return df.select(*colonnes.keys()).withColumn("source", F.lit(nom_source))


def normaliser(chaine):
    x = F.upper(F.trim(F.coalesce(chaine.cast("string"), F.lit(""))))
    x = F.translate(x, "ÀÂÄÁÇÉÈÊËÎÏÍÔÖÓÙÛÜÚŸ", "AAAACEEEEIIIOOOUUUUY")
    x = F.regexp_replace(x, r"[^A-Z0-9]+", " ")
    return F.trim(F.regexp_replace(x, r"\s+", " "))


def normaliser_rue(chaine):
    x = normaliser(chaine)
    x = F.regexp_replace(x, r"^R (?=\S)", "RUE ")
    x = F.regexp_replace(x, r"^AV (?=\S)", "AVENUE ")
    return F.regexp_replace(x, r"^BD (?=\S)", "BOULEVARD ")


def charger_sources(categorie, **context):
    """Etape 1 : charge et rassemble S1/S2 en Parquet temporaire."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    base = dossier_execution(context)
    with session_spark("Doublons_chargement_" + categorie) as spark:
        gauche, droite = SOURCES[categorie]
        spec = SCHEMAS[categorie]
        df = lire_csv(spark, gauche, spec).unionByName(lire_csv(spark, droite, spec))
        df.write.mode("overwrite").parquet(os.path.join(base, categorie, "01_sources"))


def preparer_donnees(categorie, **context):
    """Etape 2 : normalise et definit les blocs de comparaison."""
    base = os.path.join(dossier_execution(context), categorie)
    with session_spark("Doublons_preparation_" + categorie) as spark:
        df = spark.read.parquet(os.path.join(base, "01_sources"))
        if categorie == "consommation":
            df = df.withColumn("bloc", normaliser(F.col("Code_Postal")))
            df = df.withColumn("libelle_distinct", normaliser(F.col("Nom_Rue")))
            df = df.withColumn("valeur", normaliser_rue(F.col("Nom_Rue")))
            df = df.filter((F.length("bloc") > 0) & (F.length("libelle_distinct") > 0))
            # Plusieurs habitants d'une rue n'ajoutent PAS de nouveaux libelles.
            df = df.select("bloc", "libelle_distinct", "valeur").dropDuplicates(
                ["bloc", "libelle_distinct"]
            )
        else:
            df = df.withColumn("bloc", normaliser(F.col("Adresse")))
            nom = normaliser(F.col("Nom"))
            prenom = normaliser(F.col("Prenom"))
            df = df.filter((F.length("bloc") > 0) & (F.length(nom) > 0) & (F.length(prenom) > 0))
            df = df.withColumn("valeur", F.concat_ws(" ", nom, prenom)).select("bloc", "valeur")
            # On conserve ici les lignes distinctes d'individus : ne pas dedupliquer.
        df = df.withColumn("ligne_id", F.monotonically_increasing_id())
        df.write.mode("overwrite").parquet(os.path.join(base, "02_preparees"))


def comparer_donnees(categorie, **context):
    """Etape 3 : calcule la similarite des paires du meme bloc."""
    base = os.path.join(dossier_execution(context), categorie)
    with session_spark("Doublons_comparaison_" + categorie) as spark:
        df = spark.read.parquet(os.path.join(base, "02_preparees"))
        g, d = df.alias("g"), df.alias("d")
        condition = (
            (F.col("g.bloc") == F.col("d.bloc"))
            & (F.col("g.ligne_id") < F.col("d.ligne_id"))
        )
        if categorie == "consommation":
            # Jamais deux libelles identiques : ce sont des occurrences d'une rue.
            condition = condition & (F.col("g.libelle_distinct") != F.col("d.libelle_distinct"))
        paires = g.join(d, condition, "inner")
        a, b = F.col("g.valeur"), F.col("d.valeur")
        longueur = F.greatest(F.length(a), F.length(b))
        similarite = F.when(longueur > 0, 1.0 - F.levenshtein(a, b) / longueur)
        # Pas d'export des paires : seulement une donnee intermediaire pour calculer le taux.
        paires.select((similarite > F.lit(SEUIL)).alias("est_similaire")) \
            .write.mode("overwrite").parquet(os.path.join(base, "03_comparaisons"))


def calculer_metrique(categorie, **context):
    """Etape 4 : calcule le taux puis enregistre une seule petite ligne JSON temporaire."""
    base = os.path.join(dossier_execution(context), categorie)
    with session_spark("Doublons_mesure_" + categorie) as spark:
        df = spark.read.parquet(os.path.join(base, "03_comparaisons"))
        compte = df.agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("est_similaire"), 1).otherwise(0)).alias("similaires"),
        ).first()
        total = compte["total"]
        taux = round(100 * (compte["similaires"] or 0) / total, 2) if total else None

    ligne = {
        "facteur_qualite": "doublons_semantiques",
        "categorie": categorie,
        "tables_auditees": ";".join(SOURCES[categorie]),
        "attributs_audites": "Nom_Rue" if categorie == "consommation" else "Nom;Prenom",
        "cle_regroupement": "Code_Postal" if categorie == "consommation" else "Adresse",
        "taux_similarite_pct": taux,
        "seuil_similarite": SEUIL,
        "date_mesure": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    os.makedirs(base, exist_ok=True)
    with open(os.path.join(base, "04_metrique.json"), "w", encoding="utf-8") as f:
        json.dump(ligne, f, ensure_ascii=False)


def exporter_metriques(**context):
    """Etape 5 : un SEUL CSV (deux lignes) pour le futur dashboard."""
    base = dossier_execution(context)
    lignes = []
    for categorie in ("consommation", "population"):
        with open(os.path.join(base, categorie, "04_metrique.json"), encoding="utf-8") as f:
            lignes.append(json.load(f))
    colonnes = [
        "facteur_qualite", "categorie", "tables_auditees", "attributs_audites",
        "cle_regroupement", "taux_similarite_pct", "seuil_similarite", "date_mesure",
    ]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    temporaire = SORTIE + ".tmp"
    with open(temporaire, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=colonnes)
        writer.writeheader()
        writer.writerows(lignes)
    os.replace(temporaire, SORTIE)


def nettoyer_intermediaires(**context):
    """Etape 6 : elimine Parquet et JSON apres export reussi."""
    shutil.rmtree(dossier_execution(context), ignore_errors=True)


with DAG(
    dag_id="metrique_doublons_semantiques",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,  # Evite deux executions ecrivant le meme CSV final.
    tags=["datascale", "qualite", "doublons"],
    description="Mesure des doublons semantiques : pipeline detaille et CSV unique",
) as dag:
    def etapes(categorie):
        lecture = PythonOperator(
            task_id=f"charger_sources_{categorie}",
            python_callable=charger_sources,
            op_kwargs={"categorie": categorie},
        )
        preparation = PythonOperator(
            task_id=f"normaliser_{categorie}",
            python_callable=preparer_donnees,
            op_kwargs={"categorie": categorie},
        )
        comparaison = PythonOperator(
            task_id=f"comparer_{categorie}",
            python_callable=comparer_donnees,
            op_kwargs={"categorie": categorie},
        )
        calcul = PythonOperator(
            task_id=f"calculer_taux_{categorie}",
            python_callable=calculer_metrique,
            op_kwargs={"categorie": categorie},
        )
        lecture >> preparation >> comparaison >> calcul
        return calcul

    fin_consommation = etapes("consommation")
    fin_population = etapes("population")
    export = PythonOperator(task_id="exporter_csv_metriques", python_callable=exporter_metriques)
    nettoyage = PythonOperator(task_id="nettoyer_fichiers_temporaires", python_callable=nettoyer_intermediaires)
    [fin_consommation, fin_population] >> export >> nettoyage
