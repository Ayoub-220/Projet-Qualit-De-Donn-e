"""ETL DataScale - Airflow 2.10.2 / Spark SQL.

3 groupes x 3 taches : verifier -> transformer/agreger -> publier.
Le mapping est ecrit en SQL et applique tel quel : aucune correction de qualite ici,
les anomalies des sources restent donc visibles dans les tables cibles.
Seule la preparation technique est faite : decimales converties et cles de jointure
normalisees. Les corrections se font a part, sur les sources : il suffit ensuite de
relancer ce DAG, sans changer les requetes.
"""
from datetime import datetime
import csv
import glob
import hashlib
import os
import shutil

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
# Les premiers noms sont ceux des CSV transmis ; les seconds permettent d'utiliser
# egalement le jeu de CSV S1/S2/S3/S4 deja partage dans le projet.
FICHIERS = {
    "conso_paris": ("Consommation_Paris.csv", "S1_Consommation.csv"),
    "conso_evry": ("Consommation_Evry.csv", "S2_Consommation.csv"),
    "pop_paris": ("Population_Paris.csv", "S1_Population.csv"),
    "pop_evry": ("Population_Evry.csv", "S2_Population.csv"),
    "iris": ("IRIS.csv", "S4_IRIS.csv"),
    "csp": ("CSP.csv", "S3_CSP.csv"),
}
COLONNES = {
    "conso_paris": ("ID_Adr", "N", "Nom_Rue", "Code_Postal", "NB_KW_Jour"),
    "conso_evry": ("ID_Adr", "N", "Nom_Rue", "Code_Postal", "NB_KW_Jour"),
    "pop_paris": ("ID_Personne", "Adresse", "CSP"),
    "pop_evry": ("ID_Personne", "Adresse", "CSP"),
    "iris": ("ID_Rue", "ID_Ville", "ID_IRIS"),
    "csp": ("ID_CSP", "Salair_Moyen"),
}
SOURCES = {
    "paris": ("conso_paris", "iris"),
    "evry": ("conso_evry", "iris"),
    "csp": ("pop_paris", "conso_paris", "pop_evry", "conso_evry", "csp"),
}
CIBLES = {
    "paris": "Consommation_IRIS_Paris.csv",
    "evry": "Consommation_IRIS_Evry.csv",
    "csp": "Consommation_CSP.csv",
}
VUES = {
    "conso_paris": "Consommation_Paris",
    "conso_evry": "Consommation_Evry",
    "pop_paris": "Population_Paris",
    "pop_evry": "Population_Evry",
    "iris": "IRIS",
    "csp": "CSP",
}


def cle_texte(colonne):
    """Expression SQL ignorant la casse, les accents et les espaces (pour les jointures)."""
    return (f"REPLACE(TRANSLATE(LOWER({colonne}), "
            "'àâäáçéèêëîïíôöóùûüúÿ', 'aaaaceeeeiiiooouuuuy'), ' ', '')")


def conso_preparee(vue):
    return f"""
        SELECT ID_Adr, N, Nom_Rue, Code_Postal,
               CAST(REPLACE(NB_KW_Jour, ',', '.') AS DOUBLE) AS NB_KW_Jour,
               {cle_texte('Nom_Rue')} AS Rue_Cle
        FROM {vue}_Brut"""


def population_preparee(vue):
    return f"""
        SELECT ID_Personne, Adresse, CSP, {cle_texte('Adresse')} AS Adresse_Cle
        FROM {vue}_Brut"""


# Preparation technique uniquement : une vue SQL par source, sans ecarter aucune ligne.
PREPARATIONS = {
    "conso_paris": conso_preparee("Consommation_Paris"),
    "conso_evry": conso_preparee("Consommation_Evry"),
    "pop_paris": population_preparee("Population_Paris"),
    "pop_evry": population_preparee("Population_Evry"),
    "iris": "SELECT ID_Rue, ID_Ville, ID_IRIS FROM IRIS_Brut",
    "csp": """
        SELECT ID_CSP, CAST(REPLACE(Salair_Moyen, ',', '.') AS DOUBLE) AS Salair_Moyen
        FROM CSP_Brut""",
}

# Memes jointures que le SQL du README.
REQUETES = {
    "paris": """
        SELECT
            i.ID_IRIS,
            CAST(AVG(c.NB_KW_Jour) * 365 AS DECIMAL(12, 2)) AS Conso_moyenne_annuelle
        FROM Consommation_Paris c
        JOIN IRIS i
            ON LOWER(TRIM(c.Nom_Rue)) = LOWER(TRIM(i.ID_Rue))
           AND i.ID_Ville = 'Paris'
        GROUP BY i.ID_IRIS""",
    "evry": """
        SELECT
            i.ID_IRIS,
            CAST(AVG(c.NB_KW_Jour) * 365 AS DECIMAL(12, 2)) AS Conso_moyenne_annuelle
        FROM Consommation_Evry c
        JOIN IRIS i
            ON LOWER(TRIM(c.Nom_Rue)) LIKE ('% ' || LOWER(TRIM(i.ID_Rue)))
           AND i.ID_Ville = 'Evry-Courcouronnes'
        GROUP BY i.ID_IRIS""",
    "csp": """
        WITH Population_Consommation AS (
            -- Paris : Nom_Rue ne contient pas le type de voie
            SELECT p.CSP, c.NB_KW_Jour
            FROM Population_Paris p
            JOIN Consommation_Paris c
              ON p.Adresse_Cle = 'paris,' || c.N || 'rue' || c.Rue_Cle
            UNION ALL
            -- Evry : Nom_Rue contient deja le type de voie
            SELECT p.CSP, c.NB_KW_Jour
            FROM Population_Evry p
            JOIN Consommation_Evry c
              ON p.Adresse_Cle = 'evry-courcouronnes,91000,' || c.N || c.Rue_Cle
        )
        SELECT
            csp.ID_CSP,
            CAST(AVG(pc.NB_KW_Jour) * 365 AS DECIMAL(12, 2)) AS Conso_moyenne_annuelle,
            CAST(csp.Salair_Moyen AS DECIMAL(12, 2)) AS Salaire_Moyen
        FROM Population_Consommation pc
        JOIN CSP csp ON LOWER(TRIM(pc.CSP)) = LOWER(TRIM(csp.ID_CSP))
        GROUP BY csp.ID_CSP, csp.Salair_Moyen""",
}


def chemin_source(cle):
    for nom in FICHIERS[cle]:
        chemin = os.path.join(DATA_DIR, nom)
        if os.path.isfile(chemin):
            return chemin
    raise FileNotFoundError(
        f"Source '{cle}' absente. Noms attendus : {FICHIERS[cle]}. "
        f"Fichiers presents : {sorted(os.listdir(DATA_DIR)) if os.path.isdir(DATA_DIR) else 'dossier absent'}"
    )


def verifier_sources(branche):
    """Etape legere : existence, separateur et colonnes des fichiers."""
    for cle in SOURCES[branche]:
        chemin = chemin_source(cle)
        with open(chemin, "r", encoding="utf-8-sig", newline="") as f:
            entete = next(csv.reader(f, delimiter=";"), [])
        entete = [c.strip().lstrip("﻿") for c in entete]
        # Autoriser l'orthographe corrigee de Salaire_Moyen.
        attentes = ["Salaire_Moyen" if c == "Salair_Moyen" and "Salaire_Moyen" in entete else c
                    for c in COLONNES[cle]]
        manquantes = [c for c in attentes if c not in entete]
        if manquantes:
            raise ValueError(f"Colonnes absentes dans {chemin}: {manquantes}; en-tete lu : {entete}. "
                             "Les CSV fournis sont separes par des points-virgules.")
        print(f"Source OK : {chemin} ({len(entete)} colonnes)")
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def lire(spark, cle):
    df = (spark.read.option("header", True)
          .option("sep", ";")
          .option("inferSchema", False)
          .option("encoding", "UTF-8")
          .csv(chemin_source(cle)))
    # Enlever un eventuel BOM et les espaces dans les en-tetes.
    for ancien in df.columns:
        nouveau = ancien.strip().lstrip("﻿")
        if ancien != nouveau:
            df = df.withColumnRenamed(ancien, nouveau)
    if cle == "csp" and "Salaire_Moyen" in df.columns:
        df = df.withColumnRenamed("Salaire_Moyen", "Salair_Moyen")
    return df


def creer_vues(spark, branche):
    """Expose chaque source de la branche comme une table SQL."""
    for cle in SOURCES[branche]:
        lire(spark, cle).createOrReplaceTempView(f"{VUES[cle]}_Brut")
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {VUES[cle]} AS {PREPARATIONS[cle]}")


def transformer(spark, branche):
    creer_vues(spark, branche)
    return spark.sql(REQUETES[branche])


def dossier_temp(branche, run_id):
    ident = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:20]
    return os.path.join(OUTPUT_DIR, ".tmp_etl", ident, branche)


def transformer_et_ecrire(branche, **context):
    temp = dossier_temp(branche, context["run_id"])
    os.makedirs(os.path.dirname(temp), exist_ok=True)
    spark = (SparkSession.builder
             .appName("DQ_ETL_" + branche)
             .master("local[2]")  # Eviter 3 sessions local[*] simultanees.
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    try:
        resultat = transformer(spark, branche)
        resultat.coalesce(1).write.mode("overwrite").option("header", True).csv(temp)
        print(f"Traitement {branche} termine; lignes produites : {resultat.count()}")
    finally:
        spark.stop()


def publier_csv(branche, **context):
    temp = dossier_temp(branche, context["run_id"])
    fichiers = glob.glob(os.path.join(temp, "part-*.csv"))
    if len(fichiers) != 1:
        raise RuntimeError(f"Fichier Spark manquant dans {temp}: {fichiers}")
    destination = os.path.join(OUTPUT_DIR, CIBLES[branche])
    os.replace(fichiers[0], destination)
    shutil.rmtree(temp)
    parent = os.path.dirname(temp)
    try:
        os.rmdir(parent)
        os.rmdir(os.path.dirname(parent))
    except OSError:
        pass
    print("CSV publie :", destination)


with DAG(
    dag_id="etl_consommation_villes",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["datascale", "etl", "mappings"],
) as dag:
    for branche in ("paris", "evry", "csp"):
        with TaskGroup(group_id=f"etl_{branche}"):
            verification = PythonOperator(
                task_id="verifier_sources", python_callable=verifier_sources,
                op_kwargs={"branche": branche})
            transformation = PythonOperator(
                task_id="transformer_et_agreger", python_callable=transformer_et_ecrire,
                op_kwargs={"branche": branche})
            publication = PythonOperator(
                task_id="publier_csv", python_callable=publier_csv,
                op_kwargs={"branche": branche})
            verification >> transformation >> publication
