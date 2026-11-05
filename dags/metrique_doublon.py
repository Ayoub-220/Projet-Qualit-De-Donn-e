"""Doublons semantiques mesures sur les valeurs brutes des sources.

Aucune normalisation ni correction : les textes sont compares tels quels.
La distance est calculee par la fonction levenshtein de Spark SQL.
"""
import csv
import os
from contextlib import contextmanager
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "doublons")
SORTIE = os.path.join(OUTPUT_DIR, "audit_doublons.csv")
SEUIL = 0.85  # Similarite STRICTEMENT superieure a 0,85.

# Meme convention de fichiers que l'ETL. On accepte aussi les noms S1/S2.
SOURCES = {
    "Consommation_Paris": ("Consommation_Paris.csv", "S1_Consommation.csv"),
    "Consommation_Evry": ("Consommation_Evry.csv", "S2_Consommation.csv"),
    "Population_Paris": ("Population_Paris.csv", "S1_Population.csv"),
    "Population_Evry": ("Population_Evry.csv", "S2_Population.csv"),
}
SCHEMAS = {
    "Consommation_Paris": ("Nom_Rue", "Code_Postal"),
    "Consommation_Evry": ("Nom_Rue", "Code_Postal"),
    "Population_Paris": ("Nom", "Prenom", "Adresse"),
    "Population_Evry": ("Nom", "Prenom", "Adresse"),
}
COLONNES_EXPORT = [
    "type_mesure", "categorie", "table_auditee", "attributs_audites",
    "cle_regroupement", "nb_doublons_detectes", "nb_paires_testees",
    "seuil_similarite",
]

# Une rue compte une seule fois par code postal : ses logements ne sont pas des doublons.
VALEURS_RUES = """
    SELECT DISTINCT Code_Postal AS bloc, Nom_Rue AS valeur
    FROM Source
    WHERE Code_Postal IS NOT NULL AND Nom_Rue IS NOT NULL"""
VALEURS_IDENTITES = """
    SELECT Adresse AS bloc, CONCAT_WS(' ', Nom, Prenom) AS valeur
    FROM Source
    WHERE Adresse IS NOT NULL AND Nom IS NOT NULL AND Prenom IS NOT NULL"""
# Chaque paire de valeurs d'un meme bloc n'est comparee qu'une fois.
REQUETE_PAIRES = """
    WITH Valeurs AS (
        SELECT bloc, valeur, ROW_NUMBER() OVER (ORDER BY bloc, valeur) AS ligne_id
        FROM ({valeurs})
    )
    SELECT
        COUNT(*) AS nb_paires_testees,
        COALESCE(SUM(CASE
            WHEN 1.0 - LEVENSHTEIN(g.valeur, d.valeur)
                       / GREATEST(LENGTH(g.valeur), LENGTH(d.valeur)) > {seuil}
            THEN 1 ELSE 0 END), 0) AS nb_doublons_detectes
    FROM Valeurs g
    JOIN Valeurs d ON g.bloc = d.bloc AND g.ligne_id < d.ligne_id"""


def chemin_source(source):
    for fichier in SOURCES[source]:
        chemin = os.path.join(DATA_DIR, fichier)
        if os.path.isfile(chemin):
            return chemin
    raise FileNotFoundError(
        f"Aucun fichier pour {source} : {SOURCES[source]} dans {DATA_DIR}"
    )


def schema_canonique(colonnes):
    """Tolere Prenom/Prénom et un BOM UTF-8 dans les entetes."""
    mapping = {nom.lstrip("﻿").strip(): nom for nom in colonnes}
    resultat = {}
    for canonique in ("Nom", "Prenom", "Adresse", "Nom_Rue", "Code_Postal"):
        alternatives = ("Prenom", "Prénom") if canonique == "Prenom" else (canonique,)
        present = next((mapping[x] for x in alternatives if x in mapping), None)
        if present is not None:
            resultat[canonique] = present
    return resultat


def separateur_source(chemin):
    """Les sources du projet utilisent ';', accepte aussi les CSV classiques."""
    with open(chemin, "r", encoding="utf-8-sig", newline="") as fichier:
        premiere = fichier.readline()
    return ";" if premiere.count(";") > premiere.count(",") else ","


def verifier_source(source):
    """Etape 1 : presence du fichier et colonnes, sans ouvrir de session Spark."""
    chemin = chemin_source(source)
    delimiteur = separateur_source(chemin)
    with open(chemin, "r", encoding="utf-8-sig", newline="") as fichier:
        entete = next(csv.reader(fichier, delimiter=delimiteur), [])
    presentes = schema_canonique(entete)
    absentes = [nom for nom in SCHEMAS[source] if nom not in presentes]
    if absentes:
        raise ValueError(
            f"Colonnes absentes de {chemin}: {absentes}. Entete detectee: {entete}"
        )
    print(f"Source verifiee : {chemin} ; separateur = {delimiteur!r}")
    return {"source": source, "chemin": chemin, "separateur": delimiteur}


@contextmanager
def session_spark(nom):
    # Limite les ressources CPU dans le worker Airflow/Celery.
    spark = SparkSession.builder.appName(nom).master("local[2]").getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def lire_source(spark, source, information):
    donnees = (spark.read.option("header", True)
               .option("sep", information["separateur"])
               .option("inferSchema", False)
               .option("encoding", "UTF-8")
               .csv(information["chemin"]))
    aliases = schema_canonique(donnees.columns)
    manquantes = [nom for nom in SCHEMAS[source] if nom not in aliases]
    if manquantes:
        raise ValueError(f"Colonnes manquantes pour {source}: {manquantes}")
    return donnees.select(*(F.col(aliases[nom]).alias(nom) for nom in SCHEMAS[source]))


def calculer_similarites(source, **context):
    """Etape 2 : un seul traitement Spark pour charger et comparer."""
    info = context["ti"].xcom_pull(task_ids=f"groupe_{source.lower()}.verifier_source")
    if not info:
        raise ValueError(f"Informations de chargement manquantes pour {source}")
    categorie = "consommation" if source.startswith("Consommation") else "population"
    valeurs = VALEURS_RUES if categorie == "consommation" else VALEURS_IDENTITES
    with session_spark("DQ_Doublons_" + source) as spark:
        lire_source(spark, source, info).createOrReplaceTempView("Source")
        mesure = spark.sql(REQUETE_PAIRES.format(valeurs=valeurs, seuil=SEUIL)).first()
    return {
        "type_mesure": "Variations de Rue" if categorie == "consommation" else "Variations d'Identité",
        "categorie": categorie,
        "table_auditee": source,
        "attributs_audites": "Nom_Rue" if categorie == "consommation" else "Nom;Prenom",
        "cle_regroupement": "Code_Postal" if categorie == "consommation" else "Adresse",
        "nb_doublons_detectes": int(mesure["nb_doublons_detectes"]),
        "nb_paires_testees": int(mesure["nb_paires_testees"]),
        "seuil_similarite": SEUIL,
    }


def valider_mesure(source, **context):
    """Etape 3 : verifications simples, pas de nouveau demarrage Spark."""
    ligne = context["ti"].xcom_pull(task_ids=f"groupe_{source.lower()}.calculer_similarites")
    if not ligne or ligne["table_auditee"] != source:
        raise ValueError(f"Mesure manquante ou incoherente pour {source}")
    total, doublons = ligne["nb_paires_testees"], ligne["nb_doublons_detectes"]
    if not (0 <= doublons <= total):
        raise ValueError(f"Compteurs invalides pour {source} : {ligne}")
    return ligne


def exporter_metriques(**context):
    """Etape finale : seul fichier produit; aucune donnee intermediaire sur disque."""
    lignes = []
    for source in SOURCES:
        mesure = context["ti"].xcom_pull(
            task_ids=f"groupe_{source.lower()}.valider_mesure"
        )
        if not mesure:
            raise ValueError(f"Mesure non disponible pour {source}")
        lignes.append(mesure)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(SORTIE, "w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=COLONNES_EXPORT)
        writer.writeheader()
        writer.writerows(lignes)
    print(f"CSV de metriques : {SORTIE} ({len(lignes)} tables)")


with DAG(
    dag_id="metrique_doublons_semantiques",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["datascale", "qualite", "doublons"],
    description="Variations de rues et d'identites, mesure par table source",
) as dag:
    fins = []
    for source in SOURCES:
        with TaskGroup(group_id=f"groupe_{source.lower()}"):
            verification = PythonOperator(
                task_id="verifier_source",
                python_callable=verifier_source,
                op_kwargs={"source": source},
            )
            calcul = PythonOperator(
                task_id="calculer_similarites",
                python_callable=calculer_similarites,
                op_kwargs={"source": source},
            )
            validation = PythonOperator(
                task_id="valider_mesure",
                python_callable=valider_mesure,
                op_kwargs={"source": source},
            )
            verification >> calcul >> validation
            fins.append(validation)
    export = PythonOperator(
        task_id="exporter_csv_metriques", python_callable=exporter_metriques
    )
    fins >> export
