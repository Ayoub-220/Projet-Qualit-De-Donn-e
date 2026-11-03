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
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "completude")
SORTIE = os.path.join(OUTPUT_DIR, "audit_completude.csv")

# Les premiers noms correspondent aux CSV transmis avec le nouvel ETL.
# Les noms alternatifs permettent d'utiliser l'ancien jeu S1/S2/S3.
SOURCES = {
    "consommation_paris": {
        "fichiers": ("Consommation_Paris.csv", "S1_Consommation.csv"),
        "categorie": "consommation",
        "attributs": {"NB_KW_Jour": ("NB_KW_Jour",), "ID_Adr": ("ID_Adr",)},
    },
    "consommation_evry": {
        "fichiers": ("Consommation_Evry.csv", "S2_Consommation.csv"),
        "categorie": "consommation",
        "attributs": {"NB_KW_Jour": ("NB_KW_Jour",), "ID_Adr": ("ID_Adr",)},
    },
    "population_paris": {
        "fichiers": ("Population_Paris.csv", "S1_Population.csv"),
        "categorie": "population",
        "attributs": {"Adresse": ("Adresse",)},
    },
    "population_evry": {
        "fichiers": ("Population_Evry.csv", "S2_Population.csv"),
        "categorie": "population",
        "attributs": {"Adresse": ("Adresse",)},
    },
    "csp": {
        "fichiers": ("CSP.csv", "S3_CSP.csv"),
        "categorie": "csp",
        "attributs": {"Salaire_Moyen": ("Salaire_Moyen", "Salair_Moyen")},
    },
}

COLONNES_EXPORT = [
    "type_mesure", "categorie", "table_auditee", "attribut_audite",
    "taux_completude_pct", "taux_valeurs_manquantes_pct",
]


def fichier_source(source):
    """Choisir le meme fichier que le nouvel ETL (priorite aux noms sans S1/S2)."""
    noms = SOURCES[source]["fichiers"]
    for nom in noms:
        chemin = os.path.join(DATA_DIR, nom)
        if os.path.isfile(chemin):
            return chemin
    raise FileNotFoundError(
        f"Source {source!r} introuvable. Attendu : {noms} dans {DATA_DIR}"
    )


def format_csv(chemin):
    """Lire l'en-tete, identifier le separateur et absorber un eventuel BOM UTF-8."""
    with open(chemin, encoding="utf-8-sig", newline="") as fichier:
        ligne = fichier.readline()
    if not ligne:
        raise ValueError(f"Fichier CSV vide : {chemin}")
    separateur = ";" if ligne.count(";") > ligne.count(",") else ","
    entete = next(csv.reader([ligne], delimiter=separateur))
    return separateur, [nom.strip().lstrip("\ufeff") for nom in entete]


def correspondances(source, entete):
    """Associer chaque colonne attendue a sa colonne effective (alias CSP)."""
    trouvees = {}
    for canonique, possibilites in SOURCES[source]["attributs"].items():
        effectif = next((nom for nom in possibilites if nom in entete), None)
        if effectif is None:
            raise ValueError(
                f"{source} : colonne {canonique!r} absente. "
                f"Alias acceptes : {possibilites}. Colonnes presentes : {entete}"
            )
        trouvees[canonique] = effectif
    return trouvees


@contextmanager
def session_spark(nom):
    spark = SparkSession.builder.appName(nom).master("local[2]").getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def verifier_source(source):
    """Etape 1 : verifier fichier, format CSV et colonnes sans demarrer Spark."""
    chemin = fichier_source(source)
    sep, entete = format_csv(chemin)
    correspondances(source, entete)
    print(f"[Completude] Source OK : {chemin}, separateur={sep!r}, colonnes={entete}")


def mesurer_completude(source):
    """Etape 2 : agregation Spark unique des valeurs NULL, vides ou NaN."""
    chemin = fichier_source(source)
    sep, entete = format_csv(chemin)
    champs = correspondances(source, entete)

    with session_spark("DQ_Completude_" + source) as spark:
        donnees = (spark.read.option("header", True)
                   .option("sep", sep)
                   .option("inferSchema", False)
                   .option("encoding", "UTF-8")
                   .csv(chemin))
        for ancien in donnees.columns:
            nouveau = ancien.strip().lstrip("\ufeff")
            if nouveau != ancien:
                donnees = donnees.withColumnRenamed(ancien, nouveau)

        agregations = [F.count(F.lit(1)).alias("total")]
        for canonique, effectif in champs.items():
            valeur = F.col(effectif)
            texte = F.lower(F.trim(valeur.cast("string")))
            manquant = valeur.isNull() | texte.isin("", "nan", "null", "none")
            agregations.append(
                F.sum(F.when(manquant, 1).otherwise(0)).alias("manquants_" + canonique)
            )
        resultat = donnees.agg(*agregations).first()

    total = int(resultat["total"])
    categorie = SOURCES[source]["categorie"]
    table_auditee = os.path.splitext(os.path.basename(chemin))[0]
    lignes = []
    for attribut in champs:
        nombre_manquants = int(resultat["manquants_" + attribut] or 0)
        if attribut == "NB_KW_Jour":
            type_mesure = "Valeurs manquantes métiers"
        elif attribut in ("ID_Adr", "Adresse"):
            type_mesure = "Clés primaires manquantes"
        else:
            type_mesure = "Valeurs manquantes référentiel CSP"
        lignes.append({
            "type_mesure": type_mesure,
            "categorie": categorie,
            "table_auditee": table_auditee,
            "attribut_audite": attribut,
            "taux_completude_pct": round(100 * (total - nombre_manquants) / total, 2) if total else None,
            "taux_valeurs_manquantes_pct": round(100 * nombre_manquants / total, 2) if total else None,
        })
    return lignes


def valider_resultats(source, **context):
    """Etape 3 : verifier le contenu des metriques renvoyees par XCom."""
    task_id = f"source_{source}.calculer_completude"
    mesures = context["ti"].xcom_pull(task_ids=task_id)
    attendus = set(SOURCES[source]["attributs"])
    if not isinstance(mesures, list) or len(mesures) != len(attendus):
        raise ValueError(f"Mesures absentes ou incompletes pour {source}")
    if {m["attribut_audite"] for m in mesures} != attendus:
        raise ValueError(f"Attributs mesures incoherents pour {source}: {mesures}")
    for mesure in mesures:
        tc = mesure["taux_completude_pct"]
        tm = mesure["taux_valeurs_manquantes_pct"]
        if tc is None or tm is None:
            if tc is not None or tm is not None:
                raise ValueError(f"Taux incomplet : {mesure}")
        elif not (0 <= tc <= 100 and 0 <= tm <= 100 and abs(tc + tm - 100) <= 0.02):
            raise ValueError(f"Taux incoherents : {mesure}")
    return mesures


def exporter_metriques(**context):
    """Etape finale : une seule ecriture CSV dans audit_results/completude."""
    lignes = []
    for source in SOURCES:
        resultat = context["ti"].xcom_pull(task_ids=f"source_{source}.valider_resultats")
        if not isinstance(resultat, list):
            raise ValueError(f"Resultat indisponible pour {source}")
        lignes.extend(resultat)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(SORTIE, "w", newline="", encoding="utf-8") as fichier:
        writer = csv.DictWriter(fichier, fieldnames=COLONNES_EXPORT)
        writer.writeheader()
        writer.writerows(lignes)
    print(f"[Completude] {len(lignes)} metriques exportees vers {SORTIE}")


with DAG(
    dag_id="metrique_completude",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["datascale", "qualite", "completude"],
    description="Completude brute des sources utilisees par les mappings ETL",
) as dag:
    fins = []
    for source in SOURCES:
        with TaskGroup(group_id=f"source_{source}"):
            verification = PythonOperator(
                task_id="verifier_source",
                python_callable=verifier_source,
                op_kwargs={"source": source},
            )
            calcul = PythonOperator(
                task_id="calculer_completude",
                python_callable=mesurer_completude,
                op_kwargs={"source": source},
            )
            validation = PythonOperator(
                task_id="valider_resultats",
                python_callable=valider_resultats,
                op_kwargs={"source": source},
            )
            verification >> calcul >> validation
        fins.append(validation)

    export = PythonOperator(task_id="exporter_csv_metriques", python_callable=exporter_metriques)
    fins >> export
