"""DAG Airflow 2.10.2 / PySpark : mesure de la completude .
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
from pyspark.sql.window import Window

DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "metrique_completude")
SORTIE = os.path.join(OUTPUT_DIR, "metriques_completude.csv")
TMP_ROOT = os.path.join(OUTPUT_DIR, ".tmp_metrique_completude")
REMEDIATION_DIR = os.path.join(OUTPUT_DIR, "remediation")

# Perimetre exact de la section 3.3 du rapport (S1/S2 Conso et Population).
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


def exporter_csv_unique(df, chemin):
    """Ecrit un CSV unique via un dossier temporaire Spark."""
    dossier = chemin + ".tmp"
    shutil.rmtree(dossier, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(dossier)
    parties = [
        nom for nom in os.listdir(dossier)
        if nom.startswith("part-") and nom.endswith(".csv")
    ]
    if len(parties) != 1:
        raise RuntimeError(f"Fichier Spark attendu introuvable dans {dossier}")
    os.makedirs(os.path.dirname(chemin), exist_ok=True)
    os.replace(os.path.join(dossier, parties[0]), chemin)
    shutil.rmtree(dossier, ignore_errors=True)


def remedier_completude(source, **context):
    """Impute la consommation par segment IRIS/CSP et isole les clés absentes."""
    if source not in ("S1", "S2"):
        raise ValueError(f"Source de remédiation inconnue : {source}")

    consommation_path = os.path.join(DATA_DIR, f"{source}_Consommation.csv")
    population_path = os.path.join(DATA_DIR, f"{source}_Population.csv")
    iris_path = os.path.join(DATA_DIR, "S4_IRIS.csv")

    with session_spark("Completude_remedier_" + source) as spark:
        consommation = spark.read.option("header", True).option(
            "inferSchema", True
        ).csv(consommation_path)
        population = spark.read.option("header", True).option(
            "inferSchema", True
        ).csv(population_path)
        iris = spark.read.option("header", True).option(
            "inferSchema", True
        ).csv(iris_path)

        cle_conso = F.trim(consommation.ID_Adr.cast("string"))
        cle_population = F.trim(population.Adresse.cast("string"))
        conso_avec_statut = consommation.withColumn(
            "_cle_absente",
            consommation.ID_Adr.isNull() | (cle_conso == "") |
            F.lower(cle_conso).isin("nan", "null"),
        )
        quarantine = conso_avec_statut.filter(F.col("_cle_absente")).drop(
            "_cle_absente"
        )
        valides = conso_avec_statut.filter(~F.col("_cle_absente")).drop(
            "_cle_absente"
        )

        population_valide = population.filter(
            population.Adresse.isNotNull()
            & (cle_population != "")
            & ~F.lower(cle_population).isin("nan", "null")
        ).select(
            F.trim(population.Adresse.cast("string")).alias("_adresse"),
            population.CSP.alias("_csp"),
        )
        iris_ref_brut = iris.select(
            F.trim(iris.ID_Rue.cast("string")).alias("_rue"),
            F.trim(iris.ID_Ville.cast("string")).alias("_ville"),
            iris.ID_Iris.alias("_iris"),
        )
        fenetre_iris = Window.partitionBy("_rue", "_ville").orderBy(
            F.desc("_nombre")
        )
        iris_ref = (
            iris_ref_brut.groupBy("_rue", "_ville", "_iris")
            .count()
            .withColumnRenamed("count", "_nombre")
            .withColumn("_rang", F.row_number().over(fenetre_iris))
            .filter(F.col("_rang") == 1)
            .drop("_nombre", "_rang")
        )

        # Renommer la clé de population avant la jointure pour éviter une
        # condition ambiguë après l'enrichissement.
        enrichi = (
            valides.withColumn("_adresse_conso", F.trim(valides.ID_Adr.cast("string")))
            .join(
                population_valide.withColumnRenamed("_adresse", "_adresse_pop"),
                F.col("_adresse_conso") == F.col("_adresse_pop"),
                "left",
            )
            .drop("_adresse_pop")
        )
        enrichi = enrichi.join(
            iris_ref,
            (
                F.lower(F.trim(enrichi.Nom_Rue.cast("string")))
                == F.lower(F.trim(iris_ref._rue.cast("string")))
            )
            & (
                F.trim(enrichi.Code_Postal.cast("string"))
                == F.trim(iris_ref._ville.cast("string"))
            ),
            "left",
        )
        enrichi = enrichi.withColumn(
            "_nb_manquant",
            F.col("NB_KW_Jour").isNull()
            | F.isnan(F.col("NB_KW_Jour").cast("double")),
        )
        moyennes = (
            enrichi.filter(
                ~F.col("_nb_manquant")
                & F.col("_iris").isNotNull()
                & F.col("_csp").isNotNull()
            )
            .groupBy("_iris", "_csp")
            .agg(F.avg("NB_KW_Jour").alias("_moyenne_segment"))
        )
        remedie = (
            enrichi.join(moyennes, ["_iris", "_csp"], "left")
            .withColumn(
                "NB_KW_Jour",
                F.when(
                    F.col("_nb_manquant") & F.col("_moyenne_segment").isNotNull(),
                    F.round(F.col("_moyenne_segment"), 5),
                ).otherwise(F.col("NB_KW_Jour")),
            )
        )
        non_imputables = remedie.filter(
            F.col("NB_KW_Jour").isNull()
            | F.isnan(F.col("NB_KW_Jour").cast("double"))
        )
        colonnes_source = valides.columns
        remedie = remedie.select(*colonnes_source)
        non_imputables = non_imputables.select(*colonnes_source)

        sortie = os.path.join(REMEDIATION_DIR, f"{source}_Consommation_nettoyee.csv")
        quarantaine = os.path.join(
            REMEDIATION_DIR, f"{source}_Consommation_quarantaine.csv"
        )
        non_imputables_path = os.path.join(
            REMEDIATION_DIR, f"{source}_Consommation_non_imputable.csv"
        )
        exporter_csv_unique(remedie, sortie)
        exporter_csv_unique(quarantine, quarantaine)
        exporter_csv_unique(non_imputables, non_imputables_path)


def remedier_s1(**context):
    remedier_completude("S1", **context)


def remedier_s2(**context):
    remedier_completude("S2", **context)


with DAG(
    dag_id="metrique_completude",
    start_date=datetime(2026, 10, 13),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["datascale", "qualite", "completude"],
    description="Mesure par table et attribut des taux de completude des sources S1/S2",
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
    remediations = [
        PythonOperator(
            task_id="remedier_s1",
            python_callable=remedier_s1,
        ),
        PythonOperator(
            task_id="remedier_s2",
            python_callable=remedier_s2,
        ),
    ]
    fins >> export >> remediations >> nettoyage
