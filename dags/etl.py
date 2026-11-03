"""ETL DataScale - Airflow 2.10.2 / PySpark.

3 groupes x 3 taches : verifier -> transformer/agregger -> publier.
Sources brutes conservees ; aucune correction des codes postaux.
Les normalisations de rues/adresses/CSP servent uniquement de cles de jointure.
Une adresse sans IRIS unique est exclue du mapping IRIS (pas d'attribution inventee).
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
from pyspark.sql import functions as F

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
        entete = [c.strip().lstrip("\ufeff") for c in entete]
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
        nouveau = ancien.strip().lstrip("\ufeff")
        if ancien != nouveau:
            df = df.withColumnRenamed(ancien, nouveau)
    if cle == "csp" and "Salair_Moyen" in df.columns:
        df = df.withColumnRenamed("Salair_Moyen", "Salaire_Moyen")
    return df


def sans_accents(texte):
    """Normalisation Spark sans UDF Python, pour rester rapide."""
    x = F.upper(F.trim(F.coalesce(texte.cast("string"), F.lit(""))))
    x = F.translate(x, "ÀÂÄÁÇÉÈÊËÎÏÍÔÖÓÙÛÜÚŸ", "AAAACEEEEIIIOOOUUUUY")
    x = F.regexp_replace(x, r"[^A-Z0-9]+", " ")
    return F.trim(F.regexp_replace(x, r"\s+", " "))


def rue_normalisee(texte):
    return F.regexp_replace(
        sans_accents(texte),
        r"^(RUE|R|BOULEVARD|BD|AVENUE|AV|ALLEE|SQUARE|PLACE|CHEMIN|IMPASSE)\s+", ""
    )


def nombre_decimal(texte):
    """NB_KW_Jour, ex. '19403,56164' -> 19403.56164."""
    propre = F.regexp_replace(F.trim(texte.cast("string")), ",", ".")
    return propre.cast("double")


def code_postal_valide(texte):
    """Valide uniquement la forme du code postal, sans le corriger.

    Une valeur invalide reste telle quelle dans la source et n'est pas
    exploitee pour l'agregation IRIS. Son anomalie releve de l'audit
    de conformite et d'une future etape de nettoyage.
    """
    brut = F.trim(texte.cast("string"))
    return F.coalesce(brut.rlike(r"^[0-9]{5}$"), F.lit(False))


def conso_preparee(spark, cle, ville):
    df = lire(spark, cle)
    return (df.withColumn("rue_cle", rue_normalisee(F.col("Nom_Rue")))
            .withColumn("numero_cle", sans_accents(F.col("N")))
            .withColumn("cp_valide", code_postal_valide(F.col("Code_Postal")))
            .withColumn("conso_num", nombre_decimal(F.col("NB_KW_Jour")))
            .withColumn("ville_cle", F.lit(ville)))


def iris_prepare(spark):
    return (lire(spark, "iris")
            .withColumn("rue_cle", rue_normalisee(F.col("ID_Rue")))
            .withColumn("ville_cle", F.when(sans_accents(F.col("ID_Ville")) == "PARIS", F.lit("PARIS"))
                        .when(sans_accents(F.col("ID_Ville")).startswith("EVRY"), F.lit("EVRY"))
                        .otherwise(F.lit("AUTRE")))
            .withColumn("iris_cle", F.trim(F.col("ID_IRIS"))))


def transformer_iris(spark, branche):
    ville = "PARIS" if branche == "paris" else "EVRY"
    cle = "conso_paris" if branche == "paris" else "conso_evry"
    conso = conso_preparee(spark, cle, ville)
    iris = iris_prepare(spark)
    # Des lignes IRIS repetant la meme association ne doivent pas dupliquer la conso.
    iris = iris.select("ville_cle", "rue_cle", "iris_cle").dropDuplicates()
    # Rue BROCA = deux IRIS => attribution inconnue avec les colonnes presentes.
    cardinalite = iris.groupBy("ville_cle", "rue_cle").agg(
        F.countDistinct("iris_cle").alias("nb_iris"))
    iris_uniques = (iris.join(cardinalite, ["ville_cle", "rue_cle"])
                    .filter(F.col("nb_iris") == 1)
                    .select("ville_cle", "rue_cle", "iris_cle"))
    # Les codes postaux invalides NE sont PAS repares pour faire passer la jointure.
    # Ils restent non mappes. Les valeurs non numeriques ou absentes ne sont
    # pas agregees non plus (mesurees par les DAG de qualite).
    admissibles = conso.filter(F.col("cp_valide") & F.col("conso_num").isNotNull())
    jointure = admissibles.join(iris_uniques, ["ville_cle", "rue_cle"], "inner")
    return (jointure.groupBy("iris_cle")
            .agg(F.round(F.avg("conso_num") * 365, 2).alias("Conso_moyenne_annuelle"))
            .select(F.col("iris_cle").alias("ID_IRIS"), "Conso_moyenne_annuelle"))


def population_preparee(spark, cle, ville):
    """Extrait le numero/suffixe/rue d'une adresse de personne.

    Ex. 'Evry-Courcouronnes, 91000, 2bis rue Voltaire' => 2BIS + VOLTAIRE.
    """
    df = lire(spark, cle)
    brut = sans_accents(F.col("Adresse"))
    # Une fois normalise : PARIS 20 RUE ARTHUR GROUSSIER ou
    # EVRY COURCOURONNES 91000 19 BOULEVARD AGUADO.
    suite = F.regexp_replace(brut, r"^(PARIS|EVRY COURCOURONNES)\s+", "")
    suite = F.regexp_replace(suite, r"^\d{5}\s+", "")
    numero = F.regexp_extract(suite, r"^(\d+)\s*(BIS|TER)?\s+", 1)
    suffixe = F.regexp_extract(suite, r"^\d+\s*(BIS|TER)\s+", 1)
    voie = F.regexp_replace(suite, r"^\d+\s*(BIS|TER)?\s+", "")
    return (df.withColumn("numero_cle", F.concat(numero, suffixe))
            .withColumn("rue_cle", rue_normalisee(voie))
            .withColumn("ville_cle", F.lit(ville))
            .filter((F.length("numero_cle") > 0) & (F.length("rue_cle") > 0)))


def transformer_csp(spark):
    parties = []
    for ville, pop_cle, conso_cle in (
        ("PARIS", "pop_paris", "conso_paris"),
        ("EVRY", "pop_evry", "conso_evry"),
    ):
        pop = population_preparee(spark, pop_cle, ville).alias("p")
        conso = conso_preparee(spark, conso_cle, ville).alias("c")
        # Ne pas joindre un 2bis au 2, ni confondre deux rues distinctes.
        pairs = (pop.join(conso,
                    (F.col("p.ville_cle") == F.col("c.ville_cle")) &
                    (F.col("p.numero_cle") == F.col("c.numero_cle")) &
                    (F.col("p.rue_cle") == F.col("c.rue_cle")), "inner")
                 .select(F.col("p.CSP").alias("CSP_brut"),
                         F.col("c.conso_num").alias("conso_num")))
        parties.append(pairs)
    toutes = parties[0].unionByName(parties[1]).filter(F.col("conso_num").isNotNull())
    canon = sans_accents(F.col("CSP_brut"))
    # Table de correspondance pour les libelles effectivement presents dans le jeu.
    code = (F.when(canon.rlike(r"^[1-6]$"), canon)
            .when(canon.isin("CADRE", "CADRES ET PROFESSIONS INTELLECTUELLES SUPERIEURES"), "3")
            .when(canon == "PROFESSIONS INTERMEDIAIRES", "4")
            .when(canon.isin("EMPLOYE", "EMPLOYES", "EMPLOYEES"), "5")
            .when(canon.isin("OUVRIER", "OUVRIERS"), "6")
            .when(canon.isin("COMMERCANT ARTISAN", "ARTISAN COMMERCANT"), "2")
            .when(canon == "AGRICULTEUR", "1")
            .otherwise(F.lit(None).cast("string")))
    toutes = toutes.withColumn("code_csp", code).filter(F.col("code_csp").isNotNull())
    ref = (lire(spark, "csp")
           .select(F.trim(F.col("ID_CSP")).alias("code_csp"),
                   F.regexp_replace(F.col("Salaire_Moyen"), ",", ".")
                   .cast("double").alias("Salaire_Moyen"))
           .dropDuplicates(["code_csp"]))
    jointure = toutes.join(ref, "code_csp", "inner")
    return (jointure.groupBy("code_csp", "Salaire_Moyen")
            .agg(F.round(F.avg("conso_num") * 365, 2).alias("Conso_moyenne_annuelle"))
            .select(F.col("code_csp").alias("ID_CSP"),
                    "Conso_moyenne_annuelle", "Salaire_Moyen"))


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
        resultat = transformer_csp(spark) if branche == "csp" else transformer_iris(spark, branche)
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
