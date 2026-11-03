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

# Meme convention de fichiers que l'ETL corrige. On accepte aussi les noms S1/S2.
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
    mapping = {nom.lstrip("\ufeff").strip(): nom for nom in colonnes}
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


def normaliser(chaine):
    """Normalisation identique a celle utilisee par le nouvel ETL."""
    valeur = F.upper(F.trim(F.coalesce(chaine.cast("string"), F.lit(""))))
    valeur = F.translate(
        valeur, "ÀÂÄÁÇÉÈÊËÎÏÍÔÖÓÙÛÜÚŸ", "AAAACEEEEIIIOOOUUUUY"
    )
    valeur = F.regexp_replace(valeur, r"[^A-Z0-9]+", " ")
    return F.trim(F.regexp_replace(valeur, r"\s+", " "))


def rue_standardisee(chaine):
    """Developpe les abreviations mais CONSERVE le type de voie.

    R. DE LA PAIX -> RUE DE LA PAIX ; AV. VICTOR HUGO -> AVENUE VICTOR HUGO.
    Conserver RUE/AVENUE evite de confondre deux voies distinctes du meme nom.
    """
    x = normaliser(chaine)
    x = F.regexp_replace(x, r"^R\s+", "RUE ")
    x = F.regexp_replace(x, r"^AV\s+", "AVENUE ")
    x = F.regexp_replace(x, r"^BD\s+", "BOULEVARD ")
    return F.regexp_replace(x, r"^ALL\s+", "ALLEE ")


def code_postal_comparable(texte, source):
    """Meme traitement conservateur des anomalies identifiees dans l'ETL."""
    cp = F.upper(F.trim(F.coalesce(texte.cast("string"), F.lit(""))))
    ville_paris = source == "Consommation_Paris"
    if ville_paris:
        return F.when(cp == "750050", "75005").otherwise(cp)
    return F.when(cp.isin("910000", "9100000", "91000A"), "91000").otherwise(cp)


def adresse_canonique(adresse):
    """Construit une cle d'adresse numero + suffixe + voie, comme l'ETL.

    La ville est implicite dans la table source. 2BIS n'est jamais confondu avec 2.
    """
    valeur = normaliser(adresse)
    valeur = F.regexp_replace(valeur, r"^(PARIS|EVRY COURCOURONNES)\s+", "")
    valeur = F.regexp_replace(valeur, r"^\d{5}\s+", "")
    numero = F.regexp_extract(valeur, r"^(\d+)\s*(BIS|TER)?\s+", 1)
    suffixe = F.regexp_extract(valeur, r"^\d+\s*(BIS|TER)\s+", 1)
    voie = F.regexp_replace(valeur, r"^\d+\s*(BIS|TER)?\s+", "")
    # La normalisation des types est identique au nettoyage ETL, sauf que
    # l'on conserve le type pour eviter les faux rapprochements.
    return F.when(
        (F.length(numero) > 0) & (F.length(F.trim(voie)) > 0),
        F.concat_ws("|", F.concat(numero, suffixe), rue_standardisee(voie))
    )


def preparer_rues(df, source):
    """Chaque libelle de rue ne compte qu'une fois par code postal et par table."""
    prepare = (df.withColumn("bloc", code_postal_comparable(F.col("Code_Postal"), source))
               .withColumn("libelle_original", normaliser(F.col("Nom_Rue")))
               .withColumn("libelle_standard", rue_standardisee(F.col("Nom_Rue"))))
    prepare = prepare.filter(
        (F.length("bloc") > 0) & (F.length("libelle_original") > 0)
    )
    # Les 100 logements de CHEMIN GENEVIEVE HUBERT ne constituent pas 100 rues.
    return (prepare.select("bloc", "libelle_original", "libelle_standard")
            .dropDuplicates(["bloc", "libelle_original"]))


def preparer_identites(df):
    nom = normaliser(F.col("Nom"))
    prenom = normaliser(F.col("Prenom"))
    prepare = (df.withColumn("bloc", adresse_canonique(F.col("Adresse")))
               .withColumn("valeur", F.concat_ws(" ", nom, prenom)))
    return prepare.filter(
        F.col("bloc").isNotNull()
        & (F.length(nom) > 0)
        & (F.length(prenom) > 0)
    ).select("bloc", "valeur")


def calculer_similarites(source, **context):
    """Etape 2 : un seul traitement Spark pour charger, normaliser et comparer."""
    info = context["ti"].xcom_pull(task_ids=f"groupe_{source.lower()}.verifier_source")
    if not info:
        raise ValueError(f"Informations de chargement manquantes pour {source}")
    categorie = "consommation" if source.startswith("Consommation") else "population"
    with session_spark("DQ_Doublons_" + source) as spark:
        df = lire_source(spark, source, info)
        if categorie == "consommation":
            df = preparer_rues(df, source)
        else:
            df = preparer_identites(df)
        # ID technique interne pour ne former chaque paire qu'une fois.
        df = df.withColumn("ligne_id", F.monotonically_increasing_id())
        gauche, droite = df.alias("g"), df.alias("d")
        condition = (
            (F.col("g.bloc") == F.col("d.bloc"))
            & (F.col("g.ligne_id") < F.col("d.ligne_id"))
        )
        if categorie == "consommation":
            # Ne pas comparer une rue exactement avec elle-meme.
            condition = condition & (
                F.col("g.libelle_original") != F.col("d.libelle_original")
            )
            a, b = F.col("g.libelle_standard"), F.col("d.libelle_standard")
            # Une meme rue peut avoir plusieurs habitants. Deux noms de rues
            # seulement semblables (ex. BROCA / BROCAT) ne prouvent rien.
            # On exige donc la meme cle apres correction d'abreviations et
            # regroupement des espaces, pour limiter les faux positifs.
            meme_voie = (
                F.regexp_replace(a, " ", "")
                == F.regexp_replace(b, " ", "")
            )
        else:
            a, b = F.col("g.valeur"), F.col("d.valeur")
            meme_voie = F.lit(True)
        paires = gauche.join(droite, condition, "inner")
        taille = F.greatest(F.length(a), F.length(b))
        similarite = F.when(taille > 0, F.lit(1.0) - F.levenshtein(a, b) / taille)
        proches = (meme_voie & (similarite > F.lit(SEUIL)))
        mesure = paires.agg(
            F.count(F.lit(1)).alias("nb_paires_testees"),
            F.sum(F.when(proches, 1).otherwise(0)).alias("nb_doublons_detectes"),
        ).first()
    return {
        "type_mesure": "Variations de Rue" if categorie == "consommation" else "Variations d'Identité",
        "categorie": categorie,
        "table_auditee": source,
        "attributs_audites": "Nom_Rue" if categorie == "consommation" else "Nom;Prenom",
        "cle_regroupement": "Code_Postal" if categorie == "consommation" else "Adresse",
        "nb_doublons_detectes": int(mesure["nb_doublons_detectes"] or 0),
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
