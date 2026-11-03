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

# Même ordre de recherche que dans le nouvel etl.py.
FICHIERS = {
    "Consommation_Paris": ("Consommation_Paris.csv", "S1_Consommation.csv"),
    "Consommation_Evry": ("Consommation_Evry.csv", "S2_Consommation.csv"),
    "IRIS": ("IRIS.csv", "S4_IRIS.csv"),
}
ATTENDUES = {
    "Consommation_Paris": ("Nom_Rue", "NB_KW_Jour"),
    "Consommation_Evry": ("Nom_Rue", "NB_KW_Jour"),
    "IRIS": ("ID_Rue", "ID_Ville", "ID_IRIS"),
}
COLONNES_CSV = [
    "Nom_Mesure", "Dimension", "Table_Testee", "Nb_Lignes_Total",
    "Nb_Erreurs", "Taux_Conformite_Pct", "Valeur_Observee", "Seuil_Alerte",
]
SEUIL_RATIO_MEDIANES = 1000.0  # Seuil du ratio des médianes, jamais des consommations individuelles.
VERSION_AUDIT = "heterogeneite_ratio_1000_sans_seuil_individuel_v2"


def chemin_source(table):
    for nom in FICHIERS[table]:
        chemin = os.path.join(DATA_DIR, nom)
        if os.path.isfile(chemin):
            return chemin
    raise FileNotFoundError(
        f"Source {table} introuvable : {FICHIERS[table]}. "
        f"Contenu de {DATA_DIR} : "
        f"{sorted(os.listdir(DATA_DIR)) if os.path.isdir(DATA_DIR) else 'dossier absent'}"
    )


def verifier_source(table):
    """Étape 1 : présence du fichier et colonnes, sans démarrer Spark."""
    chemin = chemin_source(table)
    with open(chemin, newline="", encoding="utf-8-sig") as fichier:
        entete = [c.strip().lstrip("\ufeff") for c in next(csv.reader(fichier, delimiter=";"), [])]
    manquantes = [c for c in ATTENDUES[table] if c not in entete]
    if manquantes:
        raise ValueError(f"Colonnes absentes dans {chemin} : {manquantes}. En-tête : {entete}")
    print(f"Source {table} OK : {chemin}")


def lire_csv(spark, table):
    donnees = (spark.read.option("header", True)
               .option("sep", ";")
               .option("inferSchema", False)
               .option("encoding", "UTF-8")
               .csv(chemin_source(table)))
    for ancien in donnees.columns:
        nouveau = ancien.strip().lstrip("\ufeff")
        if nouveau != ancien:
            donnees = donnees.withColumnRenamed(ancien, nouveau)
    manquantes = [c for c in ATTENDUES[table] if c not in donnees.columns]
    if manquantes:
        raise ValueError(f"Colonnes Spark absentes pour {table} : {manquantes}")
    return donnees


def normaliser(chaine):
    """Même principe de normalisation que dans etl_corrige.py."""
    x = F.upper(F.trim(F.coalesce(chaine.cast("string"), F.lit(""))))
    x = F.translate(x, "ÀÂÄÁÇÉÈÊËÎÏÍÔÖÓÙÛÜÚŸ", "AAAACEEEEIIIOOOUUUUY")
    x = F.regexp_replace(x, r"[^A-Z0-9]+", " ")
    return F.trim(F.regexp_replace(x, r"\s+", " "))


def rue_normalisee(chaine):
    return F.regexp_replace(
        normaliser(chaine),
        r"^(RUE|R|BOULEVARD|BD|AVENUE|AV|ALLEE|SQUARE|PLACE|CHEMIN|IMPASSE)\s+", ""
    )


def ville_normalisee(chaine):
    ville = normaliser(chaine)
    return (F.when(ville == "PARIS", F.lit("PARIS"))
            .when(ville.startswith("EVRY"), F.lit("EVRY"))
            .otherwise(F.lit("AUTRE")))


def ligne(mesure, dimension, table, total, erreurs, valeur=None, seuil=None):
    total, erreurs = int(total), int(erreurs)
    if not 0 <= erreurs <= total:
        raise ValueError(f"Compte invalide ({mesure}, {table}) : {erreurs}/{total}")
    return {
        "Nom_Mesure": mesure, "Dimension": dimension, "Table_Testee": table,
        "Nb_Lignes_Total": total, "Nb_Erreurs": erreurs,
        "Taux_Conformite_Pct": round(100 * (total - erreurs) / total, 2) if total else None,
        "Valeur_Observee": valeur, "Seuil_Alerte": seuil,
    }


def calculer_echelle(table):
    """Étape 2 : conversion des décimales et détection des valeurs atypiques (IQR).

    Les valeurs manquantes sont réservées à la complétude. Le facteur 1000
    concerne la comparaison des médianes Paris/Évry, pas les lignes isolées.
    """
    spark = (SparkSession.builder.appName("DQ_Echelle_" + table)
             .master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    try:
        df = lire_csv(spark, table)
        brut = F.trim(F.col("NB_KW_Jour").cast("string"))
        present = (F.col("NB_KW_Jour").isNotNull()
                   & (brut != "") & ~F.lower(brut).isin("nan", "null"))
        nombre = F.regexp_replace(brut, ",", ".").cast("double")
        df = df.withColumn("present", present).withColumn("kw", nombre)
        df = df.withColumn("valide", F.col("present") & F.col("kw").isNotNull()
                           & ~F.isnan(F.col("kw")))
        stats = df.agg(
            F.sum(F.when(F.col("present"), 1).otherwise(0)).alias("presents"),
            F.sum(F.when(F.col("present") & ~F.col("valide"), 1).otherwise(0))
             .alias("conversions_invalides"),
            F.sum(F.when(F.col("valide"), 1).otherwise(0)).alias("numeriques"),
        ).first()
        valeurs = df.filter("valide").select("kw")
        if (stats["numeriques"] or 0) > 0:
            q1, mediane, q3 = valeurs.approxQuantile("kw", [0.25, 0.5, 0.75], 0.0)
            iqr = q3 - q1
            # Pas d'alerte IQR si l'intervalle est nul ou l'échantillon minuscule.
            outliers = (valeurs.filter((F.col("kw") < q1 - 1.5 * iqr)
                                      | (F.col("kw") > q3 + 1.5 * iqr)).count()
                        if iqr > 0 and stats["numeriques"] >= 4 else 0)
        else:
            mediane, outliers = None, 0
        return {
            "table": table,
            "presents": int(stats["presents"] or 0),
            "conversions_invalides": int(stats["conversions_invalides"] or 0),
            "numeriques": int(stats["numeriques"] or 0),
            "outliers": int(outliers),
            "mediane": mediane,
        }
    finally:
        spark.stop()


def valider_echelle(table, task_id_calcul, **context):
    """Étape 3 : calcule les métriques à partir des petits nombres XCom."""
    stat = context["ti"].xcom_pull(task_ids=task_id_calcul)
    if not isinstance(stat, dict) or stat.get("table") != table:
        raise ValueError(f"Statistiques absentes ou incohérentes : {table}")
    presentes, valides = stat["presents"], stat["numeriques"]
    if stat["conversions_invalides"] + valides != presentes:
        raise ValueError(f"Effectifs incohérents pour {table}")
    return [
        ligne("Echelle_Conversion_Numerique", "Heterogeneite_Echelle", table,
              presentes, stat["conversions_invalides"]),
        ligne("Echelle_Valeurs_Atypiques_IQR", "Heterogeneite_Echelle", table,
              valides, stat["outliers"], seuil="Q1-1.5*IQR / Q3+1.5*IQR"),
    ]


def calculer_granularite():
    """Étape 2 : mesure les ambiguïtés dans IRIS et leur impact sur les consommations.

    BROCA -> 2 IRIS est une relation potentiellement valide : c'est la jointure
    rue -> IRIS qui est ambiguë, pas nécessairement une donnée erronée.
    """
    spark = (SparkSession.builder.appName("DQ_Granularite_IRIS")
             .master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    try:
        iris = (lire_csv(spark, "IRIS")
                .select(ville_normalisee(F.col("ID_Ville")).alias("ville"),
                        rue_normalisee(F.col("ID_Rue")).alias("rue"),
                        F.trim(F.coalesce(F.col("ID_IRIS"), F.lit(""))).alias("iris"))
                .filter((F.length("rue") > 0) & (F.length("iris") > 0)
                        & F.col("ville").isin("PARIS", "EVRY"))
                .dropDuplicates(["ville", "rue", "iris"]))
        cles = iris.groupBy("ville", "rue").agg(F.countDistinct("iris").alias("nb_iris"))
        total_cles = cles.count()
        ambigues = cles.filter(F.col("nb_iris") > 1).select("ville", "rue")
        n_ambigues = ambigues.count()
        impacts = {}
        for table, ville in (("Consommation_Paris", "PARIS"),
                             ("Consommation_Evry", "EVRY")):
            conso = (lire_csv(spark, table)
                     .select(rue_normalisee(F.col("Nom_Rue")).alias("rue"))
                     .filter(F.length("rue") > 0)
                     .withColumn("ville", F.lit(ville)))
            total = conso.count()
            impact = conso.join(ambigues, ["ville", "rue"], "inner").count()
            impacts[table] = {"total": int(total), "ambigues": int(impact)}
        return {"table": "IRIS", "total_cles": int(total_cles),
                "cles_ambigues": int(n_ambigues), "impacts": impacts}
    finally:
        spark.stop()


def valider_granularite(task_id_calcul, **context):
    """Étape 3 : validation des résultats et description des indicateurs IRIS."""
    stat = context["ti"].xcom_pull(task_ids=task_id_calcul)
    if not isinstance(stat, dict) or stat.get("table") != "IRIS":
        raise ValueError("Résultats de granularité IRIS absents")
    lignes = [ligne("Granularite_Rues_Multi_IRIS", "Granularite", "IRIS",
                    stat["total_cles"], stat["cles_ambigues"])]
    for table, valeurs in stat["impacts"].items():
        lignes.append(ligne("Granularite_Consommations_IRIS_Ambigu", "Granularite",
                            table, valeurs["total"], valeurs["ambigues"]))
    return lignes


def exporter_csv(**context):
    """Une seule tâche d'export, sans écriture concurrente ni dossiers temporaires."""
    print(f"Version du DAG exécutée : {VERSION_AUDIT}")
    ti = context["ti"]
    blocs = (
        "consommation_paris.valider_et_calculer_taux",
        "consommation_evry.valider_et_calculer_taux",
        "referentiel_iris.valider_granularite",
    )
    lignes = []
    for task_id in blocs:
        contenu = ti.xcom_pull(task_ids=task_id)
        if not isinstance(contenu, list):
            raise ValueError(f"Résultat absent : {task_id}")
        lignes.extend(contenu)

    stats_paris = ti.xcom_pull(task_ids="consommation_paris.calculer_heterogeneite_echelle")
    stats_evry = ti.xcom_pull(task_ids="consommation_evry.calculer_heterogeneite_echelle")
    mp, me = stats_paris["mediane"], stats_evry["mediane"]
    if mp is not None and me is not None and min(abs(mp), abs(me)) > 0:
        ratio = max(abs(mp), abs(me)) / min(abs(mp), abs(me))
        lignes.append(ligne("Echelle_Ratio_Medianes_Paris_Evry", "Heterogeneite_Echelle",
                            "Consommation_Paris;Consommation_Evry", 1,
                            int(ratio > SEUIL_RATIO_MEDIANES),
                            valeur=round(ratio, 4), seuil=SEUIL_RATIO_MEDIANES))
        print(f"Ratio des médianes brutes Paris / Évry : {ratio:.2f} ")
    else:
        print("Ratio des médianes non calculable : médiane nulle ou absente")

    # Garde-fou : aucune ancienne mesure fondée sur 100000 ou les négatifs.
    mesures_interdites = {"Echelle_Seuil_100000", "Echelle_Valeurs_Negatives"}
    presentes = {element["Nom_Mesure"] for element in lignes}
    if presentes & mesures_interdites:
        raise ValueError(f"Mesures obsolètes présentes : {presentes & mesures_interdites}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    temporaire = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", newline="", encoding="utf-8", dir=OUTPUT_DIR,
            prefix=".audit_heterogeneite_", suffix=".csv", delete=False,
        ) as fichier:
            temporaire = fichier.name
            writer = csv.DictWriter(fichier, fieldnames=COLONNES_CSV)
            writer.writeheader()
            writer.writerows(lignes)
        os.replace(temporaire, OUTPUT_CSV)
    finally:
        if temporaire and os.path.isfile(temporaire):
            os.remove(temporaire)
    print(f"{len(lignes)} mesures exportées : {OUTPUT_CSV}")


with DAG(
    dag_id="mesure_heterogeneite",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["datascale", "heterogeneite", "granularite"],
    description="Conversion, IQR, ratio des médianes (seuil 1000) et granularité IRIS",
) as dag:
    fins = []
    for table, groupe in (("Consommation_Paris", "consommation_paris"),
                          ("Consommation_Evry", "consommation_evry")):
        with TaskGroup(group_id=groupe):
            verifier = PythonOperator(
                task_id="verifier_source", python_callable=verifier_source,
                op_kwargs={"table": table},
            )
            calculer = PythonOperator(
                task_id="calculer_heterogeneite_echelle", python_callable=calculer_echelle,
                op_kwargs={"table": table},
            )
            valider = PythonOperator(
                task_id="valider_et_calculer_taux", python_callable=valider_echelle,
                op_kwargs={"table": table,
                           "task_id_calcul": f"{groupe}.calculer_heterogeneite_echelle"},
            )
            verifier >> calculer >> valider
        fins.append(valider)

    with TaskGroup(group_id="referentiel_iris"):
        verifier_iris = PythonOperator(
            task_id="verifier_source", python_callable=verifier_source,
            op_kwargs={"table": "IRIS"},
        )
        calculer_iris = PythonOperator(
            task_id="calculer_granularite", python_callable=calculer_granularite,
        )
        valider_iris = PythonOperator(
            task_id="valider_granularite", python_callable=valider_granularite,
            op_kwargs={"task_id_calcul": "referentiel_iris.calculer_granularite"},
        )
        verifier_iris >> calculer_iris >> valider_iris
    fins.append(valider_iris)

    exporter = PythonOperator(task_id="exporter_csv_heterogeneite", python_callable=exporter_csv)
    for fin in fins:
        fin >> exporter
