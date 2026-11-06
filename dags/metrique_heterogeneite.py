"""DAG d'audit : hétérogénéité des échelles et granularité IRIS.

Question posée : les consommations NB_KW_Jour de Paris et d'Évry sont-elles
dans la même unité ? Et la jointure rue -> IRIS est-elle sans ambiguïté ?

Le DAG produit 8 mesures dans un seul CSV :
  - par ville : conversion en nombre + valeurs atypiques (IQR)      -> 4 lignes
  - granularité : rues sur plusieurs IRIS + compteurs touchés       -> 3 lignes
  - rapport des médianes Paris / Évry, alerte au-dessus de 1000     -> 1 ligne

On MESURE seulement : les fichiers sources ne sont jamais modifiés.
"""
from datetime import datetime
import csv
import os
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Dossier monté dans le conteneur Airflow (./dags -> /opt/airflow/dags).
DATA_DIR = "/opt/airflow/dags/data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output", "audit_results", "heterogeneite")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "audit_heterogeneite.csv")  # Lu par le dashboard.

# Même noms de fichiers que l'ETL : on prend le premier qui existe.
FICHIERS = {
    "Consommation_Paris": ("Consommation_Paris.csv", "S1_Consommation.csv"),
    "Consommation_Evry": ("Consommation_Evry.csv", "S2_Consommation.csv"),
    "IRIS": ("IRIS.csv", "S4_IRIS.csv"),
}
# Colonnes indispensables : sans elles, le DAG s'arrête dès la vérification.
ATTENDUES = {
    "Consommation_Paris": ("Nom_Rue", "NB_KW_Jour"),
    "Consommation_Evry": ("Nom_Rue", "NB_KW_Jour"),
    "IRIS": ("ID_Rue", "ID_Ville", "ID_IRIS"),
}
# Format commun à tous nos CSV d'audit (une ligne = une mesure).
COLONNES_CSV = [
    "Nom_Mesure", "Dimension", "Table_Testee", "Nb_Lignes_Total",
    "Nb_Erreurs", "Taux_Conformite_Pct", "Valeur_Observee", "Seuil_Alerte",
]
# 1000 = facteur entre Wh et kWh. On l'applique au rapport des MÉDIANES des
# deux villes, jamais à une consommation isolée.
SEUIL_RATIO_MEDIANES = 1000.0
VERSION_AUDIT = "heterogeneite_ratio_1000_sans_seuil_individuel_v2"  # Affichée dans les logs.


def chemin_source(table):
    """Renvoie le chemin du fichier de la table, ou une erreur claire s'il manque."""
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
    """Étape 1 : le fichier existe et a les bonnes colonnes.

    On lit seulement l'en-tête en Python, sans démarrer Spark : c'est rapide
    et ça évite de lancer un calcul sur un fichier inutilisable.
    """
    chemin = chemin_source(table)
    with open(chemin, newline="", encoding="utf-8-sig") as fichier:
        # strip + lstrip("﻿") : enlève espaces et BOM (caractère invisible
        # que certains éditeurs ajoutent au début d'un fichier UTF-8).
        entete = [c.strip().lstrip("﻿") for c in next(csv.reader(fichier, delimiter=";"), [])]
    manquantes = [c for c in ATTENDUES[table] if c not in entete]
    if manquantes:
        raise ValueError(f"Colonnes absentes dans {chemin} : {manquantes}. En-tête : {entete}")
    print(f"Source {table} OK : {chemin}")


def lire_csv(spark, table):
    """Charge la source dans Spark, tout en texte.

    inferSchema=False : on garde les valeurs brutes. C'est nous qui décidons
    ensuite comment convertir (virgule décimale), et on compte les échecs.
    """
    donnees = (spark.read.option("header", True)
               .option("sep", ";")
               .option("inferSchema", False)
               .option("encoding", "UTF-8")
               .csv(chemin_source(table)))
    # Nettoie les noms de colonnes (espaces, BOM), pas les données.
    for ancien in donnees.columns:
        nouveau = ancien.strip().lstrip("﻿")
        if nouveau != ancien:
            donnees = donnees.withColumnRenamed(ancien, nouveau)
    manquantes = [c for c in ATTENDUES[table] if c not in donnees.columns]
    if manquantes:
        raise ValueError(f"Colonnes Spark absentes pour {table} : {manquantes}")
    return donnees


# --- Clés de comparaison pour la granularité --------------------------------
# Elles servent uniquement à rapprocher IRIS et consommations dans la mesure
# (« Rue Broca » = « BROCA »). Les sources ne sont pas modifiées.

def normaliser(chaine):
    """Majuscules, sans accents, sans ponctuation, espaces simples."""
    x = F.upper(F.trim(F.coalesce(chaine.cast("string"), F.lit(""))))
    x = F.translate(x, "ÀÂÄÁÇÉÈÊËÎÏÍÔÖÓÙÛÜÚŸ", "AAAACEEEEIIIOOOUUUUY")
    x = F.regexp_replace(x, r"[^A-Z0-9]+", " ")
    return F.trim(F.regexp_replace(x, r"\s+", " "))


def rue_normalisee(chaine):
    """Retire le type de voie en tête : « RUE BROCA » -> « BROCA »."""
    return F.regexp_replace(
        normaliser(chaine),
        r"^(RUE|R|BOULEVARD|BD|AVENUE|AV|ALLEE|SQUARE|PLACE|CHEMIN|IMPASSE)\s+", ""
    )


def ville_normalisee(chaine):
    """Ramène la ville à PARIS, EVRY ou AUTRE (« Évry-Courcouronnes » -> EVRY)."""
    ville = normaliser(chaine)
    return (F.when(ville == "PARIS", F.lit("PARIS"))
            .when(ville.startswith("EVRY"), F.lit("EVRY"))
            .otherwise(F.lit("AUTRE")))


def ligne(mesure, dimension, table, total, erreurs, valeur=None, seuil=None):
    """Fabrique une ligne du CSV d'audit.

    Taux_Conformite_Pct = part des éléments SANS problème.
    Ex. IQR Paris : 1 atypique sur 13 -> 92,31 %.
    Pour le rapport des médianes : 1 test, en alerte -> 0 %. Le chiffre
    utile est alors Valeur_Observee (1590), à comparer à Seuil_Alerte (1000).
    """
    total, erreurs = int(total), int(erreurs)
    if not 0 <= erreurs <= total:  # Garde-fou : un compte incohérent arrête le DAG.
        raise ValueError(f"Compte invalide ({mesure}, {table}) : {erreurs}/{total}")
    return {
        "Nom_Mesure": mesure, "Dimension": dimension, "Table_Testee": table,
        "Nb_Lignes_Total": total, "Nb_Erreurs": erreurs,
        "Taux_Conformite_Pct": round(100 * (total - erreurs) / total, 2) if total else None,
        "Valeur_Observee": valeur, "Seuil_Alerte": seuil,
    }


def calculer_echelle(table):
    """Étape 2 (par ville) : conversion en nombre, médiane et valeurs atypiques.

    Les valeurs manquantes sont exclues ici : elles relèvent de la complétude
    (Évry : 12 lignes, 5 vides -> 7 valeurs mesurées).
    Le résultat est un petit dictionnaire passé à l'étape suivante par XCom.
    """
    # local[2] : 2 cœurs, pour ne pas saturer le worker Airflow.
    spark = (SparkSession.builder.appName("DQ_Echelle_" + table)
             .master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    try:
        df = lire_csv(spark, table)
        brut = F.trim(F.col("NB_KW_Jour").cast("string"))
        # present : la case contient quelque chose (ni vide, ni "nan", ni "null").
        present = (F.col("NB_KW_Jour").isNotNull()
                   & (brut != "") & ~F.lower(brut).isin("nan", "null"))
        # Les sources écrivent "19403,56" : on remplace la virgule par un point
        # pour obtenir un nombre. Si ça échoue, cast renvoie NULL.
        nombre = F.regexp_replace(brut, ",", ".").cast("double")
        df = df.withColumn("present", present).withColumn("kw", nombre)
        # valide : présente ET convertie en vrai nombre.
        df = df.withColumn("valide", F.col("present") & F.col("kw").isNotNull()
                           & ~F.isnan(F.col("kw")))
        # Un seul passage sur les données pour les trois comptes.
        stats = df.agg(
            F.sum(F.when(F.col("present"), 1).otherwise(0)).alias("presents"),
            # Présente mais pas convertible (ex. "12 kW") = erreur de format.
            F.sum(F.when(F.col("present") & ~F.col("valide"), 1).otherwise(0))
             .alias("conversions_invalides"),
            F.sum(F.when(F.col("valide"), 1).otherwise(0)).alias("numeriques"),
        ).first()
        valeurs = df.filter("valide").select("kw")
        if (stats["numeriques"] or 0) > 0:
            # Erreur relative 0.0 = quantiles exacts (pas d'approximation).
            # Paris : Q1 = 13000,30 ; médiane = 14000,85 ; Q3 = 18000,53.
            # Évry  : Q1 = 5,17     ; médiane = 8,81     ; Q3 = 19,67.
            q1, mediane, q3 = valeurs.approxQuantile("kw", [0.25, 0.5, 0.75], 0.0)
            iqr = q3 - q1
            # Règle de Tukey : atypique si hors de [Q1 - 1,5×IQR ; Q3 + 1,5×IQR].
            # Paris : 5000,15 < 5499,96 -> atypique. Évry : 548,49 > 41,41 -> atypique.
            # Pas de calcul si IQR nul ou moins de 4 valeurs (pas de sens statistique).
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
            "mediane": mediane,  # Réutilisée à l'export pour le rapport Paris / Évry.
        }
    finally:
        spark.stop()  # Toujours libérer Spark, même en cas d'erreur.


def valider_echelle(table, task_id_calcul, **context):
    """Étape 3 (par ville) : contrôle les comptes puis crée 2 lignes du CSV.

    - Echelle_Conversion_Numerique : valeurs non convertibles en nombre
      (0 à Paris, 0 à Évry).
    - Echelle_Valeurs_Atypiques_IQR : valeurs hors des bornes de Tukey
      (1 sur 13 à Paris, 1 sur 7 à Évry).
    L'IQR compare une ville à ELLE-MÊME : il ne peut pas voir que tout Paris
    est 1000 fois plus grand. D'où le rapport des médianes à l'export.
    """
    stat = context["ti"].xcom_pull(task_ids=task_id_calcul)
    if not isinstance(stat, dict) or stat.get("table") != table:
        raise ValueError(f"Statistiques absentes ou incohérentes : {table}")
    presentes, valides = stat["presents"], stat["numeriques"]
    # Cohérence : chaque valeur présente est soit convertie, soit en échec.
    if stat["conversions_invalides"] + valides != presentes:
        raise ValueError(f"Effectifs incohérents pour {table}")
    return [
        ligne("Echelle_Conversion_Numerique", "Heterogeneite_Echelle", table,
              presentes, stat["conversions_invalides"]),
        ligne("Echelle_Valeurs_Atypiques_IQR", "Heterogeneite_Echelle", table,
              valides, stat["outliers"], seuil="Q1-1.5*IQR / Q3+1.5*IQR"),
    ]


def calculer_granularite():
    """Étape 2 (IRIS) : rues rattachées à plusieurs IRIS et compteurs concernés.

    Une rue longue peut traverser deux IRIS (BROCA : Croulebarbe 7 et
    Val de Grace 1). Ce n'est pas forcément une erreur de donnée : c'est la
    jointure par nom de rue qui devient ambiguë, et les compteurs de la rue
    sont alors comptés dans les deux IRIS.
    """
    spark = (SparkSession.builder.appName("DQ_Granularite_IRIS")
             .master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    try:
        # Couples (ville, rue, iris) uniques, Paris et Évry seulement.
        iris = (lire_csv(spark, "IRIS")
                .select(ville_normalisee(F.col("ID_Ville")).alias("ville"),
                        rue_normalisee(F.col("ID_Rue")).alias("rue"),
                        F.trim(F.coalesce(F.col("ID_IRIS"), F.lit(""))).alias("iris"))
                .filter((F.length("rue") > 0) & (F.length("iris") > 0)
                        & F.col("ville").isin("PARIS", "EVRY"))
                .dropDuplicates(["ville", "rue", "iris"]))
        # Nombre d'IRIS distincts par rue : plus de 1 = rue ambiguë.
        cles = iris.groupBy("ville", "rue").agg(F.countDistinct("iris").alias("nb_iris"))
        total_cles = cles.count()                      # 12 rues
        ambigues = cles.filter(F.col("nb_iris") > 1).select("ville", "rue")
        n_ambigues = ambigues.count()                  # 1 rue : BROCA
        # Impact : combien de compteurs sont sur une rue ambiguë ?
        impacts = {}
        for table, ville in (("Consommation_Paris", "PARIS"),
                             ("Consommation_Evry", "EVRY")):
            conso = (lire_csv(spark, table)
                     .select(rue_normalisee(F.col("Nom_Rue")).alias("rue"))
                     .filter(F.length("rue") > 0)
                     .withColumn("ville", F.lit(ville)))
            # Toutes les lignes ayant une rue, même sans consommation
            # (Évry : 12 lignes ici, contre 7 valeurs pour l'IQR).
            total = conso.count()
            # Jointure interne = garde seulement les compteurs des rues ambiguës.
            impact = conso.join(ambigues, ["ville", "rue"], "inner").count()
            impacts[table] = {"total": int(total), "ambigues": int(impact)}
            # Résultat : Paris 4 sur 13 (les 4 compteurs Broca), Évry 0 sur 12.
        return {"table": "IRIS", "total_cles": int(total_cles),
                "cles_ambigues": int(n_ambigues), "impacts": impacts}
    finally:
        spark.stop()


def valider_granularite(task_id_calcul, **context):
    """Étape 3 (IRIS) : crée les 3 lignes de granularité du CSV."""
    stat = context["ti"].xcom_pull(task_ids=task_id_calcul)
    if not isinstance(stat, dict) or stat.get("table") != "IRIS":
        raise ValueError("Résultats de granularité IRIS absents")
    # 1 rue ambiguë sur 12 dans IRIS.
    lignes = [ligne("Granularite_Rues_Multi_IRIS", "Granularite", "IRIS",
                    stat["total_cles"], stat["cles_ambigues"])]
    # Puis une ligne par ville : compteurs touchés par une rue ambiguë.
    for table, valeurs in stat["impacts"].items():
        lignes.append(ligne("Granularite_Consommations_IRIS_Ambigu", "Granularite",
                            table, valeurs["total"], valeurs["ambigues"]))
    return lignes


def exporter_csv(**context):
    """Étape finale : rapport des médianes, puis écriture du CSV unique.

    Une seule tâche écrit le fichier : pas d'écriture concurrente entre tâches.
    """
    print(f"Version du DAG exécutée : {VERSION_AUDIT}")
    ti = context["ti"]
    # Récupère les lignes déjà prêtes (2 Paris, 2 Évry, 3 IRIS).
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

    # MESURE PRINCIPALE : rapport des médianes Paris / Évry.
    # Médiane plutôt que moyenne : elle n'est pas tirée par une valeur extrême.
    # Avec les moyennes (14 493 / 86,6), le rapport tomberait à ~167 et on
    # raterait l'alerte, à cause du 548,49 d'Évry.
    stats_paris = ti.xcom_pull(task_ids="consommation_paris.calculer_heterogeneite_echelle")
    stats_evry = ti.xcom_pull(task_ids="consommation_evry.calculer_heterogeneite_echelle")
    mp, me = stats_paris["mediane"], stats_evry["mediane"]
    if mp is not None and me is not None and min(abs(mp), abs(me)) > 0:  # Évite la division par 0.
        # Plus grande / plus petite : le rapport est toujours >= 1, quel que
        # soit le sens. Ici 14000,85 / 8,81 = 1590 > 1000 -> alerte (Wh vs kWh probable).
        # Après division de Paris par 1000, on attend environ 1,6.
        ratio = max(abs(mp), abs(me)) / min(abs(mp), abs(me))
        lignes.append(ligne("Echelle_Ratio_Medianes_Paris_Evry", "Heterogeneite_Echelle",
                            "Consommation_Paris;Consommation_Evry", 1,
                            int(ratio > SEUIL_RATIO_MEDIANES),  # 1 = alerte, 0 = OK.
                            valeur=round(ratio, 4), seuil=SEUIL_RATIO_MEDIANES))
        print(f"Ratio des médianes brutes Paris / Évry : {ratio:.2f} ")
    else:
        print("Ratio des médianes non calculable : médiane nulle ou absente")

    # Garde-fou : d'anciennes versions testaient un seuil de 100000 par valeur
    # et les valeurs négatives. Abandonnées, elles ne doivent pas revenir.
    mesures_interdites = {"Echelle_Seuil_100000", "Echelle_Valeurs_Negatives"}
    presentes = {element["Nom_Mesure"] for element in lignes}
    if presentes & mesures_interdites:
        raise ValueError(f"Mesures obsolètes présentes : {presentes & mesures_interdites}")

    # Écriture sûre : on écrit un fichier temporaire, puis on le renomme.
    # Si la tâche plante en cours, l'ancien CSV reste intact (jamais à moitié écrit).
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
        os.replace(temporaire, OUTPUT_CSV)  # Remplacement en une seule opération.
    finally:
        if temporaire and os.path.isfile(temporaire):  # Nettoie en cas d'échec.
            os.remove(temporaire)
    print(f"{len(lignes)} mesures exportées : {OUTPUT_CSV}")


# --- Enchaînement des tâches -------------------------------------------------
# Paris, Évry et IRIS : 3 groupes indépendants (vérifier -> calculer -> valider),
# exécutés 2 par 2 au maximum. L'export attend la fin des 3 groupes.
# Les tâches s'échangent seulement de petits résultats par XCom, pas de données.
with DAG(
    dag_id="mesure_heterogeneite_granularite",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Lancement manuel depuis l'interface Airflow.
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
