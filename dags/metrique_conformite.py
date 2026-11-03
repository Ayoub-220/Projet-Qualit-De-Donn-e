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
DOSSIER_SORTIE = os.path.join(DATA_DIR, "output", "audit_results", "conformite")
FICHIER_SORTIE = os.path.join(DOSSIER_SORTIE, "audit_conformite.csv")
COLONNES = [
    "Nom_Mesure", "Dimension", "Table_Testee", "Nb_Lignes_Total",
    "Nb_Erreurs", "Taux_Conformite_Pct",
]


def verifier_sources(specifications):
    """Verification rapide des fichiers et entetes, sans demarrer Spark."""
    for table, colonnes_requises in specifications:
        chemin = os.path.join(DATA_DIR, f"{table}.csv")
        if not os.path.isfile(chemin):
            raise FileNotFoundError(f"Source introuvable : {chemin}")
        with open(chemin, "r", newline="", encoding="utf-8-sig") as fichier:
            entetes = next(csv.reader(fichier), [])
        manquantes = sorted(set(colonnes_requises) - set(entetes))
        if manquantes:
            raise ValueError(f"{table} : colonnes manquantes {manquantes}. Colonnes : {entetes}")
        print(f"[Verification] {table} : OK")


def lire_source(spark, nom_table):
    chemin = os.path.join(DATA_DIR, f"{nom_table}.csv")
    return spark.read.csv(chemin, header=True, inferSchema=True)


def creer_spark(nom):
    # Une session Spark par famille de controles, avec 2 coeurs maximum.
    return SparkSession.builder.appName(nom).master("local[2]").getOrCreate()


def resultat(mesure, table, total, erreurs):
    return {
        "Nom_Mesure": mesure,
        "Dimension": "Conformité",
        "Table_Testee": table,
        "Nb_Lignes_Total": int(total),
        "Nb_Erreurs": int(erreurs),
        "Taux_Conformite_Pct": round((total - erreurs) * 100 / total, 2) if total else 0.0,
    }


def mesurer_cp(df, colonne, table):
    # NULL est non conforme ; les autres valeurs doivent avoir exactement 5 chiffres.
    valide = F.col(colonne).cast("string").rlike(r"^[0-9]{5}$")
    compte = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.coalesce(valide, F.lit(False)), 0).otherwise(1)).alias("erreurs"),
    ).first()
    return resultat("Conformite_Code_Postal", table, compte["total"], compte["erreurs"] or 0)


def calculer_cp_consommation():
    spark = creer_spark("DQ_Conformite_CP_Consommation")
    try:
        return [
            mesurer_cp(lire_source(spark, table), "Code_Postal", table)
            for table in ("S1_Consommation", "S2_Consommation")
        ]
    finally:
        spark.stop()


def calculer_cp_iris():
    spark = creer_spark("DQ_Conformite_CP_IRIS")
    try:
        # On conserve ID_Ville comme dans le code precedent.
        # Verifier que cette colonne contient bien des codes postaux.
        return [mesurer_cp(lire_source(spark, "S4_IRIS"), "ID_Ville", "S4_IRIS")]
    finally:
        spark.stop()


def calculer_referentiel_csp():
    spark = creer_spark("DQ_Conformite_Referentiel_CSP")
    try:
        reference = lire_source(spark, "S3_CSP")
        cles_valides = reference.select(F.col("ID_CSP").alias("_id_csp_ref")).distinct()
        lignes = []
        for table in ("S1_Population", "S2_Population"):
            population = lire_source(spark, table)
            total = population.count()
            # Les CSP sans correspondance, y compris NULL, sont non conformes.
            erreurs = population.join(
                cles_valides,
                population["CSP"] == cles_valides["_id_csp_ref"],
                "left_anti",
            ).count()
            lignes.append(resultat("Conformite_Referentiel_CSP", table, total, erreurs))
        return lignes
    finally:
        spark.stop()


def controler_resultats(task_calcul, tables_attendues, ti):
    """Controle les petites mesures XCom, sans recalculer les donnees."""
    lignes = ti.xcom_pull(task_ids=task_calcul)
    if not isinstance(lignes, list) or len(lignes) != len(tables_attendues):
        raise ValueError(f"Mesures manquantes ou incompletes pour {task_calcul}")
    if {r["Table_Testee"] for r in lignes} != set(tables_attendues):
        raise ValueError(f"Tables mesurees inattendues pour {task_calcul}")
    for ligne in lignes:
        total = ligne["Nb_Lignes_Total"]
        erreurs = ligne["Nb_Erreurs"]
        taux = ligne["Taux_Conformite_Pct"]
        if total < 0 or erreurs < 0 or erreurs > total or not (0 <= taux <= 100):
            raise ValueError(f"Mesure incoherente : {ligne}")
    print(f"[Controle] {len(lignes)} mesures coherentes pour {task_calcul}")


def exporter_csv_conformite(ti):
    """Une seule tache ecrit le CSV, pour eviter les acces concurrents."""
    identifiants = (
        "codes_postaux_consommation.calculer_conformite_cp_consommation",
        "codes_postaux_iris.calculer_conformite_cp_iris",
        "referentiel_csp.calculer_conformite_csp",
    )
    lignes = []
    for task_id in identifiants:
        donnees = ti.xcom_pull(task_ids=task_id)
        if donnees is None:
            raise RuntimeError(f"Resultats absents : {task_id}")
        lignes.extend(donnees)
    if len(lignes) != 5:
        raise ValueError(f"Cinq mesures attendues, {len(lignes)} obtenues")

    os.makedirs(DOSSIER_SORTIE, exist_ok=True)
    fd, temporaire = tempfile.mkstemp(prefix=".conformite_", suffix=".csv", dir=DOSSIER_SORTIE)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fichier:
            writer = csv.DictWriter(fichier, fieldnames=COLONNES)
            writer.writeheader()
            writer.writerows(lignes)
        os.replace(temporaire, FICHIER_SORTIE)
    finally:
        if os.path.exists(temporaire):
            os.remove(temporaire)
    print(f"[Conformite] 5 mesures exportees dans {FICHIER_SORTIE}")


with DAG(
    dag_id="mesure_conformite",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
    max_active_runs=1,
    tags=["datascale", "conformite"],
    description="Mesure de conformite : sources, calculs, controles et export",
) as dag:
    with TaskGroup(group_id="codes_postaux_consommation", tooltip="S1/S2 Consommation : Code_Postal"):
        verifier_cp_conso = PythonOperator(
            task_id="verifier_fichiers_et_colonnes",
            python_callable=verifier_sources,
            op_kwargs={"specifications": [
                ("S1_Consommation", ["Code_Postal"]),
                ("S2_Consommation", ["Code_Postal"]),
            ]},
        )
        calcul_cp_conso = PythonOperator(
            task_id="calculer_conformite_cp_consommation",
            python_callable=calculer_cp_consommation,
        )
        controle_cp_conso = PythonOperator(
            task_id="controler_resultats",
            python_callable=controler_resultats,
            op_kwargs={
                "task_calcul": "codes_postaux_consommation.calculer_conformite_cp_consommation",
                "tables_attendues": ["S1_Consommation", "S2_Consommation"],
            },
        )
        verifier_cp_conso >> calcul_cp_conso >> controle_cp_conso

    with TaskGroup(group_id="codes_postaux_iris", tooltip="S4_IRIS : ID_Ville"):
        verifier_iris = PythonOperator(
            task_id="verifier_fichier_et_colonne",
            python_callable=verifier_sources,
            op_kwargs={"specifications": [("S4_IRIS", ["ID_Ville"])]},
        )
        calcul_iris = PythonOperator(
            task_id="calculer_conformite_cp_iris",
            python_callable=calculer_cp_iris,
        )
        controle_iris = PythonOperator(
            task_id="controler_resultats",
            python_callable=controler_resultats,
            op_kwargs={
                "task_calcul": "codes_postaux_iris.calculer_conformite_cp_iris",
                "tables_attendues": ["S4_IRIS"],
            },
        )
        verifier_iris >> calcul_iris >> controle_iris

    with TaskGroup(group_id="referentiel_csp", tooltip="S1/S2 Population vs S3_CSP"):
        verifier_csp = PythonOperator(
            task_id="verifier_fichiers_et_colonnes",
            python_callable=verifier_sources,
            op_kwargs={"specifications": [
                ("S1_Population", ["CSP"]),
                ("S2_Population", ["CSP"]),
                ("S3_CSP", ["ID_CSP"]),
            ]},
        )
        calcul_csp = PythonOperator(
            task_id="calculer_conformite_csp",
            python_callable=calculer_referentiel_csp,
        )
        controle_csp = PythonOperator(
            task_id="controler_resultats",
            python_callable=controler_resultats,
            op_kwargs={
                "task_calcul": "referentiel_csp.calculer_conformite_csp",
                "tables_attendues": ["S1_Population", "S2_Population"],
            },
        )
        verifier_csp >> calcul_csp >> controle_csp

    exporter = PythonOperator(
        task_id="exporter_csv_conformite",
        python_callable=exporter_csv_conformite,
    )
    [controle_cp_conso, controle_iris, controle_csp] >> exporter
