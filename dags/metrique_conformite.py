from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import csv
import pandas as pd

DATA_DIR = '/opt/airflow/dags/data'
DOSSIER_RESULTATS = os.path.join(DATA_DIR, 'output', 'audit_results', 'conformite')
FICHIER_RESULTATS = os.path.join(DOSSIER_RESULTATS, 'audit_conformite.csv')

# Noms actuels et anciens noms possibles dans votre depot.
SOURCES = {
    'Consommation_Paris': ('Consommation_Paris.csv', 'S1_Consommation.csv'),
    'Consommation_Evry': ('Consommation_Evry.csv', 'S2_Consommation.csv'),
    'Population_Paris': ('Population_Paris.csv', 'S1_Population.csv'),
    'Population_Evry': ('Population_Evry.csv', 'S2_Population.csv'),
    'IRIS': ('IRIS.csv', 'Iris.csv', 'S4_IRIS.csv'),
    'CSP': ('CSP.csv', 'Csp.csv', 'S3_CSP.csv'),
}
COLONNES_ATTENDUES = {
    'Consommation_Paris': ('Code_Postal',),
    'Consommation_Evry': ('Code_Postal',),
    'Population_Paris': ('CSP',),
    'Population_Evry': ('CSP',),
    'IRIS': ('ID_Ville',),
    'CSP': ('ID_CSP',),
}
COLONNES_EXPORT = [
    'Nom_Mesure', 'Dimension', 'Table_Testee', 'Nb_Lignes_Total',
    'Nb_Erreurs', 'Taux_Conformite_Pct',
]


def trouver_fichier(source):
    for nom in SOURCES[source]:
        chemin = os.path.join(DATA_DIR, nom)
        if os.path.isfile(chemin):
            return chemin
    raise FileNotFoundError(
        f"Fichier absent pour {source}. Noms acceptes : {SOURCES[source]} ; "
        f"dossier : {DATA_DIR}"
    )


def verifier_entete(chemin, colonnes):
    """Le jeu de test est separe par ; et peut contenir un BOM UTF-8."""
    with open(chemin, 'r', encoding='utf-8-sig', newline='') as fichier:
        entete = [c.strip() for c in next(csv.reader(fichier, delimiter=';'), [])]
    manquantes = [c for c in colonnes if c not in entete]
    if manquantes:
        raise ValueError(f"Colonnes absentes dans {chemin}: {manquantes}. En-tete : {entete}")


def charger_source(source):
    """Tache 1 : verifier les CSV, puis transmettre leurs chemins par XCom."""
    chemin = trouver_fichier(source)
    verifier_entete(chemin, COLONNES_ATTENDUES[source])
    resultat = {'source': source, 'chemin': chemin}
    if source.startswith('Population_'):
        chemin_csp = trouver_fichier('CSP')
        verifier_entete(chemin_csp, COLONNES_ATTENDUES['CSP'])
        resultat['chemin_csp'] = chemin_csp
    print(f"Fichiers verifies pour {source} : {resultat}")
    return resultat


def lire_csv(spark, chemin):
    """Lecture Spark sans inference pour proteger les codes postaux et identifiants."""
    df = (spark.read.option('header', True)
          .option('sep', ';')
          .option('inferSchema', False)
          .option('encoding', 'UTF-8')
          .csv(chemin))
    for ancien in df.columns:
        nouveau = ancien.replace('\ufeff', '').strip()
        if nouveau != ancien:
            df = df.withColumnRenamed(ancien, nouveau)
    return df


def normaliser_texte(champ):
    """Majuscules, accents, ponctuation : uniquement pour comparaison."""
    x = F.upper(F.trim(F.coalesce(champ.cast('string'), F.lit(''))))
    x = F.translate(x, 'ÀÂÄÁÇÉÈÊËÎÏÍÔÖÓÙÛÜÚŸ', 'AAAACEEEEIIIOOOUUUUY')
    x = F.regexp_replace(x, r'[^A-Z0-9]+', ' ')
    return F.trim(F.regexp_replace(x, r'\s+', ' '))


def code_csp_normalise(champ):
    """Correspondances fondees sur les libelles observes et le referentiel CSP.csv.

    Etudiant et Retraitee n'ont pas de code explicite dans ce referentiel :
    on les laisse non conformes plutot que de leur inventer une categorie.
    """
    texte = normaliser_texte(champ)
    return (F.when(texte.rlike(r'^[1-6]$'), texte)
            .when(texte.isin('CADRE', 'CADRES ET PROFESSIONS INTELLECTUELLES SUPERIEURES'), F.lit('3'))
            .when(texte == 'PROFESSIONS INTERMEDIAIRES', F.lit('4'))
            .when(texte.isin('EMPLOYE', 'EMPLOYES', 'EMPLOYEE', 'EMPLOYEES'), F.lit('5'))
            .when(texte.isin('OUVRIER', 'OUVRIERS', 'OUVRIERE', 'OUVRIERES'), F.lit('6'))
            .when(texte.isin('COMMERCANT ARTISAN', 'ARTISAN COMMERCANT'), F.lit('2'))
            .when(texte.isin('AGRICULTEUR', 'AGRICULTEURS', 'AGRICULTRICE', 'AGRICULTRICES'), F.lit('1'))
            .otherwise(F.lit(None).cast('string')))


def identifier_anomalies(source, **context):
    """Tache 2 : identifier et compter les anomalies avec une session Spark."""
    groupe = GROUPES[source]
    infos = context['ti'].xcom_pull(task_ids=f'{groupe}.charger')
    if not infos:
        raise ValueError(f"Chemins introuvables dans XCom pour {source}")
    spark = (SparkSession.builder.appName('DQ_Conformite_' + source)
             .master('local[2]').getOrCreate())
    try:
        df = lire_csv(spark, infos['chemin'])
        nb_lignes = df.count()
        if source.startswith('Consommation_'):
            # NULL, espaces, lettres et longueurs erronees sont non conformes.
            code_postal = F.trim(F.col('Code_Postal').cast('string'))
            nb_erreurs = df.filter(~F.coalesce(code_postal.rlike(r'^\d{5}$'), F.lit(False))).count()
            return [{'mesure': 'Conformite_Code_Postal', 'total': nb_lignes, 'erreurs': nb_erreurs}]

        if source == 'IRIS':
            # ID_Ville est un nom de ville, PAS un code postal.
            ville = normaliser_texte(F.col('ID_Ville'))
            ville_conforme = ville.isin('PARIS', 'EVRY COURCOURONNES')
            nb_erreurs = df.filter(~ville_conforme).count()
            return [{'mesure': 'Conformite_Nom_Ville_IRIS', 'total': nb_lignes, 'erreurs': nb_erreurs}]

        # Conformite CSP brute : jointure exacte ID/CSP (utile pour mesurer la qualite initiale).
        # Conformite CSP normalisee : meme left_anti apres conversion des libelles en codes.
        referentiel = (lire_csv(spark, infos['chemin_csp'])
                       .select(F.trim(F.col('ID_CSP').cast('string')).alias('code_csp'))
                       .filter(F.col('code_csp').isNotNull())
                       .dropDuplicates(['code_csp']))
        populations = df.withColumn('csp_brut', F.trim(F.col('CSP').cast('string')))
        populations = populations.withColumn('csp_normalise', code_csp_normalise(F.col('CSP')))
        erreurs_brutes = populations.join(
            referentiel, populations['csp_brut'] == referentiel['code_csp'], 'left_anti'
        ).count()
        erreurs_normalisees = populations.join(
            referentiel, populations['csp_normalise'] == referentiel['code_csp'], 'left_anti'
        ).count()
        return [
            {'mesure': 'Conformite_Referentiel_CSP_Brute', 'total': nb_lignes, 'erreurs': erreurs_brutes},
            {'mesure': 'Conformite_Referentiel_CSP_Normalisee', 'total': nb_lignes, 'erreurs': erreurs_normalisees},
        ]
    finally:
        spark.stop()


def calculer_taux(source, **context):
    """Tache 3 : calcul du taux en Python, sans relancer Spark."""
    groupe = GROUPES[source]
    donnees = context['ti'].xcom_pull(task_ids=f'{groupe}.identifier_anomalies')
    if not isinstance(donnees, list):
        raise ValueError(f"Resultats absents pour {source}")
    lignes = []
    for mesure in donnees:
        nb_lignes, nb_erreurs = mesure['total'], mesure['erreurs']
        if nb_erreurs < 0 or nb_erreurs > nb_lignes:
            raise ValueError(f"Comptages incoherents : {source}, {mesure}")
        # Aucune ligne : taux non defini, et non 100 % ou 0 % arbitraire.
        taux = round(((nb_lignes - nb_erreurs) / nb_lignes) * 100, 2) if nb_lignes else None
        lignes.append({
            'Nom_Mesure': mesure['mesure'],
            'Dimension': 'Conformité',
            'Table_Testee': source,
            'Nb_Lignes_Total': nb_lignes,
            'Nb_Erreurs': nb_erreurs,
            'Taux_Conformite_Pct': taux,
        })
        print(f"[Conformite] {mesure['mesure']} / {source} : {taux}% ({nb_erreurs}/{nb_lignes})")
    return lignes


def exporter_csv_metriques(**context):
    """Un seul export pandas : evite les ecritures concurrentes dans le CSV final."""
    lignes = []
    for source, groupe in GROUPES.items():
        resultat = context['ti'].xcom_pull(task_ids=f'{groupe}.calculer_taux')
        if not isinstance(resultat, list) or not resultat:
            raise ValueError(f"Pas de resultat pour {source}")
        lignes.extend(resultat)

    os.makedirs(DOSSIER_RESULTATS, exist_ok=True)
    df = pd.DataFrame(lignes, columns=COLONNES_EXPORT)
    # CSV final unique ; pas de dossier Parquet/JSON temporaire.
    df.to_csv(FICHIER_RESULTATS, index=False, encoding='utf-8-sig')
    print(f"[Export OK] {len(df)} mesures dans {FICHIER_RESULTATS}")


GROUPES = {
    'Consommation_Paris': 'groupe_conso_paris',
    'Consommation_Evry': 'groupe_conso_evry',
    'Population_Paris': 'groupe_pop_paris',
    'Population_Evry': 'groupe_pop_evry',
    'IRIS': 'groupe_iris',
}

with DAG(
    dag_id='mesure_conformite',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=['datascale', 'conformite'],
    description='Conformite des codes postaux, villes et CSP (avant/apres harmonisation)',
) as dag:
    fins = []
    for source, identifiant_groupe in GROUPES.items():
        with TaskGroup(group_id=identifiant_groupe):
            t_charg = PythonOperator(
                task_id='charger', python_callable=charger_source,
                op_kwargs={'source': source},
            )
            t_ident = PythonOperator(
                task_id='identifier_anomalies', python_callable=identifier_anomalies,
                op_kwargs={'source': source},
            )
            t_calc = PythonOperator(
                task_id='calculer_taux', python_callable=calculer_taux,
                op_kwargs={'source': source},
            )
            t_charg >> t_ident >> t_calc
            fins.append(t_calc)

    export = PythonOperator(task_id='exporter_csv_metriques', python_callable=exporter_csv_metriques)
    fins >> export
