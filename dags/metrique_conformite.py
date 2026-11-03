from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import pandas as pd

def sauvegarder_resultat_dq(mesure, dimension, table, nb_lignes, nb_erreurs):
    dossier = '/opt/airflow/dags/data/output/audit_results/conformite'
    os.makedirs(dossier, exist_ok=True)
    audit = f'{dossier}/audit_conformite.csv'
    
    taux = round(((nb_lignes - nb_erreurs) / nb_lignes) * 100, 2) if nb_lignes > 0 else 0.0
    
    df = pd.DataFrame(
        [[mesure, dimension, table, nb_lignes, nb_erreurs, taux]],
        columns=['Nom_Mesure', 'Dimension', 'Table_Testee', 'Nb_Lignes_Total', 'Nb_Erreurs', 'Taux_Conformite_Pct']
    )

    # consolidation dans le fichier unique parce que l'on veut un audit global pour toutes les mesures de conformité
    if os.path.exists(audit):
        df_exist = pd.read_csv(audit)
        df_final = pd.concat([df_exist, df]).drop_duplicates(subset=['Nom_Mesure', 'Table_Testee'], keep='last')
    else:
        df_final = df
        
    df_final.to_csv(audit, index=False)
    print(f"[Conformité] {mesure} sur {table} : {taux}% (Erreurs: {nb_erreurs}/{nb_lignes})")

# 1 : Conformité des Codes Postaux (Consommations + S4_IRIS)
def mesurer_conformite_code_postal():
    spark = SparkSession.builder.appName("DQ_Conformite_CP").master("local[*]").getOrCreate()
    sources = [
        ("S1_Consommation", '/opt/airflow/dags/data/S1_Consommation.csv', 'Code_Postal'),
        ("S2_Consommation", '/opt/airflow/dags/data/S2_Consommation.csv', 'Code_Postal'),
        ("S4_IRIS", '/opt/airflow/dags/data/S4_IRIS.csv', 'ID_Ville')
    ]
    
    for source, path, col_name in sources:
        df = spark.read.csv(path, header=True, inferSchema=True)
        df = df.withColumn("CP_Str", col(col_name).cast("string"))
        nb = df.count()
        err = df.filter(~col("CP_Str").rlike(r'^\d{5}$')).count()
        sauvegarder_resultat_dq('Conformite_Code_Postal', 'Conformité', source, nb, err)
        
    spark.stop()

# 2 : Conformité du Référentiel CSP (Populations)
def mesurer_conformite_referentiel_csp():
    spark = SparkSession.builder.appName("DQ_Conformite_CSP").master("local[*]").getOrCreate()
    df_csp = spark.read.csv('/opt/airflow/dags/data/S3_CSP.csv', header=True, inferSchema=True)
    
    for source, path in [("S1_Population", '/opt/airflow/dags/data/S1_Population.csv'), 
                         ("S2_Population", '/opt/airflow/dags/data/S2_Population.csv')]:
        df_pop = spark.read.csv(path, header=True, inferSchema=True)
        nb = df_pop.count()
        # left anti join pour trouver les populations dont le CSP n'existe pas dans le référentiel
        orphelins = df_pop.join(df_csp, df_pop.CSP == df_csp.ID_CSP, "left_anti")
        err = orphelins.count()
        sauvegarder_resultat_dq('Conformite_Referentiel_CSP', 'Conformité', source, nb, err)
    spark.stop()

with DAG(
    dag_id='mesure_conformite',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['datascale', 'conformite']
) as dag:
    
    t_code_postal = PythonOperator(
        task_id='mesurer_conformite_code_postal',
        python_callable=mesurer_conformite_code_postal
    )
    
    t_referentiel_csp = PythonOperator(
        task_id='mesurer_conformite_referentiel_csp',
        python_callable=mesurer_conformite_referentiel_csp
    )
    [t_code_postal, t_referentiel_csp]