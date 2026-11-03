from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import pandas as pd

def sauvegarder_resultat_dq(mesure, dimension, table, nb_lignes, nb_erreurs):
    dossier = '/opt/airflow/dags/data/output/audit_results/heterogeneite'
    os.makedirs(dossier, exist_ok=True)
    
    fichier_global = f'{dossier}/audit_heterogeneite.csv'
    
    taux = round(((nb_lignes - nb_erreurs) / nb_lignes) * 100, 2) if nb_lignes > 0 else 0.0
    
    df = pd.DataFrame(
        [[mesure, dimension, table, nb_lignes, nb_erreurs, taux]],
        columns=['Nom_Mesure', 'Dimension', 'Table_Testee', 'Nb_Lignes_Total', 'Nb_Erreurs', 'Taux_Conformite_Pct']
    )
    
    # Consolidation dans le fichier unique
    if os.path.exists(fichier_global):
        df_exist = pd.read_csv(fichier_global)
        df_final = pd.concat([df_exist, df]).drop_duplicates(subset=['Nom_Mesure', 'Table_Testee'], keep='last')
    else:
        df_final = df
        
    df_final.to_csv(fichier_global, index=False)
    print(f"[{dimension}] {mesure} sur {table} : {taux}% (Erreurs: {nb_erreurs}/{nb_lignes})")

# Hétérogénéité d'Échelle (Watts vs kW et négatifs)
def mesurer_heterogeneite_echelle():
    spark = SparkSession.builder.appName("DQ_Heterogeneite_Echelle").master("local[*]").getOrCreate()
    
    for source, path in [("S1_Consommation", '/opt/airflow/dags/data/S1_Consommation.csv'), 
                         ("S2_Consommation", '/opt/airflow/dags/data/S2_Consommation.csv')]:
        df = spark.read.csv(path, header=True, inferSchema=True)
        df = df.withColumn("KW_Num", col("NB_KW_Jour").cast("double"))
        nb = df.count()
        
        # Détection des erreurs : valeurs négatives ou valeurs anormalement hautes (en Wh au lieu de kWh)
        erreurs_echelle = df.filter((col("KW_Num") < 0) | (col("KW_Num") > 100000))
        err = erreurs_echelle.count()
        
        sauvegarder_resultat_dq('Heterogeneite_Echelle_KW', 'Heterogeneite', source, nb, err)
        
    spark.stop()

# Configuration du DAG Airflow
with DAG(
    dag_id='mesure_heterogeneite',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['datascale', 'heterogeneite']
) as dag:
    
    t_echelle = PythonOperator(
        task_id='mesurer_heterogeneite_echelle',
        python_callable=mesurer_heterogeneite_echelle
    )
    
    t_echelle