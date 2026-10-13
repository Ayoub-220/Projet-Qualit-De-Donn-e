from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, avg, round as spark_round
import os
import glob
import shutil

def executer_etl_paris_pyspark():
    spark = SparkSession.builder \
        .appName("ETL_Paris_Spark") \
        .master("local[*]") \
        .getOrCreate()
        
    chemin_s1 = '/opt/airflow/dags/data/S1_Consommation.csv'
    chemin_s4 = '/opt/airflow/dags/data/S4_IRIS.csv'
    dossier_temp = '/opt/airflow/dags/data/output/temp_paris'
    fichier_final = '/opt/airflow/dags/data/output/Consommation_IRIS_Paris.csv'
    
    os.makedirs('/opt/airflow/dags/data/output', exist_ok=True)
    
    s1_conso = spark.read.csv(chemin_s1, header=True, inferSchema=True)
    s4_iris = spark.read.csv(chemin_s4, header=True, inferSchema=True)
    
    df_join = s1_conso.join(
        s4_iris,
        (lower(trim(s1_conso.Nom_Rue)) == lower(trim(s4_iris.ID_Rue))) & 
        (trim(s1_conso.Code_Postal) == trim(s4_iris.Code_Postal)),
        "inner"
    )
    
    df_paris = df_join.groupBy("ID_Iris") \
        .agg(spark_round(avg(col("NB_KW_Jour")) * 365, 2).alias("Conso_moyenne_annuelle")) \
        .select(col("ID_Iris").alias("ID_IRIS"), col("Conso_moyenne_annuelle"))
        
    df_paris.coalesce(1).write.mode("overwrite").csv(dossier_temp, header=True)
    spark.stop()

    # permet de récupérer le nom du fichier généré par Spark (qui est un nom aléatoire) et de le renommer en notre fichier final
    fichier_part = glob.glob(os.path.join(dossier_temp, 'part-*.csv'))[0]
    shutil.move(fichier_part, fichier_final)
    shutil.rmtree(dossier_temp)

def executer_etl_evry_pyspark():
    spark = SparkSession.builder \
        .appName("ETL_Evry_Spark") \
        .master("local[*]") \
        .getOrCreate()
        
    chemin_s2 = '/opt/airflow/dags/data/S2_Consommation.csv'
    chemin_s4 = '/opt/airflow/dags/data/S4_IRIS.csv'
    dossier_temp = '/opt/airflow/dags/data/output/temp_evry'
    fichier_final = '/opt/airflow/dags/data/output/Consommation_IRIS_Evry.csv'
    
    os.makedirs('/opt/airflow/output', exist_ok=True)
    
    s2_conso = spark.read.csv(chemin_s2, header=True, inferSchema=True)
    s4_iris = spark.read.csv(chemin_s4, header=True, inferSchema=True)
    
    df_join = s2_conso.join(
        s4_iris,
        (lower(trim(s2_conso.Nom_Rue)) == lower(trim(s4_iris.ID_Rue))) & 
        (trim(s2_conso.Code_Postal) == trim(s4_iris.Code_Postal)),
        "inner"
    )
    
    df_evry = df_join.groupBy("ID_Iris") \
        .agg(spark_round(avg(col("NB_KW_Jour")) * 365, 2).alias("Conso_moyenne_annuelle")) \
        .select(col("ID_Iris").alias("ID_IRIS"), col("Conso_moyenne_annuelle"))
        
    df_evry.coalesce(1).write.mode("overwrite").csv(dossier_temp, header=True)
    spark.stop()

    # permet de récupérer le nom du fichier généré par Spark (qui est un nom aléatoire) et de le renommer en notre fichier final
    fichier_part = glob.glob(os.path.join(dossier_temp, 'part-*.csv'))[0]
    shutil.move(fichier_part, fichier_final)
    shutil.rmtree(dossier_temp)

# config du dag airflow
with DAG(
    dag_id='etl_consommation_villes',
    start_date=datetime(2026, 13, 10),
    schedule_interval=None,
    catchup=False,
    tags=['datascale']
) as dag:

    tache_paris = PythonOperator(
        task_id='generer_table_cible_paris',
        python_callable=executer_etl_paris_pyspark
    )

    tache_evry = PythonOperator(
        task_id='generer_table_cible_evry',
        python_callable=executer_etl_evry_pyspark
    )

    # Les deux taches n'ont pas de dependance entre elles, 
    # elles s'executeront en parallele !
    [tache_paris, tache_evry]