from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# importation des modules spécifiques à PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, upper, round as spark_round


def executer_etl_pyspark():
    # initialisation du moteur Spark local
    spark = SparkSession.builder \
        .appName("ETL_Voitures_Spark") \
        .master("local[*]") \
        .getOrCreate()
        
    chemin_source = '/opt/airflow/dags/data/ventes_concession_brutes.csv'
    # Spark écrit par défaut dans un dossier du coup, on lui donne un nom de dossier cible
    chemin_cible = '/opt/airflow/dags/data/ventes_nettoyees_spark_dossier'
    
    # EXTRACT : 
    # ici le inferSchema est important pour que Spark comprenne que certaines colonnes sont des nombres et pas des chaînes de caractères et les dates au format date et pas des chaînes de caractères   
    df = spark.read.csv(chemin_source, header=True, inferSchema=True) 
    
    # TRANSFORM :
    df_transforme = df \
        .filter(col("quantite") >= 1) \
        .withColumn("date_vente", to_date(col("date_vente"), "dd/MM/yyyy")) \
        .withColumn("marque", upper(col("marque"))) \
        .withColumn("montant_total", spark_round(col("quantite") * col("prix_unitaire"), 2))
        
    # LOAD :
    # coalesce(1) force Spark à tout rassembler dans un seul fichier CSV (au lieu de le découper)
    df_transforme.coalesce(1).write.mode("overwrite").csv(chemin_cible, header=True)
    
    spark.stop()

# config du DAG Airflow (rien change ici)
with DAG(
    dag_id='etl_voitures_pyspark',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['projet_ecole']
) as dag:

    # création de la tâche
    tache_etl = PythonOperator(
        task_id='nettoyage_pyspark',
        python_callable=executer_etl_pyspark
    )