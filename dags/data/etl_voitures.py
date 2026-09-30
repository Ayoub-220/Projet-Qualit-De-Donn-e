import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Fonction qui contient la logique ETL
def executer_etl():
    # Les chemins tels qu'ils sont vus à l'intérieur du conteneur Docker
    chemin_source = '/opt/airflow/dags/data/ventes_concession_brutes.csv'
    chemin_cible = '/opt/airflow/dags/data/ventes_concession_nettoyees.csv'
    
    lignes_propres = []
    
    # EXTRACT : Lecture du fichier source
    with open(chemin_source, mode='r', encoding='utf-8') as fichier:
        lecteur = csv.DictReader(fichier)
        
        # TRANSFORM : Application des règles métier
        for ligne in lecteur:
            quantite = int(ligne['quantite'])
            
            # Règle 1 : Filtre des rejets (on ignore les quantités négatives)
            if quantite < 1:
                continue 
            
            # Règle 2 : Standardisation de la date (de JJ/MM/AAAA vers AAAA-MM-JJ)
            date_obj = datetime.strptime(ligne['date_vente'], '%d/%m/%Y')
            ligne['date_vente'] = date_obj.strftime('%Y-%m-%d')
            
            # Règle 3 : Nettoyage de texte (Marque en majuscules)
            ligne['marque'] = ligne['marque'].upper()
            
            # Règle 4 : Calcul métier
            prix = float(ligne['prix_unitaire'])
            ligne['montant_total'] = round(quantite * prix, 2)
            
            lignes_propres.append(ligne)
            
    # LOAD : Écriture du fichier résultat
    with open(chemin_cible, mode='w', newline='', encoding='utf-8') as fichier_cible:
        champs = ['id_vente', 'date_vente', 'marque', 'modele', 'quantite', 'prix_unitaire', 'montant_total']
        ecrivain = csv.DictWriter(fichier_cible, fieldnames=champs)
        ecrivain.writeheader()
        ecrivain.writerows(lignes_propres)

# 2. Configuration du DAG Airflow
with DAG(
    dag_id='etl_voitures',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None, # Exécution manuelle pour cet exemple
    catchup=False,
    tags=['projet_ecole']
) as dag:

    # 3. Création de la tâche qui appelle la fonction
    tache_etl = PythonOperator(
        task_id='nettoyage_et_calcul_ventes',
        python_callable=executer_etl
    )