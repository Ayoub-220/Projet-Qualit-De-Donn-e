# 1. on part de l'image officielle d'Airflow (ajuste la version si tu en utilises une autre)
FROM apache/airflow:2.10.2

# 2. mode "administrateur" (root) pour installer des logiciels système
USER root

# 3. mise à jour du système et on installe Java (obligatoire pour Spark)
RUN apt-get update \
  && apt-get install -y default-jre-headless \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 4. l'environnement où trouver Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 5. on repasse sur l'utilisateur "airflow" normal par mesure de sécurité
USER airflow

# 6. on installe la bibliothèque PySpark en Python
RUN pip install --no-cache-dir pyspark