FROM apache/airflow:2.10.2

USER root

RUN apt-get update \
  && apt-get install -y default-jre-headless \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

RUN pip install --no-cache-dir pyspark