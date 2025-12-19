FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

# Spark JAR directory yaradın
RUN mkdir -p /opt/airflow/jars

# JDBC və Iceberg driver-lərini yükləyin
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /opt/airflow/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar -P /opt/airflow/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.77.1/nessie-spark-extensions-3.5_2.12-0.77.1.jar -P /opt/airflow/jars/ && \
    wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar -P /opt/airflow/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/airflow/jars/

# JAR-lara icazə verin
RUN chown -R airflow:root /opt/airflow/jars && \
    chmod -R 755 /opt/airflow/jars

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    apache-airflow-providers-apache-spark==4.5.0 \
    apache-airflow-providers-postgres==5.9.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.12 \
    pandas==2.1.4

# Spark konfiqurasiyası
ENV SPARK_CLASSPATH=/opt/airflow/jars/*