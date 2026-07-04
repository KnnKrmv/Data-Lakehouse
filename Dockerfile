FROM apache/airflow:2.7.3-python3.8

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Spark-ı düzgün quraşdırın (binary ilə) — jar-lar yoxdur, sadəcə core Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

# İcazələr
RUN chown -R airflow:root /opt/spark && \
    chmod -R 755 /opt/spark

USER airflow

RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    apache-airflow-providers-apache-spark==4.5.0 \
    apache-airflow-providers-postgres==5.9.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.12 \
    pandas==2.0.3