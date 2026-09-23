# Airflow com Java 17, necessário para o PySpark rodar dentro das tarefas.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17 \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
RUN pip install --no-cache-dir "pyspark>=3.5,<4" "pyarrow>=14" "pandas>=2.1,<3" openpyxl matplotlib psycopg2-binary
