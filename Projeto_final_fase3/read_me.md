# Pipeline de Dados – Análise de Crédito

Instruções completas para reproduzir o pipeline (Spark), carregar os dados no PostgreSQL e, opcionalmente, orquestrar com Airflow no VS Code.

> Esta é a branch `spark`, que roda com **PySpark** (requer Java 17). A mesma pipeline implementada em **pandas puro** está na branch `main`.

## Estrutura do repositório
- `data/raw/dados_credito.xlsx`: fonte original.
- `data/bronze | silver | gold`: saídas das camadas.
- `scripts/01_*.py`: scripts python.
- `spark_utils.py` / `db_utils.py`: criação da SparkSession e persistência em PostgreSQL.
- Notebooks `01_bronze_layer.ipynb` → `07_monitoring.ipynb`: mesmo fluxo das scripts.

## Pré-requisitos
- Python 3.10+ e pip.
- Java 17 (necessário para PySpark).
- Docker (para subir PostgreSQL e opcionalmente Airflow).
- VS Code com extensões **Python** e **Jupyter**.

## Setup local rápido
1) Crie o ambiente Python:
```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
2) Garanta o Java disponível (ex.: `java -version`) e, se precisar, exporte o caminho:
```bash
export JAVA_HOME=$(/usr/libexec/java_home)   # macOS
```
3) Teste o Spark no terminal integrado do VS Code:
```bash
python - <<'PY'
from spark_utils import get_spark
spark = get_spark("SmokeTest")
print("Spark ok:", spark.version)
PY
```

## Banco de Dados (PostgreSQL local)
Os scripts usam as variáveis `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` (defaults: localhost:5432/pipeline/postgres/postgres).
```bash
docker run --name pipeline-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pipeline -p 5432:5432 -d postgres:14
export PGHOST=localhost PGPORT=5432 PGDATABASE=pipeline PGUSER=postgres PGPASSWORD=postgres
```

## Execução do pipeline com Spark
Opcionalmente rode no VS Code, execute diretamente pelas scripts:
```bash
python scripts/01_bronze_layer.py    # cria data/bronze/dados_brutos.csv
python scripts/02_silver_layer.py    # limpeza → data/silver/dados_limpos.csv
python scripts/03_gold_layer.py      # agregações → data/gold/*.csv
python scripts/04_load_database.py   # carrega tabelas no PostgreSQL
python scripts/05_sql_queries.ipynb.py  # consultas Spark SQL
python scripts/06_quality_report.py  # gera quality_report.png
python scripts/Ml_score.py           # exploração + modelo de regressão
```

## Orquestração com Airflow 
Instale o Airflow no mesmo `venv` (exemplo com Python 3.10):
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
python -m pip install "apache-airflow==2.9.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```
Crie um DAG em `$AIRFLOW_HOME/dags/` com `BashOperator` ou `PythonOperator` chamando os comandos da seção anterior na mesma ordem. Para subir a UI:
```bash
airflow webserver -p 8080 &
airflow scheduler &
```
Abra http://localhost:8080, ative o DAG e acompanhe as execuções.

## Outputs esperados
- CSVs processados em `data/bronze`, `data/silver`, `data/gold`.
- Tabelas no PostgreSQL: `projeto_final`, `metricas_estado`, `ativos_patrimonio`.
- Relatório de qualidade: `quality_report.png`.
- Artefatos de ML: métricas impressas no console (script `Ml_score.py`).
