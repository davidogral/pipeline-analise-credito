# Pipeline de Dados – Análise de Crédito (versão pandas)

Instruções completas para reproduzir o pipeline (pandas), carregar os dados no PostgreSQL e, opcionalmente, orquestrar com Airflow no VS Code.

> Esta é a branch `main`, que roda com **pandas puro**. A mesma pipeline implementada em **PySpark** está na branch `spark`.

## Estrutura do repositório
- `data/raw/dados_credito.xlsx`: fonte original.
- `data/bronze | silver | gold`: saídas das camadas.
- `scripts/01_*.py`: scripts python.
- `pandas_utils.py` / `db_utils.py`: leitura/escrita das camadas em CSV e persistência em PostgreSQL.
- Notebooks `01_bronze_layer.ipynb` → `07_monitoring.ipynb`: mesmo fluxo das scripts.

## Pré-requisitos
- Python 3.10+ e pip.
- Docker (para subir PostgreSQL e opcionalmente Airflow).
- VS Code com extensões **Python** e **Jupyter**.

Não é necessário Java: a versão pandas não usa Spark.

## Setup local rápido
1) Crie o ambiente Python:
```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
2) Teste a instalação no terminal integrado do VS Code:
```bash
python - <<'PY'
import pandas as pd
print("pandas ok:", pd.__version__)
PY
```

## Banco de Dados (PostgreSQL local)
Os scripts usam as variáveis `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` (defaults: localhost:5432/pipeline/postgres/postgres).
```bash
docker run --name pipeline-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pipeline -p 5432:5432 -d postgres:14
export PGHOST=localhost PGPORT=5432 PGDATABASE=pipeline PGUSER=postgres PGPASSWORD=postgres
```

## Execução do pipeline com pandas
Rode sempre a partir da raiz do projeto (`Projeto_final_fase3`), porque os caminhos de dados são relativos:
```bash
python scripts/01_bronze_layer.py    # cria data/bronze/dados_brutos.csv
python scripts/02_silver_layer.py    # limpeza → data/silver/dados_limpos.csv
python scripts/03_gold_layer.py      # agregações → data/gold/*.csv
python scripts/04_load_database.py   # carrega tabelas no PostgreSQL
python scripts/05_sql_queries.ipynb.py  # consultas SQL (SQLite em memória)
python scripts/06_quality_report.py  # gera quality_report.png
python scripts/Ml_score.py           # exploração + modelo de regressão
```
Os notebooks executam o mesmo código, célula a célula, com o diretório de trabalho na raiz do projeto.

## Orquestração com Airflow
Instale o Airflow no mesmo `venv` (exemplo com Python 3.10):
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
python -m pip install "apache-airflow==2.9.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```
O DAG `pipeline_credito` já está em `airflow_home/dags/pipeline_credito.py` e encadeia `bronze >> silver >> gold >> load_db`. Para subir a UI:
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

## Diferenças em relação à branch `spark`
| Tema | `spark` | `main` (pandas) |
| --- | --- | --- |
| Motor | PySpark + `spark_utils.py` | pandas + `pandas_utils.py` |
| Pré-requisito | Java 17 | nenhum além do Python |
| Camada SQL | Spark SQL (`spark.sql`) | SQLite em memória (`pd.read_sql_query`), mesmas queries |
| ML | `pyspark.ml` (StringIndexer + LinearRegression) | scikit-learn (OrdinalEncoder + LinearRegression) |
| Saídas | idênticas: mesmos CSVs de bronze, silver e gold | idênticas |
