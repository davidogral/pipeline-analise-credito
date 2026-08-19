from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Diretório base do projeto
BASE_DIR = "/Users/davispecia/Documents/GitHub/Pf_pipeline_dados/Projeto_final_fase3"
# Comandos comuns: ativar venv e setar variáveis do Postgres (porta 5433)
ENV = (
    f"cd {BASE_DIR} && "
    f"source {BASE_DIR}/venv/bin/activate && "
    "export PGHOST=localhost PGPORT=5433 PGDATABASE=pipeline PGUSER=postgres PGPASSWORD=postgres && "
    f"cd {BASE_DIR}"
)

with DAG(
    dag_id="pipeline_credito",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"{ENV} && python scripts/01_bronze_layer.py",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{ENV} && python scripts/02_silver_layer.py",
    )
    gold = BashOperator(
        task_id="gold",
        bash_command=f"{ENV} && python scripts/03_gold_layer.py",
    )
    load_db = BashOperator(
        task_id="load_db",
        bash_command=f"{ENV} && python scripts/04_load_database.py",
    )

    bronze >> silver >> gold >> load_db
