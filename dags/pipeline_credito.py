"""DAG diária do pipeline de crédito: bronze >> silver >> gold >> quality >> load.

Cada tarefa chama a mesma função usada pela CLI (`credit-pipeline run`), então
o que roda no Airflow é exatamente o que é testado no CI. A etapa `quality`
funciona como gate: se uma regra crítica falhar, a carga no banco não acontece.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from credit_pipeline.config import Paths

DEFAULT_ARGS = {
    "owner": "dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_credito",
    description="Ingestão, tratamento, validação e carga dos dados de crédito",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["credito", "medallion"],
)
def pipeline_credito():
    @task
    def bronze():
        from credit_pipeline import bronze

        return len(bronze.run(Paths.from_env()))

    @task
    def silver():
        from credit_pipeline import silver

        return len(silver.run(Paths.from_env()))

    @task
    def gold():
        from credit_pipeline import gold

        return {name: len(df) for name, df in gold.run(Paths.from_env()).items()}

    @task
    def quality():
        from credit_pipeline import quality

        report = quality.run(Paths.from_env())
        return {key: report[key] for key in ["regras_aprovadas", "regras_total", "completude_pct"]}

    @task
    def load():
        from credit_pipeline import load

        return load.run(Paths.from_env())

    bronze() >> silver() >> gold() >> quality() >> load()


pipeline_credito()
