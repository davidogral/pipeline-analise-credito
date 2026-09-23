"""Executa as consultas analíticas de `sql/`.

As mesmas queries rodam no Spark SQL, sobre views temporárias das camadas
Silver e Gold, ou no PostgreSQL após a carga.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from credit_pipeline.config import PROJECT_ROOT, Paths
from credit_pipeline.load import tables_to_load
from credit_pipeline.spark import get_spark

logger = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"


def sql_files(sql_dir: Path = SQL_DIR) -> list[Path]:
    return sorted(sql_dir.glob("*.sql"))


def register_views(paths: Paths) -> None:
    """Expõe cada tabela como view temporária, com colunas em minúsculas como no PostgreSQL."""
    for name, df in tables_to_load(paths).items():
        df.toDF(*[column.lower() for column in df.columns]).createOrReplaceTempView(name)


def query_postgres(conn, statement: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(statement)
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def run(paths: Paths, engine: str = "spark") -> dict[str, pd.DataFrame]:
    if engine == "postgres":
        from credit_pipeline.db import get_pg_connection

        conn = get_pg_connection()
        try:
            results = {f.stem: query_postgres(conn, f.read_text()) for f in sql_files()}
        finally:
            conn.close()
    else:
        register_views(paths)
        spark = get_spark()
        # Spark SQL não aceita o ";" final que o PostgreSQL tolera.
        results = {f.stem: spark.sql(f.read_text().strip().rstrip(";")).toPandas() for f in sql_files()}

    for name, result in results.items():
        print(f"\n== {name} ==\n{result.to_string(index=False)}")
    logger.info("Analytics: %d consultas executadas via %s", len(results), engine)
    return results
