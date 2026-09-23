"""Executa as consultas analíticas de `sql/`.

As mesmas queries rodam no PostgreSQL (após a carga) ou em um SQLite em memória
montado a partir das camadas Silver e Gold, útil para rodar sem banco.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

from credit_pipeline.config import PROJECT_ROOT, Paths
from credit_pipeline.load import tables_to_load

logger = logging.getLogger(__name__)

SQL_DIR = PROJECT_ROOT / "sql"


def sqlite_from_layers(paths: Paths) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for name, df in tables_to_load(paths).items():
        df.columns = [column.lower() for column in df.columns]
        df.to_sql(name, conn, index=False)
    return conn


def query(conn, statement: str) -> pd.DataFrame:
    """Executa via cursor DB-API, o que funciona igual para sqlite3 e psycopg2."""
    cursor = conn.cursor()
    try:
        cursor.execute(statement)
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        cursor.close()


def run_queries(conn, sql_dir: Path = SQL_DIR) -> dict[str, pd.DataFrame]:
    return {sql_file.stem: query(conn, sql_file.read_text()) for sql_file in sorted(sql_dir.glob("*.sql"))}


def run(paths: Paths, engine: str = "sqlite") -> dict[str, pd.DataFrame]:
    if engine == "postgres":
        from credit_pipeline.db import get_pg_connection

        conn = get_pg_connection()
    else:
        conn = sqlite_from_layers(paths)
    try:
        results = run_queries(conn)
    finally:
        conn.close()

    for name, result in results.items():
        print(f"\n== {name} ==\n{result.to_string(index=False)}")
    logger.info("Analytics: %d consultas executadas via %s", len(results), engine)
    return results
