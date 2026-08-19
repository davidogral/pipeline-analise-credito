import os
from typing import Any, List, Optional

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def get_pg_connection():
    """
    Returns an autocommit psycopg2 connection using environment variables:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (with sensible defaults).
    """
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "pipeline"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )
    conn.autocommit = True
    return conn


def _qualified_table(table_name: str, schema: Optional[str]) -> str:
    if schema:
        return f'{schema}."{table_name}"'
    return f'"{table_name}"'


def _pg_type(dtype) -> str:
    """Map a pandas dtype to the closest PostgreSQL column type."""
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"


def _to_python(value: Any) -> Any:
    """Convert a pandas/numpy scalar into something psycopg2 knows how to adapt."""
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.to_pydatetime()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def persist_dataframe(
    df: pd.DataFrame,
    table_name: str,
    connection,
    schema: Optional[str] = "public",
) -> None:
    """
    Drops and recreates table_name in PostgreSQL, inserting all rows from the DataFrame.
    """
    fields = [f'"{column}" {_pg_type(dtype)}' for column, dtype in df.dtypes.items()]
    ddl = f"CREATE TABLE {_qualified_table(table_name, schema)} ({', '.join(fields)})"
    rows = [tuple(_to_python(value) for value in row) for row in df.itertuples(index=False, name=None)]
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {_qualified_table(table_name, schema)} CASCADE;")
        cursor.execute(ddl)
        if rows:
            column_list = ", ".join(f'"{column}"' for column in df.columns)
            insert_sql = f"INSERT INTO {_qualified_table(table_name, schema)} ({column_list}) VALUES %s"
            execute_values(cursor, insert_sql, rows)
    print(f"Tabela {table_name} criada: {len(rows)} registros")


def list_tables(connection, schema: str = "public") -> List[str]:
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        ORDER BY table_name;
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (schema,))
        return [row[0] for row in cursor.fetchall()]
