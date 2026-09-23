from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from credit_pipeline.config import PostgresSettings


def get_pg_connection(settings: PostgresSettings | None = None):
    """Abre uma conexão psycopg2 (sem autocommit) a partir das variáveis PG*."""
    settings = settings or PostgresSettings.from_env()
    return psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
    )


def pg_type(dtype) -> str:
    """Mapeia um dtype do pandas para o tipo de coluna PostgreSQL mais próximo."""
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    return "TEXT"


def to_python(value: Any) -> Any:
    """Converte escalares numpy/pandas em tipos que o psycopg2 sabe adaptar (NaN/NaT viram NULL)."""
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


def replace_table(cursor, df: pd.DataFrame, table_name: str, schema: str = "public") -> int:
    """Recria a tabela com o schema do DataFrame e insere todas as linhas. Não faz commit."""
    table = sql.Identifier(schema, table_name.lower())
    columns = [sql.Identifier(column.lower()) for column in df.columns]
    fields = sql.SQL(", ").join(
        sql.SQL("{} {}").format(column, sql.SQL(pg_type(dtype)))
        for column, dtype in zip(columns, df.dtypes, strict=True)
    )
    rows = [tuple(to_python(value) for value in row) for row in df.itertuples(index=False, name=None)]

    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(table))
    cursor.execute(sql.SQL("CREATE TABLE {} ({})").format(table, fields))
    if rows:
        insert = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(table, sql.SQL(", ").join(columns))
        execute_values(cursor, insert.as_string(cursor), rows, page_size=1000)
    return len(rows)


def list_tables(cursor, schema: str = "public") -> list[str]:
    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name",
        (schema,),
    )
    return [row[0] for row in cursor.fetchall()]
