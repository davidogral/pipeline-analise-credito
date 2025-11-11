import os
from typing import List, Sequence

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame


TYPE_MAPPING = {
    "StringType": "TEXT",
    "IntegerType": "INTEGER",
    "LongType": "BIGINT",
    "DoubleType": "DOUBLE PRECISION",
    "FloatType": "REAL",
    "BooleanType": "BOOLEAN",
    "TimestampType": "TIMESTAMP",
}


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


def _qualified_table(table_name: str, schema: str | None) -> str:
    if schema:
        return f'{schema}."{table_name}"'
    return f'"{table_name}"'


def persist_dataframe(
    df: DataFrame,
    table_name: str,
    connection,
    schema: str | None = "public",
) -> None:
    """
    Drops and recreates table_name in PostgreSQL, inserting all rows from the Spark DataFrame.
    """
    fields = []
    for field in df.schema:
        data_type = TYPE_MAPPING.get(type(field.dataType).__name__, "TEXT")
        fields.append(f'"{field.name}" {data_type}')
    ddl = f"CREATE TABLE {_qualified_table(table_name, schema)} ({', '.join(fields)})"
    rows = [tuple(row[col] for col in df.columns) for row in df.collect()]
    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {_qualified_table(table_name, schema)} CASCADE;")
        cursor.execute(ddl)
        if rows:
            column_list = ", ".join(f'"{col}"' for col in df.columns)
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
