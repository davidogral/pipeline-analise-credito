"""Carga no PostgreSQL: publica as tabelas Silver e Gold em uma única transação.

Se qualquer tabela falhar, nada é publicado e o banco continua com a última
carga bem-sucedida.
"""

from __future__ import annotations

import logging

from credit_pipeline.config import Paths, PostgresSettings
from credit_pipeline.db import get_pg_connection, list_tables, replace_table
from credit_pipeline.io import read_layer_csv

logger = logging.getLogger(__name__)

PK_CLIENTE = "codigo_cliente"


def tables_to_load(paths: Paths) -> dict:
    return {
        "clientes_credito": read_layer_csv(paths.silver_file),
        "analise_clientes": read_layer_csv(paths.gold_dir / "analise_clientes.csv"),
        "metricas_estado": read_layer_csv(paths.gold_dir / "metricas_estado.csv"),
        "ativos_patrimonio": read_layer_csv(paths.gold_dir / "ativos_patrimonio.csv"),
    }


def run(paths: Paths, settings: PostgresSettings | None = None) -> dict[str, int]:
    tables = tables_to_load(paths)
    loaded: dict[str, int] = {}

    conn = get_pg_connection(settings)
    try:
        with conn, conn.cursor() as cursor:
            for name, df in tables.items():
                loaded[name] = replace_table(cursor, df, name)
                logger.info("Load: %s com %d registros", name, loaded[name])

            cursor.execute(f"ALTER TABLE clientes_credito ADD PRIMARY KEY ({PK_CLIENTE})")
            cursor.execute(f"ALTER TABLE analise_clientes ADD PRIMARY KEY ({PK_CLIENTE})")
            cursor.execute(
                f"ALTER TABLE analise_clientes ADD FOREIGN KEY ({PK_CLIENTE}) "
                f"REFERENCES clientes_credito ({PK_CLIENTE})"
            )
            cursor.execute("ALTER TABLE metricas_estado ADD PRIMARY KEY (uf)")
            cursor.execute("ALTER TABLE ativos_patrimonio ADD PRIMARY KEY (faixa_etaria)")
            logger.info("Load: tabelas publicadas -> %s", list_tables(cursor))
    finally:
        conn.close()
    return loaded
