"""Carga no PostgreSQL: publica as tabelas Silver e Gold em uma única transação.

Se qualquer tabela falhar, nada é publicado e o banco continua com a última
carga bem-sucedida. As tabelas são pequenas (~10 mil linhas), então são
coletadas via Arrow e inseridas em lote pelo psycopg2, o que permite criar
PK/FK e publicar tudo atomicamente. Para volumes maiores, o caminho seria o
conector JDBC do Spark escrevendo em tabelas de staging.
"""

from __future__ import annotations

import logging

from credit_pipeline.config import SILVER, Paths, PostgresSettings, gold
from credit_pipeline.io import read_layer

logger = logging.getLogger(__name__)

PK_CLIENTE = "codigo_cliente"


def tables_to_load(paths: Paths) -> dict:
    """Tabelas publicadas no banco, lidas das camadas Silver e Gold."""
    return {
        "clientes_credito": read_layer(paths, SILVER),
        "analise_clientes": read_layer(paths, gold("analise_clientes")),
        "metricas_estado": read_layer(paths, gold("metricas_estado")),
        "ativos_patrimonio": read_layer(paths, gold("ativos_patrimonio")),
    }


def run(paths: Paths, settings: PostgresSettings | None = None) -> dict[str, int]:
    # Import tardio: o driver só é necessário (e instalado, via extra [postgres]) quando há carga no banco.
    from credit_pipeline.db import get_pg_connection, list_tables, replace_table

    tables = {name: df.toPandas() for name, df in tables_to_load(paths).items()}
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
