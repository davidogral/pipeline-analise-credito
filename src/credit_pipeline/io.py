"""Leitura e gravação das camadas, em Parquet local ou em tabelas Delta no Unity Catalog."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame

from credit_pipeline.config import Layer, Paths
from credit_pipeline.spark import get_spark

logger = logging.getLogger(__name__)


def read_layer(paths: Paths, layer: Layer) -> DataFrame:
    spark = get_spark()
    if paths.storage == "delta":
        return spark.read.table(paths.table(layer))
    return spark.read.parquet(str(paths.location(layer)))


def write_layer(
    df: DataFrame, paths: Paths, layer: Layer, mode: str = "overwrite", merge_keys: list[str] | None = None
) -> str:
    """Grava a camada.

    - ``mode="overwrite"`` recria a tabela; ``mode="append"`` acumula histórico.
    - ``merge_keys`` (só no modo Delta) faz upsert com ``MERGE INTO`` pelas chaves,
      atualizando registros existentes e inserindo os novos.
    """
    if paths.storage == "parquet":
        target = str(paths.location(layer))
        df.write.mode(mode).parquet(target)
        return target

    table = paths.table(layer)
    spark = get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {paths.catalog}.{layer.schema}")
    if merge_keys and spark.catalog.tableExists(table):
        merge_into(df, table, merge_keys)
    else:
        df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table)
    return table


def merge_into(df: DataFrame, table: str, keys: list[str]) -> None:
    source = f"merge_source_{table.replace('.', '_')}"
    df.createOrReplaceTempView(source)
    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    get_spark().sql(
        f"MERGE INTO {table} AS t USING {source} AS s ON {condition} "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    logger.info("MERGE em %s pelas chaves %s", table, keys)
