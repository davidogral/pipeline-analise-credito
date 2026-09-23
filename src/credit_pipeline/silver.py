"""Camada Silver: limpeza, tipagem, padronização e tratamento de nulos, outliers e duplicatas."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from credit_pipeline.config import Paths
from credit_pipeline.io import read_layer, write_layer

logger = logging.getLogger(__name__)

BOOLEAN_COLUMNS = ["CASA_PROPRIA", "OUTRA_RENDA", "TRABALHANDO_ATUALMENTE"]
INTEGER_COLUMNS = ["IDADE", "QT_FILHOS", "QT_IMOVEIS", "TEMPO_ULTIMO_EMPREGO_MESES", "QT_CARROS", "SCORE"]
FLOAT_COLUMNS = ["VL_IMOVEIS", "OUTRA_RENDA_VALOR", "ULTIMO_SALARIO", "VALOR_TABELA_CARROS"]

# Regra de negócio: acima de 3 filhos é tratado como erro de digitação e substituído pela moda.
MAX_QT_FILHOS = 3


def transform(df: DataFrame) -> DataFrame:
    """Aplica todas as regras da camada Silver."""
    for col in BOOLEAN_COLUMNS:
        cleaned = F.upper(F.trim(F.col(col)))
        df = df.withColumn(
            col,
            F.when(cleaned == "SIM", True).when(cleaned.isin("NAO", "NÃO"), False).otherwise(None),
        )

    df = df.select(
        *[F.col(c).cast("int").alias(c) if c in INTEGER_COLUMNS else F.col(c) for c in df.columns]
    ).select(*[F.col(c).cast("double").alias(c) if c in FLOAT_COLUMNS else F.col(c) for c in df.columns])

    # Padroniza todo texto em caixa alta e sem espaços nas bordas.
    string_columns = {name for name, dtype in df.dtypes if dtype == "string"}
    df = df.select(*[F.upper(F.trim(F.col(c))).alias(c) if c in string_columns else F.col(c) for c in df.columns])
    df = df.replace("SEM DADOS", None, subset=list(string_columns))

    # Mediana exata (relativeError=0) para ficar igual à versão pandas.
    median_salario = df.approxQuantile("ULTIMO_SALARIO", [0.5], 0.0)
    if median_salario:
        df = df.fillna({"ULTIMO_SALARIO": median_salario[0]})

    moda = df.groupBy("QT_FILHOS").count().orderBy(F.desc("count"), "QT_FILHOS").first()
    moda_filhos = moda["QT_FILHOS"] if moda else 0
    df = df.withColumn(
        "QT_FILHOS", F.when(F.col("QT_FILHOS") > MAX_QT_FILHOS, F.lit(moda_filhos)).otherwise(F.col("QT_FILHOS"))
    )
    logger.info("Silver: mediana ULTIMO_SALARIO=%s, moda QT_FILHOS=%s", median_salario, moda_filhos)

    return (
        df.dropDuplicates()
        .withColumn(
            "RENDA_TOTAL",
            F.coalesce(F.col("ULTIMO_SALARIO"), F.lit(0.0)) + F.coalesce(F.col("OUTRA_RENDA_VALOR"), F.lit(0.0)),
        )
        .withColumn("DATA_TRATAMENTO", F.current_timestamp())
    )


def run(paths: Paths) -> DataFrame:
    df = transform(read_layer(paths.bronze_path))
    write_layer(df, paths.silver_path)
    logger.info("Silver: -> %s", paths.silver_path)
    return df
