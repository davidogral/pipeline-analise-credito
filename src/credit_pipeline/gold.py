"""Camada Gold: features de negócio e agregações prontas para consumo analítico."""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from credit_pipeline.config import Paths
from credit_pipeline.io import read_layer, write_layer

logger = logging.getLogger(__name__)

FAIXAS_ETARIAS = ["Até 20", "21 a 30", "31 a 45", "46 a 55", "Maior que 55"]
CATEGORIAS_RENDA = ["Baixa", "Média-Baixa", "Média", "Média-Alta", "Alta"]

# Regra de negócio da capacidade de crédito: 30% da renda + 10 pontos por ponto de score.
PESO_RENDA_CAPACIDADE = 0.3
PESO_SCORE_CAPACIDADE = 10


def faixa_etaria(idade: Column) -> Column:
    return (
        F.when(idade <= 20, FAIXAS_ETARIAS[0])
        .when(idade <= 30, FAIXAS_ETARIAS[1])
        .when(idade <= 45, FAIXAS_ETARIAS[2])
        .when(idade <= 55, FAIXAS_ETARIAS[3])
        .otherwise(FAIXAS_ETARIAS[4])
    )


def categoria_renda(renda: Column) -> Column:
    return (
        F.when(renda <= 2_500, CATEGORIAS_RENDA[0])
        .when(renda <= 5_000, CATEGORIAS_RENDA[1])
        .when(renda <= 10_000, CATEGORIAS_RENDA[2])
        .when(renda <= 20_000, CATEGORIAS_RENDA[3])
        .otherwise(CATEGORIAS_RENDA[4])
    )


def _flag(col: str) -> Column:
    """Converte SIM/TRUE/1 em 1 e qualquer outro valor em 0."""
    normalized = F.upper(F.trim(F.col(col).cast("string")))
    return F.when(normalized.isin("SIM", "TRUE", "1"), 1).otherwise(0)


def build_features(df: DataFrame) -> DataFrame:
    renda_total = F.coalesce(F.col("ULTIMO_SALARIO"), F.lit(0.0)) + F.coalesce(F.col("OUTRA_RENDA_VALOR"), F.lit(0.0))
    return (
        df.withColumn("FAIXA_ETARIA", faixa_etaria(F.col("IDADE")))
        .withColumn("TEM_FILHOS", F.when(F.col("QT_FILHOS") > 0, 1).otherwise(0))
        .withColumn("RENDA_TOTAL", renda_total)
        .withColumn("CATEGORIA_RENDA", categoria_renda(F.col("RENDA_TOTAL")))
        .withColumn("TRABALHANDO_ATUALMENTE", _flag("TRABALHANDO_ATUALMENTE"))
        .withColumn("CASA_PROPRIA", _flag("CASA_PROPRIA"))
    )


def metricas_por_estado(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("UF")
        .agg(
            F.count("CODIGO_CLIENTE").alias("total_clientes"),
            F.avg("RENDA_TOTAL").alias("renda_media"),
            F.avg("SCORE").alias("score_medio"),
            F.avg("QT_IMOVEIS").alias("media_imoveis"),
            F.avg("QT_CARROS").alias("media_carros"),
            (F.avg("TEM_FILHOS") * 100).alias("percentual_com_filhos"),
        )
        .orderBy("UF")
    )


def analise_clientes(df: DataFrame) -> DataFrame:
    return df.select(
        "CODIGO_CLIENTE", "IDADE", "FAIXA_ETARIA", "RENDA_TOTAL", "CATEGORIA_RENDA", "SCORE",
        "QT_IMOVEIS", "QT_CARROS", "ULTIMO_SALARIO", "TRABALHANDO_ATUALMENTE",
    ).withColumn(
        "capacidade_credito",
        F.coalesce(F.col("RENDA_TOTAL"), F.lit(0.0)) * PESO_RENDA_CAPACIDADE
        + F.coalesce(F.col("SCORE"), F.lit(0)) * PESO_SCORE_CAPACIDADE,
    )


def ativos_por_faixa_etaria(df: DataFrame) -> DataFrame:
    ordem = F.create_map(*[F.lit(v) for i, f in enumerate(FAIXAS_ETARIAS) for v in (f, i)])[F.col("FAIXA_ETARIA")]
    return (
        df.groupBy("FAIXA_ETARIA")
        .agg(
            F.avg("QT_IMOVEIS").alias("media_qt_imoveis"),
            F.avg("VL_IMOVEIS").alias("media_valor_imoveis"),
            F.avg("QT_CARROS").alias("media_qt_carros"),
            F.avg("VALOR_TABELA_CARROS").alias("media_valor_carros"),
            F.avg("RENDA_TOTAL").alias("renda_media"),
            F.avg("SCORE").alias("score_medio"),
        )
        .orderBy(ordem)
    )


def run(paths: Paths) -> dict[str, DataFrame]:
    # cache: a base de features é lida por três agregações diferentes.
    df = build_features(read_layer(paths.silver_path)).cache()
    outputs = {
        "dados_gold": df,
        "metricas_estado": metricas_por_estado(df),
        "analise_clientes": analise_clientes(df),
        "ativos_patrimonio": ativos_por_faixa_etaria(df),
    }
    for name, table in outputs.items():
        # As agregações são pequenas: uma partição evita dezenas de arquivos minúsculos.
        write_layer(table if name in ("dados_gold", "analise_clientes") else table.coalesce(1), paths.gold_dir / name)
        logger.info("Gold: %s -> %s", name, paths.gold_dir / name)
    return outputs
