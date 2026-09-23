"""Camada Gold: features de negócio e agregações prontas para consumo analítico."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from credit_pipeline.config import Paths
from credit_pipeline.io import read_layer_csv, write_single_csv

logger = logging.getLogger(__name__)

FAIXAS_ETARIAS = ["Até 20", "21 a 30", "31 a 45", "46 a 55", "Maior que 55"]
CATEGORIAS_RENDA = ["Baixa", "Média-Baixa", "Média", "Média-Alta", "Alta"]

# Regra de negócio da capacidade de crédito: 30% da renda + 10 pontos por ponto de score.
PESO_RENDA_CAPACIDADE = 0.3
PESO_SCORE_CAPACIDADE = 10


def faixa_etaria(idade: pd.Series) -> np.ndarray:
    idade = pd.to_numeric(idade, errors="coerce")
    condicoes = [idade <= 20, idade <= 30, idade <= 45, idade <= 55]
    return np.select(condicoes, FAIXAS_ETARIAS[:-1], default=FAIXAS_ETARIAS[-1])


def categoria_renda(renda: pd.Series) -> np.ndarray:
    condicoes = [renda <= 2_500, renda <= 5_000, renda <= 10_000, renda <= 20_000]
    return np.select(condicoes, CATEGORIAS_RENDA[:-1], default=CATEGORIAS_RENDA[-1])


def _flag(series: pd.Series) -> pd.Series:
    """Converte SIM/TRUE/1 em 1 e qualquer outro valor em 0."""
    return series.astype("string").str.strip().str.upper().isin(["SIM", "TRUE", "1"]).astype(int)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["FAIXA_ETARIA"] = faixa_etaria(df["IDADE"])
    df["TEM_FILHOS"] = (pd.to_numeric(df["QT_FILHOS"], errors="coerce") > 0).astype(int)
    df["RENDA_TOTAL"] = df["ULTIMO_SALARIO"].fillna(0.0) + df["OUTRA_RENDA_VALOR"].fillna(0.0)
    df["CATEGORIA_RENDA"] = categoria_renda(df["RENDA_TOTAL"])
    for col in ["TRABALHANDO_ATUALMENTE", "CASA_PROPRIA"]:
        df[col] = _flag(df[col])
    return df


def metricas_por_estado(df: pd.DataFrame) -> pd.DataFrame:
    metricas = (
        df.groupby("UF", dropna=False)
        .agg(
            total_clientes=("CODIGO_CLIENTE", "count"),
            renda_media=("RENDA_TOTAL", "mean"),
            score_medio=("SCORE", "mean"),
            media_imoveis=("QT_IMOVEIS", "mean"),
            media_carros=("QT_CARROS", "mean"),
            percentual_com_filhos=("TEM_FILHOS", "mean"),
        )
        .reset_index()
        .sort_values("UF")
    )
    metricas["percentual_com_filhos"] *= 100
    return metricas


def analise_clientes(df: pd.DataFrame) -> pd.DataFrame:
    clientes = df[[
        "CODIGO_CLIENTE", "IDADE", "FAIXA_ETARIA", "RENDA_TOTAL", "CATEGORIA_RENDA", "SCORE",
        "QT_IMOVEIS", "QT_CARROS", "ULTIMO_SALARIO", "TRABALHANDO_ATUALMENTE",
    ]].copy()
    clientes["capacidade_credito"] = (
        clientes["RENDA_TOTAL"].fillna(0.0) * PESO_RENDA_CAPACIDADE
        + clientes["SCORE"].fillna(0.0) * PESO_SCORE_CAPACIDADE
    )
    return clientes


def ativos_por_faixa_etaria(df: pd.DataFrame) -> pd.DataFrame:
    ativos = (
        df.groupby("FAIXA_ETARIA", dropna=False)
        .agg(
            media_qt_imoveis=("QT_IMOVEIS", "mean"),
            media_valor_imoveis=("VL_IMOVEIS", "mean"),
            media_qt_carros=("QT_CARROS", "mean"),
            media_valor_carros=("VALOR_TABELA_CARROS", "mean"),
            renda_media=("RENDA_TOTAL", "mean"),
            score_medio=("SCORE", "mean"),
        )
        .reset_index()
    )
    ordem = pd.Categorical(ativos["FAIXA_ETARIA"], categories=FAIXAS_ETARIAS, ordered=True)
    return ativos.iloc[np.argsort(ordem.codes, kind="stable")].reset_index(drop=True)


def run(paths: Paths) -> dict[str, pd.DataFrame]:
    df = build_features(read_layer_csv(paths.silver_file))
    outputs = {
        "dados_gold": df,
        "metricas_estado": metricas_por_estado(df),
        "analise_clientes": analise_clientes(df),
        "ativos_patrimonio": ativos_por_faixa_etaria(df),
    }
    for name, table in outputs.items():
        write_single_csv(table, paths.gold_dir / f"{name}.csv")
        logger.info("Gold: %s com %d linhas", name, len(table))
    return outputs
