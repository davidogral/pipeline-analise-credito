"""Camada Silver: limpeza, tipagem, padronização e tratamento de nulos, outliers e duplicatas."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from credit_pipeline.config import Paths
from credit_pipeline.io import read_layer_csv, write_single_csv

logger = logging.getLogger(__name__)

CATEGORICAL_COLUMNS = ["UF", "ESCOLARIDADE", "ESTADO_CIVIL"]
BOOLEAN_COLUMNS = ["CASA_PROPRIA", "OUTRA_RENDA", "TRABALHANDO_ATUALMENTE"]
INTEGER_COLUMNS = ["IDADE", "QT_FILHOS", "QT_IMOVEIS", "TEMPO_ULTIMO_EMPREGO_MESES", "QT_CARROS", "SCORE"]
FLOAT_COLUMNS = ["VL_IMOVEIS", "OUTRA_RENDA_VALOR", "ULTIMO_SALARIO", "VALOR_TABELA_CARROS"]

BOOLEAN_MAP = {"SIM": True, "NAO": False, "NÃO": False}

# Regra de negócio: acima de 3 filhos é tratado como erro de digitação e substituído pela moda.
MAX_QT_FILHOS = 3


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica todas as regras da camada Silver. Não altera o DataFrame recebido."""
    df = df.copy()
    df["CODIGO_CLIENTE"] = df["CODIGO_CLIENTE"].astype("string")

    for col in BOOLEAN_COLUMNS:
        cleaned = df[col].astype("string").str.strip().str.upper()
        df[col] = cleaned.map(BOOLEAN_MAP).astype("boolean")

    for col in INTEGER_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64").astype("Int32")

    for col in FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Padroniza todo texto (inclui as categóricas) em caixa alta e sem espaços nas bordas.
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype("string").str.strip().str.upper()
    df = df.replace("SEM DADOS", np.nan)

    median_salario = df["ULTIMO_SALARIO"].median()
    if pd.notna(median_salario):
        df["ULTIMO_SALARIO"] = df["ULTIMO_SALARIO"].fillna(median_salario)

    contagem_filhos = df["QT_FILHOS"].value_counts()
    moda_filhos = contagem_filhos.index[0] if not contagem_filhos.empty else 0
    df.loc[df["QT_FILHOS"] > MAX_QT_FILHOS, "QT_FILHOS"] = moda_filhos

    total = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    logger.info(
        "Silver: mediana ULTIMO_SALARIO=%s, moda QT_FILHOS=%s, %d duplicatas removidas",
        median_salario, moda_filhos, total - len(df),
    )

    df["RENDA_TOTAL"] = df["ULTIMO_SALARIO"].fillna(0.0) + df["OUTRA_RENDA_VALOR"].fillna(0.0)
    df["DATA_TRATAMENTO"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    return df


def run(paths: Paths) -> pd.DataFrame:
    df = transform(read_layer_csv(paths.bronze_file))
    write_single_csv(df, paths.silver_file)
    logger.info("Silver: %d registros -> %s", len(df), paths.silver_file)
    return df
