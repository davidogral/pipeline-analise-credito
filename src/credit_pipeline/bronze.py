"""Camada Bronze: ingestão do arquivo bruto com schema explícito e metadados de linhagem."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from credit_pipeline.config import Paths
from credit_pipeline.io import write_single_csv

logger = logging.getLogger(__name__)

# Contrato de entrada: qualquer coluna ausente na fonte vira uma coluna nula do tipo esperado.
SCHEMA: dict[str, str] = {
    "CODIGO_CLIENTE": "Int64",
    "UF": "string",
    "IDADE": "Int32",
    "ESCOLARIDADE": "string",
    "ESTADO_CIVIL": "string",
    "QT_FILHOS": "Int32",
    "CASA_PROPRIA": "string",
    "QT_IMOVEIS": "Int32",
    "VL_IMOVEIS": "float64",
    "OUTRA_RENDA": "string",
    "OUTRA_RENDA_VALOR": "float64",
    "TEMPO_ULTIMO_EMPREGO_MESES": "Int32",
    "TRABALHANDO_ATUALMENTE": "string",
    "ULTIMO_SALARIO": "float64",
    "QT_CARROS": "Int32",
    "VALOR_TABELA_CARROS": "float64",
    "SCORE": "float64",
}

BLANK_MARKERS = ["", "None", "NULL"]


def cast_column(series: pd.Series, dtype: str) -> pd.Series:
    """Aplica o tipo do schema, tratando marcadores de vazio e valores não numéricos como nulos."""
    cleaned = series.map(lambda value: value.strip() if isinstance(value, str) else value)
    cleaned = cleaned.replace(BLANK_MARKERS, np.nan)
    if dtype.startswith("Int"):
        numeric = pd.to_numeric(cleaned, errors="coerce")
        return np.trunc(numeric).astype("Float64").astype(dtype)
    if dtype == "float64":
        return pd.to_numeric(cleaned, errors="coerce").astype("float64")
    return cleaned.astype("string")


def extract(raw_file: Path) -> pd.DataFrame:
    """Lê o Excel de origem e devolve um DataFrame aderente ao SCHEMA, com colunas de linhagem."""
    if not raw_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {raw_file}")

    raw_df = pd.read_excel(raw_file, engine="openpyxl")
    raw_df.columns = [str(column).strip() for column in raw_df.columns]
    raw_df = raw_df.dropna(how="all")

    df = pd.DataFrame({
        column: cast_column(raw_df.get(column, pd.Series(index=raw_df.index, dtype="object")), dtype)
        for column, dtype in SCHEMA.items()
    })
    df["DATA_UPLOAD"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    df["ARQUIVO_FONTE"] = raw_file.name
    return df


def run(paths: Paths) -> pd.DataFrame:
    df = extract(paths.raw_file)
    write_single_csv(df, paths.bronze_file)
    logger.info("Bronze: %d registros, %d colunas -> %s", len(df), df.shape[1], paths.bronze_file)
    return df
