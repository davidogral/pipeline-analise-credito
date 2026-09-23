"""Modelo de regressão que estima o SCORE de crédito a partir do perfil do cliente.

A análise exploratória que motivou as features está em
`notebooks/exploracao_e_modelo.ipynb`.
"""

from __future__ import annotations

import json
import logging

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from credit_pipeline.config import Paths
from credit_pipeline.io import read_layer_csv

logger = logging.getLogger(__name__)

TARGET = "SCORE"
# RENDA_TOTAL (= ULTIMO_SALARIO + OUTRA_RENDA_VALOR) e OUTRA_RENDA (= OUTRA_RENDA_VALOR > 0) ficam de fora:
# são combinações de outras features e tornam os coeficientes da regressão instáveis.
NUMERIC_FEATURES = [
    "IDADE", "QT_FILHOS", "QT_IMOVEIS", "VL_IMOVEIS", "OUTRA_RENDA_VALOR",
    "TEMPO_ULTIMO_EMPREGO_MESES", "ULTIMO_SALARIO", "QT_CARROS", "VALOR_TABELA_CARROS",
]
CATEGORICAL_FEATURES = [
    "UF", "ESCOLARIDADE", "ESTADO_CIVIL", "FAIXA_ETARIA",
    "CATEGORIA_RENDA", "TRABALHANDO_ATUALMENTE", "CASA_PROPRIA", "TEM_FILHOS",
]
TEST_SIZE = 0.3
RANDOM_STATE = 398


def build_model() -> Pipeline:
    preprocessor = ColumnTransformer([
        ("numericas", StandardScaler(), NUMERIC_FEATURES),
        ("categoricas", OneHotEncoder(handle_unknown="ignore", drop="first"), CATEGORICAL_FEATURES),
    ])
    return Pipeline([("preprocessador", preprocessor), ("regressor", LinearRegression())])


def prepare(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    df = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES + [TARGET]].dropna()
    X = df[NUMERIC_FEATURES + CATEGORICAL_FEATURES].copy()
    X[CATEGORICAL_FEATURES] = X[CATEGORICAL_FEATURES].astype(str)
    return X, df[TARGET].astype(float)


def evaluate(y_true, y_pred) -> dict[str, float]:
    return {
        "r2": round(float(r2_score(y_true, y_pred)), 4),
        "mae": round(float(mean_absolute_error(y_true, y_pred)), 4),
        "rmse": round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 4),
    }


def train(df: pd.DataFrame) -> tuple[Pipeline, dict]:
    X, y = prepare(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE)

    model = build_model().fit(X_train, y_train)
    baseline = DummyRegressor(strategy="mean").fit(X_train, y_train)

    metrics = {
        "registros_treino": len(X_train),
        "registros_teste": len(X_test),
        "modelo": evaluate(y_test, model.predict(X_test)),
        "baseline_media": evaluate(y_test, baseline.predict(X_test)),
    }
    return model, metrics


def run(paths: Paths) -> dict:
    _, metrics = train(read_layer_csv(paths.gold_dir / "dados_gold.csv"))
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    (paths.reports_dir / "ml_metrics.json").write_text(json.dumps(metrics, indent=2))
    logger.info("ML: modelo %s | baseline %s", metrics["modelo"], metrics["baseline_media"])
    return metrics
