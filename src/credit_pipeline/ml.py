"""Modelo de regressão que estima o SCORE de crédito a partir do perfil do cliente, com `pyspark.ml`.

A análise exploratória que motivou as features está em
`notebooks/exploracao_e_modelo.ipynb`.
"""

from __future__ import annotations

import json
import logging

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from credit_pipeline.config import Paths
from credit_pipeline.io import read_layer

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
    indexed = [f"{c}_idx" for c in CATEGORICAL_FEATURES]
    encoded = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]
    return Pipeline(stages=[
        # dropLast (padrão) remove uma categoria por coluna e evita a colinearidade com o intercepto.
        StringIndexer(inputCols=CATEGORICAL_FEATURES, outputCols=indexed, stringOrderType="alphabetAsc"),
        OneHotEncoder(inputCols=indexed, outputCols=encoded),
        VectorAssembler(inputCols=NUMERIC_FEATURES, outputCol="numericas"),
        StandardScaler(inputCol="numericas", outputCol="numericas_padronizadas", withMean=True),
        VectorAssembler(inputCols=["numericas_padronizadas", *encoded], outputCol="features"),
        LinearRegression(featuresCol="features", labelCol=TARGET),
    ])


def prepare(df: DataFrame) -> DataFrame:
    return (
        df.select(*NUMERIC_FEATURES, *CATEGORICAL_FEATURES, TARGET)
        .dropna()
        .select(
            *[F.col(c).cast("double") for c in NUMERIC_FEATURES],
            *[F.col(c).cast("string") for c in CATEGORICAL_FEATURES],
            F.col(TARGET).cast("double"),
        )
    )


def evaluate(predictions: DataFrame, prediction_col: str = "prediction") -> dict[str, float]:
    return {
        metric: round(
            RegressionEvaluator(labelCol=TARGET, predictionCol=prediction_col, metricName=metric).evaluate(predictions),
            4,
        )
        for metric in ["r2", "mae", "rmse"]
    }


def train(df: DataFrame) -> tuple[PipelineModel, dict]:
    train_df, test_df = prepare(df).randomSplit([1 - TEST_SIZE, TEST_SIZE], seed=RANDOM_STATE)
    train_df, test_df = train_df.cache(), test_df.cache()

    model = build_model().fit(train_df)
    predictions = model.transform(test_df)

    # Baseline: sempre prevê a média do treino.
    media = train_df.agg(F.avg(TARGET)).first()[0]
    baseline = predictions.withColumn("baseline", F.lit(media))

    metrics = {
        "registros_treino": train_df.count(),
        "registros_teste": test_df.count(),
        "modelo": evaluate(predictions),
        "baseline_media": evaluate(baseline, "baseline"),
    }
    return model, metrics


def run(paths: Paths) -> dict:
    _, metrics = train(read_layer(paths.gold_dir / "dados_gold"))
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    (paths.reports_dir / "ml_metrics.json").write_text(json.dumps(metrics, indent=2))
    logger.info("ML: modelo %s | baseline %s", metrics["modelo"], metrics["baseline_media"])
    return metrics
