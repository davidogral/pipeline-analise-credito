from pathlib import Path

from pyspark.sql import DataFrame

from credit_pipeline.spark import get_spark


def read_layer(path: str | Path) -> DataFrame:
    """Lê uma camada persistida em Parquet (o schema vem junto com os dados)."""
    return get_spark().read.parquet(str(path))


def write_layer(df: DataFrame, path: str | Path) -> Path:
    """Persiste a camada em Parquet, sobrescrevendo a execução anterior."""
    path = Path(path)
    df.write.mode("overwrite").parquet(str(path))
    return path
