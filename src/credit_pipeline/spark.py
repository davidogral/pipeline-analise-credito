import os

from pyspark.sql import DataFrame, SparkSession


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "credit-pipeline") -> SparkSession:
    """Devolve a SparkSession: a do Databricks quando roda lá, ou uma local configurada."""
    if on_databricks():
        # No Databricks a sessão já existe; o compute serverless não aceita alterar configs estáticas.
        return SparkSession.builder.getOrCreate()

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cache(df: DataFrame) -> DataFrame:
    """Cacheia o DataFrame onde isso é suportado; o compute serverless do Databricks não permite cache."""
    if on_databricks():
        return df
    return df.cache()
