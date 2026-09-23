from pyspark.sql import SparkSession


def get_spark(app_name: str = "credit-pipeline") -> SparkSession:
    """Cria (ou reaproveita) a SparkSession com a configuração padrão do projeto."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
