import shutil
from datetime import datetime
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from credit_pipeline.bronze import SCHEMA
from credit_pipeline.config import PROJECT_ROOT, Paths


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    # Delta habilitado para testar localmente o modo usado no Databricks (tabelas, MERGE, histórico).
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("credit-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def bronze_df(spark) -> DataFrame:
    """Amostra no formato da camada Bronze, já com os problemas que a Silver precisa tratar."""
    rows = [
        # cliente, UF, idade, escolaridade, estado civil, filhos, casa, imoveis, vl_imoveis, outra_renda,
        # vl_outra_renda, tempo_emprego, trabalhando, salario, carros, vl_carros, score
        (1, " sp ", 19, "Superior Cursando", "Solteiro", 0, "Não", 0, 0.0, "Não", 0.0, 8, "Sim", 1800.0, 0, 0.0, 12.0),
        (2, "MG", 40, "Superior Completo", "Casado", 7, "Sim", 1, 250000.0, "Sim", 1500.0, 60, "Não", None, 1, 50000.0,
         55.0),
        (3, "RJ", 60, "Segundo Grau Completo", "Viuvo", 1, "Sim", 2, 400000.0, "Não", 0.0, 120, "Sim", 22000.0, 2,
         180000.0, 98.0),
    ]
    schema = T.StructType([
        *SCHEMA.fields,
        T.StructField("DATA_UPLOAD", T.TimestampType()),
        T.StructField("ARQUIVO_FONTE", T.StringType()),
    ])
    lineage = (datetime(2025, 1, 1), "dados_credito.xlsx")
    return spark.createDataFrame([row + lineage for row in rows], schema=schema)


@pytest.fixture
def pipeline_paths(tmp_path: Path, spark) -> Paths:
    """Data lake temporário com o arquivo bruto real, para testes ponta a ponta."""
    paths = Paths(tmp_path / "data")
    paths.raw_file.parent.mkdir(parents=True)
    shutil.copy(PROJECT_ROOT / "data" / "raw" / "dados_credito.xlsx", paths.raw_file)
    return paths
