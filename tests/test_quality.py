import pytest
from pyspark.sql import functions as F

from credit_pipeline import quality
from credit_pipeline.config import gold
from credit_pipeline.gold import build_features
from credit_pipeline.io import write_layer
from credit_pipeline.silver import transform


@pytest.fixture
def gold_df(bronze_df):
    return build_features(transform(bronze_df))


def failed_rules(df):
    results, _ = quality.run_checks(df)
    return {(r.rule, r.column) for r in results if not r.passed}


def test_dados_validos_passam_em_todas_as_regras(gold_df):
    assert failed_rules(gold_df) == set()


def test_detecta_chave_duplicada(gold_df):
    codigo = F.col("CODIGO_CLIENTE")
    df = gold_df.withColumn("CODIGO_CLIENTE", F.when(codigo == 2, 1).otherwise(codigo))
    assert ("chave_unica", "CODIGO_CLIENTE") in failed_rules(df)


def test_detecta_valor_fora_do_intervalo(gold_df):
    df = gold_df.withColumn("IDADE", F.when(F.col("CODIGO_CLIENTE") == 1, 150).otherwise(F.col("IDADE")))
    assert ("intervalo_18_120", "IDADE") in failed_rules(df)


def test_detecta_uf_invalida(gold_df):
    df = gold_df.withColumn("UF", F.when(F.col("CODIGO_CLIENTE") == 1, "XX").otherwise(F.col("UF")))
    assert ("dominio_uf", "UF") in failed_rules(df)


def test_quality_gate_bloqueia_carga(pipeline_paths, gold_df):
    df = gold_df.withColumn("SCORE", F.when(F.col("CODIGO_CLIENTE") == 1, None).otherwise(F.col("SCORE")))
    write_layer(df, pipeline_paths, gold("dados_gold"))
    with pytest.raises(quality.QualityGateError):
        quality.run(pipeline_paths)
    assert (pipeline_paths.reports_dir / "quality_report.json").exists()
