"""Testes ponta a ponta com o arquivo bruto real."""

import os

import pytest

from credit_pipeline import analytics, load, ml
from credit_pipeline.cli import run_steps
from credit_pipeline.config import BRONZE, SILVER, Paths, PostgresSettings, gold

TRANSFORM_STEPS = ["bronze", "silver", "gold", "quality"]


@pytest.fixture
def processed_paths(pipeline_paths):
    run_steps(TRANSFORM_STEPS, pipeline_paths)
    return pipeline_paths


def test_pipeline_gera_todas_as_camadas(processed_paths):
    assert (processed_paths.location(BRONZE) / "_SUCCESS").exists()
    assert (processed_paths.location(SILVER) / "_SUCCESS").exists()
    for name in ["dados_gold", "metricas_estado", "analise_clientes", "ativos_patrimonio"]:
        assert (processed_paths.location(gold(name)) / "_SUCCESS").exists()
    assert (processed_paths.reports_dir / "quality_report.json").exists()
    assert (processed_paths.reports_dir / "quality_report.png").exists()


def test_consultas_sql_rodam_sobre_as_camadas(processed_paths):
    results = analytics.run(processed_paths)
    assert len(results) == len(analytics.sql_files())
    assert all(not df.empty for df in results.values())


def test_modelo_supera_baseline(processed_paths):
    metrics = ml.run(processed_paths)
    assert metrics["modelo"]["r2"] > metrics["baseline_media"]["r2"]
    assert metrics["modelo"]["rmse"] < metrics["baseline_media"]["rmse"]


def test_modo_delta_historico_merge_e_auditoria(pipeline_paths, spark):
    """Mesmo fluxo do Databricks, com Delta local: duas execuções seguidas do pipeline."""
    paths = Paths(pipeline_paths.data_dir, storage="delta", catalog="spark_catalog")
    run_steps(TRANSFORM_STEPS, paths)
    run_steps(TRANSFORM_STEPS, paths)

    bronze = spark.table(paths.table(BRONZE))
    silver = spark.table(paths.table(SILVER))
    # Bronze acumula as duas ingestões; Silver continua com um registro por cliente.
    assert bronze.select("DATA_UPLOAD").distinct().count() == 2
    assert bronze.count() == 2 * silver.count()
    assert silver.count() == silver.select("CODIGO_CLIENTE").distinct().count()

    operacoes = [r.operation for r in spark.sql(f"DESCRIBE HISTORY {paths.table(SILVER)}").collect()]
    assert "MERGE" in operacoes

    auditoria = spark.table(paths.table(gold("qualidade_execucoes")))
    assert auditoria.select("executado_em").distinct().count() == 2
    assert auditoria.filter("NOT aprovada AND severidade = 'error'").count() == 0


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("PGHOST"), reason="defina PGHOST para rodar contra um PostgreSQL")
def test_carga_no_postgres_e_consultas(processed_paths):
    loaded = load.run(processed_paths, PostgresSettings.from_env())
    assert loaded["clientes_credito"] == loaded["analise_clientes"] > 0

    results = analytics.run(processed_paths, engine="postgres")
    local = analytics.run(processed_paths, engine="spark")
    assert results["01_visao_geral_por_uf"]["total_clientes"].tolist() == \
        local["01_visao_geral_por_uf"]["total_clientes"].tolist()
