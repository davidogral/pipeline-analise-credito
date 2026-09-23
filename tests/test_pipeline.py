"""Testes ponta a ponta com o arquivo bruto real."""

import os

import pytest

from credit_pipeline import analytics, load, ml
from credit_pipeline.cli import run_steps
from credit_pipeline.config import PostgresSettings

TRANSFORM_STEPS = ["bronze", "silver", "gold", "quality"]


@pytest.fixture
def processed_paths(pipeline_paths):
    run_steps(TRANSFORM_STEPS, pipeline_paths)
    return pipeline_paths


def test_pipeline_gera_todas_as_camadas(processed_paths):
    assert (processed_paths.bronze_path / "_SUCCESS").exists()
    assert (processed_paths.silver_path / "_SUCCESS").exists()
    for name in ["dados_gold", "metricas_estado", "analise_clientes", "ativos_patrimonio"]:
        assert (processed_paths.gold_dir / name / "_SUCCESS").exists()
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


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("PGHOST"), reason="defina PGHOST para rodar contra um PostgreSQL")
def test_carga_no_postgres_e_consultas(processed_paths):
    loaded = load.run(processed_paths, PostgresSettings.from_env())
    assert loaded["clientes_credito"] == loaded["analise_clientes"] > 0

    results = analytics.run(processed_paths, engine="postgres")
    local = analytics.run(processed_paths, engine="spark")
    assert results["01_visao_geral_por_uf"]["total_clientes"].tolist() == \
        local["01_visao_geral_por_uf"]["total_clientes"].tolist()
