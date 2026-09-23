import pytest
from pyspark.sql import functions as F

from credit_pipeline.gold import (
    FAIXAS_ETARIAS,
    analise_clientes,
    ativos_por_faixa_etaria,
    build_features,
    categoria_renda,
    faixa_etaria,
    metricas_por_estado,
)
from credit_pipeline.silver import transform


@pytest.mark.parametrize(
    ("idade", "faixa"),
    [(20, "Até 20"), (21, "21 a 30"), (30, "21 a 30"), (45, "31 a 45"), (55, "46 a 55"), (56, "Maior que 55")],
)
def test_faixa_etaria_nos_limites(spark, idade, faixa):
    df = spark.createDataFrame([(idade,)], ["IDADE"])
    assert df.select(faixa_etaria(F.col("IDADE"))).first()[0] == faixa


@pytest.mark.parametrize(
    ("renda", "categoria"),
    [(2500.0, "Baixa"), (2500.01, "Média-Baixa"), (5000.0, "Média-Baixa"), (10000.0, "Média"),
     (20000.0, "Média-Alta"), (20000.01, "Alta")],
)
def test_categoria_renda_nos_limites(spark, renda, categoria):
    df = spark.createDataFrame([(renda,)], ["RENDA"])
    assert df.select(categoria_renda(F.col("RENDA"))).first()[0] == categoria


@pytest.fixture
def gold_df(bronze_df):
    return build_features(transform(bronze_df)).cache()


def test_capacidade_de_credito(gold_df):
    for row in analise_clientes(gold_df).collect():
        assert row.capacidade_credito == pytest.approx(row.RENDA_TOTAL * 0.3 + row.SCORE * 10)


def test_flags_viram_indicadores_numericos(gold_df):
    rows = {r.CODIGO_CLIENTE: r for r in gold_df.collect()}
    assert {r.TEM_FILHOS for r in rows.values()} <= {0, 1}
    assert [rows[i].TRABALHANDO_ATUALMENTE for i in (1, 2, 3)] == [1, 0, 1]


def test_metricas_por_estado_uma_linha_por_uf(gold_df):
    metricas = metricas_por_estado(gold_df).collect()
    assert len({r.UF for r in metricas}) == len(metricas)
    assert sum(r.total_clientes for r in metricas) == gold_df.count()


def test_ativos_ordenados_por_faixa_etaria(gold_df):
    faixas = [r.FAIXA_ETARIA for r in ativos_por_faixa_etaria(gold_df).collect()]
    assert faixas == sorted(faixas, key=FAIXAS_ETARIAS.index)
