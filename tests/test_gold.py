import pandas as pd
import pytest

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
def test_faixa_etaria_nos_limites(idade, faixa):
    assert faixa_etaria(pd.Series([idade]))[0] == faixa


@pytest.mark.parametrize(
    ("renda", "categoria"),
    [(2500, "Baixa"), (2500.01, "Média-Baixa"), (5000, "Média-Baixa"), (10000, "Média"), (20000, "Média-Alta"),
     (20000.01, "Alta")],
)
def test_categoria_renda_nos_limites(renda, categoria):
    assert categoria_renda(pd.Series([renda]))[0] == categoria


@pytest.fixture
def gold_df(bronze_df):
    return build_features(transform(bronze_df))


def test_capacidade_de_credito(gold_df):
    clientes = analise_clientes(gold_df)
    expected = gold_df["RENDA_TOTAL"] * 0.3 + gold_df["SCORE"] * 10
    assert clientes["capacidade_credito"].tolist() == pytest.approx(expected.tolist())


def test_flags_viram_indicadores_numericos(gold_df):
    assert set(gold_df["TEM_FILHOS"]) <= {0, 1}
    assert gold_df["TRABALHANDO_ATUALMENTE"].tolist() == [1, 0, 1]


def test_metricas_por_estado_uma_linha_por_uf(gold_df):
    metricas = metricas_por_estado(gold_df)
    assert metricas["UF"].is_unique
    assert metricas["total_clientes"].sum() == len(gold_df)


def test_ativos_ordenados_por_faixa_etaria(gold_df):
    faixas = ativos_por_faixa_etaria(gold_df)["FAIXA_ETARIA"].tolist()
    assert faixas == sorted(faixas, key=FAIXAS_ETARIAS.index)
