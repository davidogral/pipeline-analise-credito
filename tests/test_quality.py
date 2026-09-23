import pytest

from credit_pipeline import quality
from credit_pipeline.gold import build_features
from credit_pipeline.silver import transform


@pytest.fixture
def gold_df(bronze_df):
    return build_features(transform(bronze_df))


def failed_rules(df):
    return {(r.rule, r.column) for r in quality.run_checks(df) if not r.passed}


def test_dados_validos_passam_em_todas_as_regras(gold_df):
    assert failed_rules(gold_df) == set()


def test_detecta_chave_duplicada(gold_df):
    gold_df.loc[1, "CODIGO_CLIENTE"] = gold_df.loc[0, "CODIGO_CLIENTE"]
    assert ("chave_unica", "CODIGO_CLIENTE") in failed_rules(gold_df)


def test_detecta_valor_fora_do_intervalo(gold_df):
    gold_df.loc[0, "IDADE"] = 150
    assert ("intervalo_18_120", "IDADE") in failed_rules(gold_df)


def test_detecta_uf_invalida(gold_df):
    gold_df["UF"] = gold_df["UF"].astype(object)
    gold_df.loc[0, "UF"] = "XX"
    assert ("dominio_uf", "UF") in failed_rules(gold_df)


def test_quality_gate_bloqueia_carga(pipeline_paths, gold_df):
    from credit_pipeline.io import write_single_csv

    gold_df.loc[0, "SCORE"] = None
    write_single_csv(gold_df, pipeline_paths.gold_dir / "dados_gold.csv")
    with pytest.raises(quality.QualityGateError):
        quality.run(pipeline_paths)
    assert (pipeline_paths.reports_dir / "quality_report.json").exists()
