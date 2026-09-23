import pandas as pd

from credit_pipeline.silver import transform


def test_padroniza_texto_e_booleanos(bronze_df):
    df = transform(bronze_df)
    assert df.loc[0, "UF"] == "SP"
    assert df.loc[0, "ESCOLARIDADE"] == "SUPERIOR CURSANDO"
    assert df["CASA_PROPRIA"].tolist() == [False, True, True]
    assert df["TRABALHANDO_ATUALMENTE"].dtype == "boolean"


def test_preenche_salario_nulo_com_mediana(bronze_df):
    df = transform(bronze_df)
    assert df.loc[1, "ULTIMO_SALARIO"] == (1800.0 + 22000.0) / 2


def test_substitui_outlier_de_filhos_pela_moda(bronze_df):
    df = transform(bronze_df)
    assert df["QT_FILHOS"].max() <= 3
    assert df.loc[1, "QT_FILHOS"] != 7


def test_calcula_renda_total(bronze_df):
    df = transform(bronze_df)
    expected = df["ULTIMO_SALARIO"] + df["OUTRA_RENDA_VALOR"]
    pd.testing.assert_series_equal(df["RENDA_TOTAL"], expected, check_names=False)


def test_remove_duplicatas(bronze_df):
    duplicated = pd.concat([bronze_df, bronze_df.iloc[[0]]], ignore_index=True)
    assert len(transform(duplicated)) == len(bronze_df)


def test_nao_altera_o_dataframe_de_entrada(bronze_df):
    before = bronze_df.copy()
    transform(bronze_df)
    pd.testing.assert_frame_equal(bronze_df, before)
