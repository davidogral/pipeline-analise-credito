from pyspark.sql import functions as F

from credit_pipeline.silver import transform


def rows_by_id(df):
    return {row.CODIGO_CLIENTE: row for row in df.collect()}


def test_padroniza_texto_e_booleanos(bronze_df):
    rows = rows_by_id(transform(bronze_df))
    assert rows[1].UF == "SP"
    assert rows[1].ESCOLARIDADE == "SUPERIOR CURSANDO"
    assert [rows[i].CASA_PROPRIA for i in (1, 2, 3)] == [False, True, True]
    assert dict(transform(bronze_df).dtypes)["TRABALHANDO_ATUALMENTE"] == "boolean"


def test_preenche_salario_nulo_com_mediana(bronze_df):
    rows = rows_by_id(transform(bronze_df))
    assert rows[2].ULTIMO_SALARIO in (1800.0, 22000.0)  # mediana exata de dois valores é um deles no Spark


def test_substitui_outlier_de_filhos_pela_moda(bronze_df):
    df = transform(bronze_df)
    assert df.agg(F.max("QT_FILHOS")).first()[0] <= 3
    assert rows_by_id(df)[2].QT_FILHOS != 7


def test_calcula_renda_total(bronze_df):
    for row in transform(bronze_df).collect():
        assert row.RENDA_TOTAL == row.ULTIMO_SALARIO + row.OUTRA_RENDA_VALOR


def test_remove_duplicatas(bronze_df):
    assert transform(bronze_df.unionByName(bronze_df.limit(1))).count() == bronze_df.count()
