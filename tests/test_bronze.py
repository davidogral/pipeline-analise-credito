import pytest
from pyspark.sql import types as T

from credit_pipeline.bronze import SCHEMA, cast_value, extract


@pytest.mark.parametrize(
    ("value", "data_type", "expected"),
    [
        ("10", T.IntegerType(), 10),
        (7.9, T.IntegerType(), 7),  # trunca, não arredonda
        (" ", T.IntegerType(), None),
        ("NULL", T.DoubleType(), None),
        ("abc", T.DoubleType(), None),
        ("  SP ", T.StringType(), "SP"),
    ],
)
def test_cast_value(value, data_type, expected):
    assert cast_value(value, data_type) == expected


def test_extract_falha_sem_arquivo(tmp_path, spark):
    with pytest.raises(FileNotFoundError):
        extract(tmp_path / "inexistente.xlsx")


def test_extract_respeita_schema_e_adiciona_linhagem(pipeline_paths):
    df = extract(pipeline_paths.raw_file)
    assert df.columns == [*SCHEMA.fieldNames(), "DATA_UPLOAD", "ARQUIVO_FONTE"]
    assert df.schema["IDADE"].dataType == T.IntegerType()
    assert {r.ARQUIVO_FONTE for r in df.select("ARQUIVO_FONTE").distinct().collect()} == {"dados_credito.xlsx"}
    assert df.count() > 0
