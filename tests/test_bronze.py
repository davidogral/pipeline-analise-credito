import pandas as pd
import pytest

from credit_pipeline.bronze import SCHEMA, cast_column, extract


def test_cast_column_trata_marcadores_de_vazio_como_nulo():
    result = cast_column(pd.Series(["10", " ", "NULL", "abc", 7.9]), "Int32")
    assert result.tolist()[0] == 10
    assert result.isna().tolist() == [False, True, True, True, False]
    assert result.iloc[4] == 7  # trunca, não arredonda


def test_cast_column_string_remove_espacos():
    assert cast_column(pd.Series(["  SP "]), "string").iloc[0] == "SP"


def test_extract_falha_sem_arquivo(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract(tmp_path / "inexistente.xlsx")


def test_extract_respeita_schema_e_adiciona_linhagem(pipeline_paths):
    df = extract(pipeline_paths.raw_file)
    assert list(df.columns) == [*SCHEMA, "DATA_UPLOAD", "ARQUIVO_FONTE"]
    assert (df["ARQUIVO_FONTE"] == "dados_credito.xlsx").all()
    assert len(df) > 0
