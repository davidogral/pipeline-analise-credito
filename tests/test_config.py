import pytest

from credit_pipeline.config import BRONZE, Paths, gold


def test_nomes_das_tabelas_no_unity_catalog(tmp_path):
    paths = Paths(tmp_path, storage="delta", catalog="workspace")
    assert paths.table(BRONZE) == "workspace.bronze.dados_brutos"
    assert paths.table(gold("metricas_estado")) == "workspace.gold.metricas_estado"


def test_pastas_parquet_no_modo_local(tmp_path):
    assert Paths(tmp_path).location(BRONZE) == tmp_path / "bronze" / "dados_brutos"


def test_storage_invalido(tmp_path):
    with pytest.raises(ValueError):
        Paths(tmp_path, storage="csv")


def test_argumentos_tem_prioridade_sobre_o_ambiente(monkeypatch, tmp_path):
    monkeypatch.setenv("PIPELINE_STORAGE", "parquet")
    monkeypatch.setenv("PIPELINE_CATALOG", "outro")
    paths = Paths.from_env(data_dir=str(tmp_path), storage="delta")
    assert (paths.storage, paths.catalog, paths.data_dir) == ("delta", "outro", tmp_path)
