import shutil
from pathlib import Path

import pandas as pd
import pytest

from credit_pipeline.config import PROJECT_ROOT, Paths


@pytest.fixture
def bronze_df() -> pd.DataFrame:
    """Amostra no formato da camada Bronze, já com os problemas que a Silver precisa tratar."""
    rows = [
        # cliente, UF, idade, escolaridade, estado civil, filhos, casa, imoveis, vl_imoveis, outra_renda,
        # vl_outra_renda, tempo_emprego, trabalhando, salario, carros, vl_carros, score
        (1, " sp ", 19, "Superior Cursando", "Solteiro", 0, "Não", 0, 0.0, "Não", 0.0, 8, "Sim", 1800.0, 0, 0.0, 12.0),
        (2, "MG", 40, "Superior Completo", "Casado", 7, "Sim", 1, 250000.0, "Sim", 1500.0, 60, "Não", None, 1, 50000.0,
         55.0),
        (3, "RJ", 60, "Segundo Grau Completo", "Viuvo", 1, "Sim", 2, 400000.0, "Não", 0.0, 120, "Sim", 22000.0, 2,
         180000.0, 98.0),
    ]
    columns = [
        "CODIGO_CLIENTE", "UF", "IDADE", "ESCOLARIDADE", "ESTADO_CIVIL", "QT_FILHOS", "CASA_PROPRIA", "QT_IMOVEIS",
        "VL_IMOVEIS", "OUTRA_RENDA", "OUTRA_RENDA_VALOR", "TEMPO_ULTIMO_EMPREGO_MESES", "TRABALHANDO_ATUALMENTE",
        "ULTIMO_SALARIO", "QT_CARROS", "VALOR_TABELA_CARROS", "SCORE",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["DATA_UPLOAD"] = pd.Timestamp("2025-01-01")
    df["ARQUIVO_FONTE"] = "dados_credito.xlsx"
    return df


@pytest.fixture
def pipeline_paths(tmp_path: Path) -> Paths:
    """Data lake temporário com o arquivo bruto real, para testes ponta a ponta."""
    paths = Paths(tmp_path / "data")
    paths.raw_file.parent.mkdir(parents=True)
    shutil.copy(PROJECT_ROOT / "data" / "raw" / "dados_credito.xlsx", paths.raw_file)
    return paths
