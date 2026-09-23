from pathlib import Path

import pandas as pd

TIMESTAMP_COLUMNS = ("DATA_UPLOAD", "DATA_TRATAMENTO")


def read_layer_csv(input_file: str | Path) -> pd.DataFrame:
    """Lê um CSV gerado por uma camada anterior, convertendo as colunas de timestamp do pipeline."""
    df = pd.read_csv(input_file)
    for column in TIMESTAMP_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def write_single_csv(df: pd.DataFrame, output_file: str | Path) -> Path:
    """Persiste o DataFrame em um único CSV, criando a pasta de destino se necessário."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path
