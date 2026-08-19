from pathlib import Path
import pandas as pd


TIMESTAMP_COLUMNS = ("DATA_UPLOAD", "DATA_TRATAMENTO")


def read_layer_csv(input_file) -> pd.DataFrame:
    """Read a CSV written by a previous layer, parsing the pipeline timestamp columns."""
    df = pd.read_csv(input_file)
    for column in TIMESTAMP_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def write_single_csv(df: pd.DataFrame, output_file: str, header: bool = True) -> None:
    """Persist a DataFrame as a single CSV file, creating the parent folder when needed."""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, header=header)
