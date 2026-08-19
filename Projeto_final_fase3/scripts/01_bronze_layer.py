import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from pathlib import Path
import numpy as np
import pandas as pd
from pandas_utils import write_single_csv

raw_file = Path("data/raw/dados_credito.xlsx")
if not raw_file.exists():
    raise FileNotFoundError(f"Arquivo não encontrado: {raw_file}")

schema = {
    "CODIGO_CLIENTE": "Int64",
    "UF": "string",
    "IDADE": "Int32",
    "ESCOLARIDADE": "string",
    "ESTADO_CIVIL": "string",
    "QT_FILHOS": "Int32",
    "CASA_PROPRIA": "string",
    "QT_IMOVEIS": "Int32",
    "VL_IMOVEIS": "float64",
    "OUTRA_RENDA": "string",
    "OUTRA_RENDA_VALOR": "float64",
    "TEMPO_ULTIMO_EMPREGO_MESES": "Int32",
    "TRABALHANDO_ATUALMENTE": "string",
    "ULTIMO_SALARIO": "float64",
    "QT_CARROS": "Int32",
    "VALOR_TABELA_CARROS": "float64",
    "SCORE": "float64",
}

for folder in [Path("data/bronze"), Path("data/silver"), Path("data/gold")]:
    folder.mkdir(parents=True, exist_ok=True)
print("Estrutura de diretórios verificada.")

raw_df = pd.read_excel(raw_file, engine="openpyxl")
raw_df.columns = [str(column).strip() for column in raw_df.columns]
raw_df = raw_df.dropna(how="all")

blank_markers = ["", "None", "NULL"]

def cast_column(series: pd.Series, dtype: str) -> pd.Series:
    cleaned = series.map(lambda value: value.strip() if isinstance(value, str) else value)
    cleaned = cleaned.replace(blank_markers, np.nan)
    if dtype.startswith("Int"):
        numeric = pd.to_numeric(cleaned, errors="coerce")
        return np.trunc(numeric).astype("Float64").astype(dtype)
    if dtype == "float64":
        return pd.to_numeric(cleaned, errors="coerce").astype("float64")
    return cleaned.astype("string")

df = pd.DataFrame({
    column: cast_column(raw_df.get(column, pd.Series(index=raw_df.index, dtype="object")), dtype)
    for column, dtype in schema.items()
})
print(f"Dimensão inicial do dataset: {df.shape}")
print(df.dtypes.to_string())

print(df.describe(include="all").transpose().to_string())
null_overview = df.isna().sum().to_frame("nulos").transpose()
print(null_overview.to_string())

df["DATA_UPLOAD"] = pd.Timestamp.utcnow().tz_localize(None)
df["ARQUIVO_FONTE"] = raw_file.name

bronze_path = "data/bronze/dados_brutos.csv"
write_single_csv(df, bronze_path)
print(f">>> Bronze salvo em: {bronze_path}")
print(df.head().to_string())
