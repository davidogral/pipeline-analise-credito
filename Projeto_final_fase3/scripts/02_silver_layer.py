import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from pathlib import Path
import numpy as np
import pandas as pd
from pandas_utils import read_layer_csv, write_single_csv

bronze_file = Path("data/bronze/dados_brutos.csv")
df = read_layer_csv(bronze_file)
print(f"Dados carregados da camada Bronze ({len(df)} registros).")

df["CODIGO_CLIENTE"] = df["CODIGO_CLIENTE"].astype("string")

categorical_cols = ["UF", "ESCOLARIDADE", "ESTADO_CIVIL"]
for col in categorical_cols:
    df[col] = df[col].astype("string").str.strip().str.upper()

bool_cols = ["CASA_PROPRIA", "OUTRA_RENDA", "TRABALHANDO_ATUALMENTE"]
for col in bool_cols:
    cleaned = df[col].astype("string").str.strip().str.upper()
    df[col] = cleaned.map({"SIM": True, "NAO": False, "NÃO": False}).astype("boolean")

int_cols = ["IDADE", "QT_FILHOS", "QT_IMOVEIS", "TEMPO_ULTIMO_EMPREGO_MESES", "QT_CARROS", "SCORE"]
for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64").astype("Int32")

float_cols = ["VL_IMOVEIS", "OUTRA_RENDA_VALOR", "ULTIMO_SALARIO", "VALOR_TABELA_CARROS"]
for col in float_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

string_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
for col in string_cols:
    df[col] = df[col].astype("string").str.strip().str.upper()
print(f"Colunas textuais padronizadas: {string_cols}")

null_counts = df.isna().sum().to_frame("nulos").transpose()
print(null_counts.to_string())

df = df.replace("SEM DADOS", np.nan)
median_sal = df["ULTIMO_SALARIO"].median()
if pd.notna(median_sal):
    df["ULTIMO_SALARIO"] = df["ULTIMO_SALARIO"].fillna(median_sal)
print(f"Mediana utilizada para ULTIMO_SALARIO: {median_sal}")

qt_contagem = df["QT_FILHOS"].value_counts()
qt_moda = qt_contagem.index[0] if not qt_contagem.empty else 0
df.loc[df["QT_FILHOS"] > 3, "QT_FILHOS"] = qt_moda
print(f"Moda aplicada para QT_FILHOS: {qt_moda}")

total_registros = len(df)
df = df.drop_duplicates().reset_index(drop=True)
duplicados = total_registros - len(df)
print(f"Duplicatas removidas: {duplicados}")
print(df.dtypes.to_string())

df["RENDA_TOTAL"] = df["ULTIMO_SALARIO"].fillna(0.0) + df["OUTRA_RENDA_VALOR"].fillna(0.0)
print(df[["RENDA_TOTAL"]].head().to_string())

df["DATA_TRATAMENTO"] = pd.Timestamp.utcnow().tz_localize(None)

silver_path = "data/silver/dados_limpos.csv"
write_single_csv(df, silver_path)
print(f"Dados limpos salvos: {silver_path} (registros={len(df)})")
print(df.head().to_string())
