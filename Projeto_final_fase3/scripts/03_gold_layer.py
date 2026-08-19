import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

from pathlib import Path
import numpy as np
import pandas as pd
from pandas_utils import read_layer_csv, write_single_csv

silver_file = Path("data/silver/dados_limpos.csv")
df = read_layer_csv(silver_file)
print(f"Silver carregado ({len(df)} registros).")

idade = pd.to_numeric(df["IDADE"], errors="coerce")
faixa_condicoes = [
    idade <= 20,
    (idade > 20) & (idade <= 30),
    (idade > 30) & (idade <= 45),
    (idade > 45) & (idade <= 55),
]
faixa_valores = ["Até 20", "21 a 30", "31 a 45", "46 a 55"]
df["FAIXA_ETARIA"] = np.select(faixa_condicoes, faixa_valores, default="Maior que 55")
print(df["FAIXA_ETARIA"].value_counts().to_string())

df["TEM_FILHOS"] = np.where(pd.to_numeric(df["QT_FILHOS"], errors="coerce") > 0, "Sim", "Não")
print(df["TEM_FILHOS"].value_counts().to_string())

df["RENDA_TOTAL"] = df["ULTIMO_SALARIO"].fillna(0.0) + df["OUTRA_RENDA_VALOR"].fillna(0.0)
print(df["RENDA_TOTAL"].describe().to_string())

faixas_renda = (
    df.groupby(df["RENDA_TOTAL"].round(-2).rename("RENDA_TOTAL_FAIXA"))
      .size()
      .reset_index(name="count")
      .sort_values("RENDA_TOTAL_FAIXA")
)
print(faixas_renda.head(10).to_string(index=False))

renda_condicoes = [
    df["RENDA_TOTAL"] <= 2500,
    df["RENDA_TOTAL"] <= 5000,
    df["RENDA_TOTAL"] <= 10000,
    df["RENDA_TOTAL"] <= 20000,
]
renda_valores = ["Baixa", "Média-Baixa", "Média", "Média-Alta"]
df["CATEGORIA_RENDA"] = np.select(renda_condicoes, renda_valores, default="Alta")
print(df["CATEGORIA_RENDA"].value_counts().to_string())

bool_cols = ["TEM_FILHOS", "TRABALHANDO_ATUALMENTE", "CASA_PROPRIA"]
for col in bool_cols:
    normalized = df[col].astype("string").str.strip().str.upper()
    df[col] = normalized.isin(["SIM", "TRUE", "1"]).astype(int)
print("Colunas booleanas convertidas para indicadores numéricos.")

gold_path = "data/gold/dados_gold.csv"
write_single_csv(df, gold_path)
print(f"Dados gerais salvos: {gold_path} (shape={df.shape})")

metricas_estado = (
    df.groupby("UF", dropna=False)
      .agg(
          total_clientes=("CODIGO_CLIENTE", "count"),
          renda_media=("RENDA_TOTAL", "mean"),
          score_medio=("SCORE", "mean"),
          media_imoveis=("QT_IMOVEIS", "mean"),
          media_carros=("QT_CARROS", "mean"),
          percentual_com_filhos=("TEM_FILHOS", "mean"),
      )
      .reset_index()
      .sort_values("UF")
)
metricas_estado["percentual_com_filhos"] = metricas_estado["percentual_com_filhos"] * 100
write_single_csv(metricas_estado, "data/gold/metricas_estado.csv")
print(metricas_estado.head().to_string(index=False))

analise_clientes = df[[
    "CODIGO_CLIENTE", "IDADE", "FAIXA_ETARIA", "RENDA_TOTAL",
    "CATEGORIA_RENDA", "SCORE", "QT_IMOVEIS", "QT_CARROS",
    "ULTIMO_SALARIO", "TRABALHANDO_ATUALMENTE"
]].copy()
analise_clientes["capacidade_credito"] = (
    analise_clientes["RENDA_TOTAL"].fillna(0.0) * 0.3 + analise_clientes["SCORE"].fillna(0.0) * 10
)
write_single_csv(analise_clientes, "data/gold/analise_clientes.csv")

ativos = (
    df.groupby("FAIXA_ETARIA", dropna=False)
      .agg(
          media_qt_imoveis=("QT_IMOVEIS", "mean"),
          media_valor_imoveis=("VL_IMOVEIS", "mean"),
          media_qt_carros=("QT_CARROS", "mean"),
          media_valor_carros=("VALOR_TABELA_CARROS", "mean"),
          renda_media=("RENDA_TOTAL", "mean"),
          score_medio=("SCORE", "mean"),
      )
      .reset_index()
      .sort_values("FAIXA_ETARIA")
)
write_single_csv(ativos, "data/gold/ativos_patrimonio.csv")

print(df.head().to_string())

print("Agregações criadas e salvas na camada Gold")
