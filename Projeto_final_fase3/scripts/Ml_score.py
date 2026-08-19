import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler, OrdinalEncoder, StandardScaler
from pandas_utils import read_layer_csv

sns.set(style="whitegrid")


def collect_values(df, column, limit=5000):
    sample = df[column].dropna()
    if limit:
        sample = sample.head(limit)
    return sample.tolist()


def collect_pairs(df, col_x, col_y, limit=5000):
    sample = df[[col_x, col_y]].dropna()
    if limit:
        sample = sample.head(limit)
    return sample[col_x].tolist(), sample[col_y].tolist()

df = read_layer_csv("data/gold/dados_gold.csv")
df = df.drop(columns=["CODIGO_CLIENTE", "DATA_UPLOAD", "ARQUIVO_FONTE", "DATA_TRATAMENTO"], errors="ignore")

bool_columns = df.select_dtypes(include=["bool", "boolean"]).columns.tolist()
if bool_columns:
    for col in bool_columns:
        df[col] = df[col].astype("string")
    print(f"Colunas booleanas convertidas para string: {bool_columns}")

print(f"Registros disponíveis: {len(df)}")

print(df.dtypes.to_string())

print(df.describe(include="all").transpose().to_string())

null_counts = df.isna().sum().to_frame("nulos").transpose()
print(null_counts.to_string())

numeric_cols = df.select_dtypes(include="number").columns.tolist()
print("Colunas numéricas:", numeric_cols)

categorical_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
print("Colunas categóricas:", categorical_cols)

print("Lista detalhada de colunas numéricas:")
for col in numeric_cols:
    print(" -", col)

print("Lista detalhada de colunas categóricas:")
for col in categorical_cols:
    print(" -", col)

fig_rows = math.ceil(len(numeric_cols) / 4)
fig, axes = plt.subplots(fig_rows, 4, figsize=(16, 4 * fig_rows), constrained_layout=True)
for ax, col in zip(axes.flat, numeric_cols):
    values = collect_values(df, col)
    if values:
        sns.boxplot(y=values, ax=ax)
    ax.set_title(col)
for ax in axes.flat[len(numeric_cols):]:
    ax.set_visible(False)
plt.show()

if categorical_cols:
    fig_rows = math.ceil(len(categorical_cols) / 3) or 1
    fig, axes = plt.subplots(fig_rows, 3, figsize=(18, 4 * fig_rows), constrained_layout=True)
    for ax, col in zip(axes.flat, categorical_cols):
        counts = df[col].value_counts(dropna=False)
        labels = [str(index) if pd.notna(index) else "NULO" for index in counts.index]
        values = counts.tolist()
        sns.barplot(x=labels, y=values, ax=ax)
        ax.set_title(col)
        ax.tick_params(axis='x', rotation=45)
    for ax in axes.flat[len(categorical_cols):]:
        ax.set_visible(False)
    plt.show()
else:
    print("Não há colunas categóricas para visualizar.")

fig_rows = math.ceil(len(categorical_cols) / 3) or 1
fig, axes = plt.subplots(fig_rows, 3, figsize=(18, 4 * fig_rows), constrained_layout=True)
for ax, col in zip(axes.flat, categorical_cols):
    counts = df[col].value_counts(dropna=False)
    labels = [str(index) if pd.notna(index) else "NULO" for index in counts.index]
    values = counts.tolist()
    sns.barplot(x=labels, y=values, ax=ax)
    ax.set_title(col)
    ax.tick_params(axis='x', rotation=45)
for ax in axes.flat[len(categorical_cols):]:
    ax.set_visible(False)
plt.show()

plt.rcParams["figure.figsize"] = (18, 8)

corr_matrix = df[numeric_cols].corr().fillna(0.0)
plt.figure(figsize=(14, 10))
sns.heatmap(corr_matrix, annot=False, cmap="coolwarm", xticklabels=numeric_cols, yticklabels=numeric_cols)
plt.title("Matriz de Correlação (variáveis numéricas)")
plt.show()

x_vals, y_vals = collect_pairs(df, "VL_IMOVEIS", "SCORE")
plt.figure(figsize=(8, 5))
sns.regplot(x=x_vals, y=y_vals, scatter_kws={"s": 10})
plt.title("Relação VL_IMOVEIS x SCORE")
plt.show()

x_vals, y_vals = collect_pairs(df, "ULTIMO_SALARIO", "SCORE")
plt.figure(figsize=(8, 5))
sns.regplot(x=x_vals, y=y_vals, scatter_kws={"s": 10})
plt.title("Relação ULTIMO_SALARIO x SCORE")
plt.show()

x_vals, y_vals = collect_pairs(df, "TEMPO_ULTIMO_EMPREGO_MESES", "SCORE")
plt.figure(figsize=(8, 5))
sns.regplot(x=x_vals, y=y_vals, scatter_kws={"s": 10})
plt.title("Relação TEMPO_ULTIMO_EMPREGO_MESES x SCORE")
plt.show()

target_col = "SCORE"

numeric_features = [
    "IDADE", "QT_FILHOS", "QT_IMOVEIS", "VL_IMOVEIS", "OUTRA_RENDA_VALOR",
    "TEMPO_ULTIMO_EMPREGO_MESES", "ULTIMO_SALARIO", "QT_CARROS",
    "VALOR_TABELA_CARROS", "RENDA_TOTAL"
]

categorical_features = [
    "UF", "ESCOLARIDADE", "ESTADO_CIVIL", "OUTRA_RENDA",
    "FAIXA_ETARIA", "CATEGORIA_RENDA", "TRABALHANDO_ATUALMENTE",
    "CASA_PROPRIA", "TEM_FILHOS"
]

for col in numeric_features + [target_col]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

for col in categorical_features:
    if col in df.columns:
        df[col] = df[col].astype("string")

model_columns = [c for c in numeric_features + categorical_features + [target_col] if c in df.columns]
df_model = df[model_columns].dropna().reset_index(drop=True)
print(f"Registros utilizados para o modelo: {len(df_model)} (após dropna)")

available_numeric = [col for col in numeric_features if col in df_model.columns]
available_categorical = [col for col in categorical_features if col in df_model.columns]

# OrdinalEncoder cumpre o papel do StringIndexer do Spark: uma coluna numérica por categoria.
preprocessor = ColumnTransformer(
    transformers=[
        ("numericas", "passthrough", available_numeric),
        (
            "categoricas",
            OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
            available_categorical,
        ),
    ]
)
minmax_scaler = MinMaxScaler()
standard_scaler = StandardScaler(with_mean=True, with_std=True)
regressor = LinearRegression()

pipeline = Pipeline(steps=[
    ("preprocessador", preprocessor),
    ("minmax", minmax_scaler),
    ("standard", standard_scaler),
    ("regressor", regressor),
])

X = df_model[available_numeric + available_categorical]
y = df_model[target_col]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=398)
print(f"Registros treino: {len(X_train)} | Registros teste: {len(X_test)}")

pipeline.fit(X_train, y_train)
print("Modelo Linear Regression treinado com scikit-learn.")

predictions = pipeline.predict(X_test)
r2 = r2_score(y_test, predictions)
print(f"R² no conjunto de teste: {r2:.4f}")
