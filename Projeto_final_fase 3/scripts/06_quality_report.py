import matplotlib.pyplot as plt
from pyspark.sql import functions as F
from spark_utils import get_spark

spark = get_spark("QualityReport")
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/gold/dados_gold.csv")
    .cache()
)
record_count = df.count()
column_count = len(df.columns)
print("=" * 50)
print("RELATÓRIO DE QUALIDADE DE DADOS")
print("=" * 50)
print(f"Total de registros: {record_count} | Total de colunas: {column_count}")

print("1. COMPLETUDE DOS DADOS")
print("-" * 50)
filled_counts_row = df.select([F.count(F.col(c)).alias(c) for c in df.columns]).collect()[0]
filled_counts = filled_counts_row.asDict()
completude_por_coluna = {}
for coluna, preenchidos in filled_counts.items():
    percentual = (preenchidos / record_count * 100) if record_count else 0
    completude_por_coluna[coluna] = percentual
    print(f" {coluna}: {percentual:.2f}%")

total_celulas = record_count * column_count
celulas_preenchidas = sum(filled_counts.values())
completude_geral = (celulas_preenchidas / total_celulas * 100) if total_celulas else 0
print(f"Completude Geral: {completude_geral:.2f}%")

print("2. UNICIDADE DOS DADOS")
print("-" * 50)
distinct_count = df.dropDuplicates().count()
duplicatas = record_count - distinct_count
unicidade = ((record_count - duplicatas) / record_count * 100) if record_count else 0
print(f"Linhas únicas: {unicidade:.2f}%")
print(f"Duplicatas encontradas: {duplicatas}")

print("3. CONSISTÊNCIA DOS DADOS")
print("-" * 50)
numeric_cols = [
    "CODIGO_CLIENTE", "IDADE", "QT_FILHOS", "QT_IMOVEIS", "VL_IMOVEIS",
    "TEMPO_ULTIMO_EMPREGO_MESES", "ULTIMO_SALARIO", "QT_CARROS",
    "RENDA_TOTAL", "VALOR_TABELA_CARROS", "SCORE"
]
for coluna in numeric_cols:
    if coluna in df.columns:
        negativos = df.filter(F.col(coluna) < 0).count()
        print(f"{coluna}: {negativos} valores negativos encontrados")

print("4. VALIDADE DOS DADOS")
print("-" * 50)
validacoes = {
    'CODIGO_CLIENTE': (1, 999999999),
    'IDADE': (0, 120),
    'QT_FILHOS': (0, 20),
    'QT_IMOVEIS': (0, 50),
    'VL_IMOVEIS': (0, 1_000_000_000),
    'TEMPO_ULTIMO_EMPREGO_MESES': (0, 600),
    'ULTIMO_SALARIO': (0, 1_000_000),
    'QT_CARROS': (0, 20),
    'RENDA_TOTAL': (0, 2_000_000),
    'VALOR_TABELA_CARROS': (0, 2_000_000),
    'SCORE': (0, 1000)
}
for coluna, (min_val, max_val) in validacoes.items():
    if coluna in df.columns:
        fora_range = df.filter((F.col(coluna) < min_val) | (F.col(coluna) > max_val)).count()
        print(f"{coluna}: {fora_range} valores fora do intervalo válido ({min_val} – {max_val})")
    else:
        print(f"{coluna}: coluna não encontrada no DataFrame.")

print("5. VISUALIZAÇÃO")
print("-" * 50)
ordered_cols = sorted(completude_por_coluna.items(), key=lambda item: item[1])
labels = [item[0] for item in ordered_cols]
values = [item[1] for item in ordered_cols]
plt.figure(figsize=(10, 6))
plt.barh(labels, values)
plt.xlabel('Completude (%)')
plt.title('Completude dos Dados por Coluna')
plt.tight_layout()
plt.savefig('quality_report.png')
print("Gráfico de qualidade salvo em: quality_report.png")

print("" + "=" * 50)
print("SCORE GERAL DE QUALIDADE")
print("=" * 50)
score_final = (completude_geral + unicidade) / 2
print(f"Score Final: {score_final:.2f}%")
if score_final >= 90:
    print("Classificação: EXCELENTE")
elif score_final >= 80:
    print("Classificação: BOM")
elif score_final >= 70:
    print("Classificação: REGULAR")
else:
    print("Classificação: NECESSITA MELHORIAS")
