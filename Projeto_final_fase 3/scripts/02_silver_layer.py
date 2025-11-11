from pathlib import Path
from pyspark.sql import functions as F
from spark_utils import get_spark, write_single_csv

spark = get_spark("SilverLayer")

bronze_file = Path("data/bronze/dados_brutos.csv")
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(bronze_file))
    .cache()
)
print(f"Dados carregados da camada Bronze ({df.count()} registros).")

df = df.withColumn("CODIGO_CLIENTE", F.col("CODIGO_CLIENTE").cast("string"))

categorical_cols = ["UF", "ESCOLARIDADE", "ESTADO_CIVIL"]
for col in categorical_cols:
    df = df.withColumn(col, F.upper(F.trim(F.col(col))))

bool_cols = ["CASA_PROPRIA", "OUTRA_RENDA", "TRABALHANDO_ATUALMENTE"]
for col in bool_cols:
    cleaned = F.upper(F.trim(F.col(col)))
    df = df.withColumn(
        col,
        F.when(cleaned == F.lit("SIM"), F.lit(True))
         .when(cleaned.isin("NAO", "NÃO"), F.lit(False))
         .otherwise(None)
    )

int_cols = ["IDADE", "QT_FILHOS", "QT_IMOVEIS", "TEMPO_ULTIMO_EMPREGO_MESES", "QT_CARROS", "SCORE"]
for col in int_cols:
    df = df.withColumn(col, F.col(col).cast("int"))

float_cols = ["VL_IMOVEIS", "OUTRA_RENDA_VALOR", "ULTIMO_SALARIO", "VALOR_TABELA_CARROS"]
for col in float_cols:
    df = df.withColumn(col, F.col(col).cast("double"))

string_cols = [name for name, dtype in df.dtypes if dtype == "string"]
for col in string_cols:
    df = df.withColumn(col, F.upper(F.trim(F.col(col))))
print(f"Colunas textuais padronizadas: {string_cols}")

null_counts = df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns])
null_counts.show(truncate=False)

df = df.replace('SEM DADOS', None)
median_result = df.approxQuantile("ULTIMO_SALARIO", [0.5], 0.01)
median_sal = median_result[0] if median_result else None
if median_sal is not None:
    df = df.withColumn(
        "ULTIMO_SALARIO",
        F.when(F.col("ULTIMO_SALARIO").isNull(), F.lit(median_sal)).otherwise(F.col("ULTIMO_SALARIO"))
    )
print(f"Mediana utilizada para ULTIMO_SALARIO: {median_sal}")

qt_mode_row = (
    df.groupBy("QT_FILHOS")
      .count()
      .orderBy(F.desc("count"))
      .first()
)
qt_moda = qt_mode_row["QT_FILHOS"] if qt_mode_row else 0
df = df.withColumn(
    "QT_FILHOS",
    F.when(F.col("QT_FILHOS") > 3, F.lit(qt_moda)).otherwise(F.col("QT_FILHOS"))
)
print(f"Moda aplicada para QT_FILHOS: {qt_moda}")

total_registros = df.count()
df_distinct = df.dropDuplicates()
duplicados = total_registros - df_distinct.count()
print(f"Duplicatas removidas: {duplicados}")
df = df_distinct
df.printSchema()

df = df.withColumn(
    "RENDA_TOTAL",
    F.coalesce(F.col("ULTIMO_SALARIO"), F.lit(0.0)) + F.coalesce(F.col("OUTRA_RENDA_VALOR"), F.lit(0.0))
)
df.select("RENDA_TOTAL").show(5)

df = df.withColumn("DATA_TRATAMENTO", F.current_timestamp())

silver_path = "data/silver/dados_limpos.csv"
write_single_csv(df, silver_path)
print(f"Dados limpos salvos: {silver_path} (registros={df.count()})")
df.show(5, truncate=False)
