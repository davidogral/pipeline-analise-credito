from pathlib import Path
from pyspark.sql import functions as F
from spark_utils import get_spark, write_single_csv

spark = get_spark("GoldLayer")

silver_file = Path("data/silver/dados_limpos.csv")
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(silver_file))
    .cache()
)
print(f"Silver carregado ({df.count()} registros).")

idade_expr = (
    F.when(F.col("IDADE") <= 20, "Até 20")
     .when((F.col("IDADE") > 20) & (F.col("IDADE") <= 30), "21 a 30")
     .when((F.col("IDADE") > 30) & (F.col("IDADE") <= 45), "31 a 45")
     .when((F.col("IDADE") > 45) & (F.col("IDADE") <= 55), "46 a 55")
     .otherwise("Maior que 55")
)
df = df.withColumn("FAIXA_ETARIA", idade_expr)
df.groupBy("FAIXA_ETARIA").count().show()

df = df.withColumn(
    "TEM_FILHOS",
    F.when(F.col("QT_FILHOS") > 0, F.lit("Sim")).otherwise(F.lit("Não"))
)
df.groupBy("TEM_FILHOS").count().show()

df = df.withColumn(
    "RENDA_TOTAL",
    F.coalesce(F.col("ULTIMO_SALARIO"), F.lit(0.0)) + F.coalesce(F.col("OUTRA_RENDA_VALOR"), F.lit(0.0))
)
df.select("RENDA_TOTAL").summary().show()

df.groupBy(F.round(F.col("RENDA_TOTAL"), -2).alias("RENDA_TOTAL_FAIXA")).count().orderBy(F.col("RENDA_TOTAL_FAIXA")).show(10)

df = df.withColumn(
    "CATEGORIA_RENDA",
    F.when(F.col("RENDA_TOTAL") <= 2500, "Baixa")
     .when(F.col("RENDA_TOTAL") <= 5000, "Média-Baixa")
     .when(F.col("RENDA_TOTAL") <= 10000, "Média")
     .when(F.col("RENDA_TOTAL") <= 20000, "Média-Alta")
     .otherwise("Alta")
)
df.groupBy("CATEGORIA_RENDA").count().show()

bool_cols = ["TEM_FILHOS", "TRABALHANDO_ATUALMENTE", "CASA_PROPRIA"]
for col in bool_cols:
    normalized = F.upper(F.trim(F.col(col).cast("string")))
    df = df.withColumn(
        col,
        F.when(normalized.isin("SIM", "TRUE", "1"), F.lit(1)).otherwise(F.lit(0))
    )
print("Colunas booleanas convertidas para indicadores numéricos.")

gold_path = "data/gold/dados_gold.csv"
write_single_csv(df, gold_path)
print(f"Dados gerais salvos: {gold_path} (shape=({df.count()}, {len(df.columns)}))")

metricas_estado = (
    df.groupBy("UF")
      .agg(
          F.count("CODIGO_CLIENTE").alias("total_clientes"),
          F.avg("RENDA_TOTAL").alias("renda_media"),
          F.avg("SCORE").alias("score_medio"),
          F.avg("QT_IMOVEIS").alias("media_imoveis"),
          F.avg("QT_CARROS").alias("media_carros"),
          F.avg("TEM_FILHOS").alias("percentual_com_filhos")
      )
      .withColumn("percentual_com_filhos", F.col("percentual_com_filhos") * 100)
      .orderBy("UF")
)
write_single_csv(metricas_estado, "data/gold/metricas_estado.csv")
metricas_estado.show(5, truncate=False)

analise_clientes = (
    df.select(
        "CODIGO_CLIENTE", "IDADE", "FAIXA_ETARIA", "RENDA_TOTAL",
        "CATEGORIA_RENDA", "SCORE", "QT_IMOVEIS", "QT_CARROS",
        "ULTIMO_SALARIO", "TRABALHANDO_ATUALMENTE"
    )
    .withColumn(
        "capacidade_credito",
        F.coalesce(F.col("RENDA_TOTAL"), F.lit(0.0)) * F.lit(0.3) + F.coalesce(F.col("SCORE"), F.lit(0.0)) * F.lit(10)
    )
)
write_single_csv(analise_clientes, "data/gold/analise_clientes.csv")

ativos = (
    df.groupBy("FAIXA_ETARIA")
      .agg(
          F.avg("QT_IMOVEIS").alias("media_qt_imoveis"),
          F.avg("VL_IMOVEIS").alias("media_valor_imoveis"),
          F.avg("QT_CARROS").alias("media_qt_carros"),
          F.avg("VALOR_TABELA_CARROS").alias("media_valor_carros"),
          F.avg("RENDA_TOTAL").alias("renda_media"),
          F.avg("SCORE").alias("score_medio")
      )
      .orderBy("FAIXA_ETARIA")
)
write_single_csv(ativos, "data/gold/ativos_patrimonio.csv")

df.show(5, truncate=False)

print("Agregações criadas e salvas na camada Gold")
