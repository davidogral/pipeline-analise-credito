from spark_utils import get_spark

spark = get_spark("SparkSQLQueries")
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/silver/dados_limpos.csv")
)
df.createOrReplaceTempView("projeto_final")
print(f"View 'projeto_final' registrada com {df.count()} linhas.")

resultado = spark.sql("""
SELECT COUNT(*) as total_registros
FROM projeto_final
""")
resultado.show()

query_top_rendas = spark.sql("""
SELECT CODIGO_CLIENTE, RENDA_TOTAL
FROM projeto_final
ORDER BY RENDA_TOTAL DESC
LIMIT 10
""")
query_top_rendas.show()

score_faixas = spark.sql("""
SELECT
    CASE
        WHEN SCORE < 25 THEN 'Baixo (0-24)'
        WHEN SCORE BETWEEN 25 AND 49 THEN 'Regular (25-49)'
        WHEN SCORE BETWEEN 50 AND 74 THEN 'Bom (50-74)'
        ELSE 'Excelente (75-100)'
    END AS faixa_score,
    COUNT(*) AS total_clientes
FROM projeto_final
GROUP BY faixa_score
ORDER BY total_clientes DESC
""")
score_faixas.show()

carros_renda = spark.sql("""
SELECT
    QT_CARROS,
    AVG(RENDA_TOTAL) AS renda_media,
    AVG(ULTIMO_SALARIO) AS salario_medio,
    AVG(SCORE) AS score_medio
FROM projeto_final
GROUP BY QT_CARROS
ORDER BY QT_CARROS
""")
carros_renda.show()

trabalho_renda = spark.sql("""
SELECT
    TRABALHANDO_ATUALMENTE,
    COUNT(*) AS total_clientes,
    AVG(RENDA_TOTAL) AS renda_media,
    AVG(ULTIMO_SALARIO) AS salario_medio,
    AVG(SCORE) AS score_medio
FROM projeto_final
GROUP BY TRABALHANDO_ATUALMENTE
""")
trabalho_renda.show()
