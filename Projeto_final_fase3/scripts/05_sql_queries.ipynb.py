import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

import sqlite3
import pandas as pd
from pandas_utils import read_layer_csv

df = read_layer_csv("data/silver/dados_limpos.csv")
conn = sqlite3.connect(":memory:")
df.to_sql("projeto_final", conn, index=False, if_exists="replace")
print(f"Tabela 'projeto_final' registrada com {len(df)} linhas.")

resultado = pd.read_sql_query("""
SELECT COUNT(*) as total_registros
FROM projeto_final
""", conn)
print(resultado.to_string(index=False))

query_top_rendas = pd.read_sql_query("""
SELECT CODIGO_CLIENTE, RENDA_TOTAL
FROM projeto_final
ORDER BY RENDA_TOTAL DESC
LIMIT 10
""", conn)
print(query_top_rendas.to_string(index=False))

score_faixas = pd.read_sql_query("""
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
""", conn)
print(score_faixas.to_string(index=False))

carros_renda = pd.read_sql_query("""
SELECT
    QT_CARROS,
    AVG(RENDA_TOTAL) AS renda_media,
    AVG(ULTIMO_SALARIO) AS salario_medio,
    AVG(SCORE) AS score_medio
FROM projeto_final
GROUP BY QT_CARROS
ORDER BY QT_CARROS
""", conn)
print(carros_renda.to_string(index=False))

trabalho_renda = pd.read_sql_query("""
SELECT
    TRABALHANDO_ATUALMENTE,
    COUNT(*) AS total_clientes,
    AVG(RENDA_TOTAL) AS renda_media,
    AVG(ULTIMO_SALARIO) AS salario_medio,
    AVG(SCORE) AS score_medio
FROM projeto_final
GROUP BY TRABALHANDO_ATUALMENTE
""", conn)
print(trabalho_renda.to_string(index=False))
conn.close()
