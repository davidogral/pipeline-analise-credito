-- Visão geral da carteira por UF: volume, renda, score e participação no total.
SELECT
    uf,
    COUNT(*) AS total_clientes,
    ROUND(CAST(AVG(renda_total) AS DECIMAL(18, 4)), 2) AS renda_media,
    ROUND(CAST(AVG(score) AS DECIMAL(18, 4)), 2) AS score_medio,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_carteira
FROM clientes_credito
GROUP BY uf
ORDER BY total_clientes DESC;
