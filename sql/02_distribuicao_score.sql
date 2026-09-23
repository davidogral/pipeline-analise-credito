-- Distribuição dos clientes por faixa de score.
SELECT
    CASE
        WHEN score < 25 THEN '1. Baixo (0-24)'
        WHEN score < 50 THEN '2. Regular (25-49)'
        WHEN score < 75 THEN '3. Bom (50-74)'
        ELSE '4. Excelente (75-100)'
    END AS faixa_score,
    COUNT(*) AS total_clientes,
    ROUND(CAST(AVG(renda_total) AS DECIMAL(18, 4)), 2) AS renda_media
FROM clientes_credito
GROUP BY 1
ORDER BY 1;
