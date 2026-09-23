-- Renda e score de quem está ou não trabalhando atualmente.
SELECT
    CASE WHEN trabalhando_atualmente THEN 'Trabalhando' ELSE 'Sem emprego atual' END AS situacao,
    COUNT(*) AS total_clientes,
    ROUND(CAST(AVG(ultimo_salario) AS DECIMAL(18, 4)), 2) AS salario_medio,
    ROUND(CAST(AVG(renda_total) AS DECIMAL(18, 4)), 2) AS renda_media,
    ROUND(CAST(AVG(score) AS DECIMAL(18, 4)), 2) AS score_medio
FROM clientes_credito
GROUP BY 1
ORDER BY 1;
