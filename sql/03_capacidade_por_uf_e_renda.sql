-- Capacidade de crédito por UF e categoria de renda, com ranking e participação dentro de cada UF.
WITH segmentos AS (
    SELECT
        c.uf,
        a.categoria_renda,
        COUNT(*) AS total_clientes,
        AVG(a.capacidade_credito) AS capacidade_media
    FROM analise_clientes AS a
    JOIN clientes_credito AS c ON c.codigo_cliente = a.codigo_cliente
    GROUP BY c.uf, a.categoria_renda
)
SELECT
    uf,
    RANK() OVER (PARTITION BY uf ORDER BY capacidade_media DESC) AS posicao,
    categoria_renda,
    total_clientes,
    ROUND(100.0 * total_clientes / SUM(total_clientes) OVER (PARTITION BY uf), 2) AS pct_clientes_uf,
    ROUND(CAST(capacidade_media AS DECIMAL(18, 4)), 2) AS capacidade_media
FROM segmentos
ORDER BY uf, posicao;
