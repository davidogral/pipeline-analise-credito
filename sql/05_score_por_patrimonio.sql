-- Score médio por quantidade de imóveis e carros: o patrimônio explica o score?
SELECT
    qt_imoveis,
    qt_carros,
    COUNT(*) AS total_clientes,
    ROUND(CAST(AVG(score) AS NUMERIC), 2) AS score_medio
FROM clientes_credito
GROUP BY qt_imoveis, qt_carros
HAVING COUNT(*) >= 30
ORDER BY qt_imoveis, qt_carros;
