# Pipeline de Dados – Análise de Crédito

## Descrição

O problema do negócio é a ineficiência e o risco associados ao processo de análise de crédito para novos solicitantes de cartão. Instituições financeiras enfrentam o desafio de avaliar um grande volume de solicitações de forma rápida e precisa. Processos manuais são lentos, caros, de difícil escalabilidade e suscetíveis a erros humanos o que aumenta a exposição da empresa a riscos financeiros.

## Estrutura de Dados

### Camada Bronze

* Localização: `data/bronze/`
* Descrição: Dados brutos, sem transformações
* Arquivo: `dados_brutos.csv`

### Camada Silver

* Localização: `data/silver/`
* Descrição: Dados limpos e validados
* Arquivo: `dados_limpos.csv`
* Transformações aplicadas:

  1. Alteração dos tipos de dados
  2. Padronização dos valores textuais
  3. Tratamento de valores nulos
  4. Tratamento de outliers
  5. Tratamento de duplicatas
  6. Criação da coluna Renda_Total
  7. Pós análise exploratória

### Camada Gold

* Localização: `data/gold/`
* Descrição: Dados agregados para análise.
* Arquivos:

  * `ativos_patrimonio.csv`
  * `analise_clientes.csv`
  * `metricas_estado.csv`

## Banco de Dados

* Tipo: SQLite
* Localização: `db/pipeline.db`
* Tabelas:

  * `projeto_final`: Dados completos limpos
  * `metricas_estado`: dados relacionados ao estado
  * `ativos_patrimonio`: dados sobre o patromonio

## Qualidade dos Dados

* Completude: 100%
* Unicidade: 100%
* Score Geral: 100%

## Como Executar

1. Execute os notebooks na ordem:

   * `01_bronze_layer.ipynb`
   * `02_silver_layer.ipynb`
   * `03_gold_layer.ipynb`
   * `04_load_database.ipynb`
   * `05_sql_queries.ipynb`
   * `06_quality_report.ipynb`

2. Consulte o banco de dados:

import os
import pandas as pd
import psycopg2

## Conectar ao banco PostgreSQL 
```bash
conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
    dbname=os.getenv("PGDATABASE", "pipeline"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
)

def run(sql):
    return pd.read_sql_query(sql, conn)
```
```bash
# QUERY 1: Visão Geral dos Dados
query = """
SELECT COUNT(*) as total_registros
FROM projeto_final
"""
resultado = pd.read_sql_query(query, conn)
print("Total de registros:", resultado['total_registros'].values[0])

# QUERY 2: Top 10 clientes com maior renda
query_top_rendas = """
SELECT CODIGO_CLIENTE, RENDA_TOTAL
FROM projeto_final
ORDER BY RENDA_TOTAL DESC
LIMIT 11;
"""
resultado = pd.read_sql_query(query_top_rendas, conn)
print(resultado)

# QUERY 3: Distribuição de score (quantos clientes por faixa)
query = """
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
"""
score_faixas = pd.read_sql_query(query, conn)
print("\nDistribuição por faixa de score:")
print(score_faixas)
```

# Pipeline de Dados - Fase 03

## Arquitetura

### Processamento
- pandas: processamento em memória
- Parquet: formato de armazenamento
- Camadas: Bronze/Silver/Gold

### Armazenamento
- Data Lake: AWS S3 / Azure Blob Storage
- Data Warehouse: PostgreSQL
- Formato: Parquet (mais eficiente que CSV)

### Orquestração
- Apache Airflow para automação
- Execução: Diária
- Monitoramento via Airflow UI

## Estrutura de Dados

### Data Lake (Cloud)
- Bronze: `s3://bucket/bronze/` ou `azure://container/bronze/`
- Silver: `s3://bucket/silver/` ou `azure://container/silver/`
- Gold: `s3://bucket/gold/` ou `azure://container/gold/`

### Data Warehouse (PostgreSQL)
- Tabela: `vendas` (fato principal)
- Tabela: `clientes` (dimensão)
- Tabela: `produtos` (dimensão)
- Tabela: `metricas_diarias` (agregações)

## Como Executar

### 1. Processar com pandas
```bash
# Executar notebooks na ordem
01_bronze_layer.ipynb
02_silver_layer.ipynb
03_gold_layer.ipynb
```

### 2. Carregar no Banco
```bash
04_create_database_schema.ipynb
05_load_to_postgres.ipynb
```

### 3. Executar Airflow
```bash
docker-compose up -d
# Acessar: http://localhost:808
```
