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

  * `dados_gold.csv`: base completa com as features derivadas
  * `ativos_patrimonio.csv`: médias de patrimônio por faixa etária
  * `analise_clientes.csv`: visão por cliente com a capacidade de crédito
  * `metricas_estado.csv`: métricas agregadas por UF

## Banco de Dados

* Tipo: PostgreSQL
* Conexão: variáveis `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
  (defaults: `localhost:5432/pipeline/postgres/postgres`)
* Tabelas:

  * `projeto_final`: Dados completos limpos
  * `metricas_estado`: dados relacionados ao estado
  * `ativos_patrimonio`: dados sobre o patrimônio
  * `clientes` e `ativos`: tabelas de relacionamento criadas pelo script de carga

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
   * `05_sql_queries.ipynb.ipynb`
   * `06_quality_report.ipynb`
   * `07_monitoring.ipynb`
   * `Ml_score.ipynb`

2. Consulte o banco de dados:

```python
import os
import pandas as pd
import psycopg2

# Conectar ao banco PostgreSQL
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
```python
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
LIMIT 10;
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
- Apache Spark: processamento distribuído
- CSV: formato de armazenamento das camadas
- Camadas: Bronze/Silver/Gold

### Armazenamento
- Data Lake: pasta `data/` do próprio projeto
- Data Warehouse: PostgreSQL (via psycopg2)
- Formato: CSV

### Orquestração
- Apache Airflow para automação
- DAG: `pipeline_credito`, em `airflow_home/dags/pipeline_credito.py`
- Execução: Diária (`@daily`), encadeando `bronze >> silver >> gold >> load_db`
- Monitoramento via Airflow UI e pelo notebook `07_monitoring.ipynb`

## Estrutura de Dados

### Data Lake (local)
- Bronze: `data/bronze/dados_brutos.csv`
- Silver: `data/silver/dados_limpos.csv`
- Gold: `data/gold/dados_gold.csv`, `metricas_estado.csv`, `ativos_patrimonio.csv`, `analise_clientes.csv`

### Data Warehouse (PostgreSQL)
- Tabela: `projeto_final` (camada Silver completa)
- Tabela: `metricas_estado` (agregações por UF)
- Tabela: `ativos_patrimonio` (agregações por faixa etária)
- Tabelas: `clientes` e `ativos` (relacionamento)

## Como Executar

### 1. Processar as camadas
```bash
export JAVA_HOME=$( /usr/libexec/java_home -v 17 )   # PySpark requer Java 17
python scripts/01_bronze_layer.py
python scripts/02_silver_layer.py
python scripts/03_gold_layer.py
```

### 2. Carregar no Banco
```bash
python scripts/04_load_database.py
```

### 3. Executar Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow webserver -p 8080 &
airflow scheduler &
# Acessar: http://localhost:8080
```
