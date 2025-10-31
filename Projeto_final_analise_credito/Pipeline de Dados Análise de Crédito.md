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

  1. Remoção de duplicatas
  2. Tratamento de valores nulos
  3. Conversão de tipos
  4. Padronização de valores
  5. Remoção de outliers

### Camada Gold

* Localização: `data/gold/`
* Descrição: Dados agregados para análise
* Arquivos:

  * `metricas_diarias.csv`
  * `analise_clientes.csv`
  * `desempenho_produtos.csv`

## Banco de Dados

* Tipo: SQLite
* Localização: `data/pipeline.db`
* Tabelas:

  * `tabela_principal`: Dados completos limpos
  * `clientes`: Informações de clientes
  * `produtos`: Catálogo de produtos
  * `metricas_diarias`: Agregações diárias

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

```python
import sqlite3
conn = sqlite3.connect('data/pipeline.db')
# suas queries aqui
```


