# Dicionário de dados

## Fonte: `data/raw/dados_credito.xlsx`

Uma linha por cliente solicitante de crédito. Base didática com 10.476 registros.

| Coluna | Tipo (Silver) | Descrição | Tratamento na Silver |
| --- | --- | --- | --- |
| `CODIGO_CLIENTE` | inteiro (BIGINT) | Identificador do cliente (chave primária) | — |
| `UF` | texto | Estado de residência | caixa alta, sem espaços |
| `IDADE` | inteiro | Idade em anos | — |
| `ESCOLARIDADE` | texto | Nível de escolaridade | caixa alta |
| `ESTADO_CIVIL` | texto | Estado civil | caixa alta |
| `QT_FILHOS` | inteiro | Quantidade de filhos | valores acima de 3 substituídos pela moda |
| `CASA_PROPRIA` | booleano | Possui casa própria | "Sim"/"Não" → `true`/`false` |
| `QT_IMOVEIS` | inteiro | Quantidade de imóveis | — |
| `VL_IMOVEIS` | decimal | Valor total dos imóveis (R$) | — |
| `OUTRA_RENDA` | booleano | Possui outra fonte de renda | "Sim"/"Não" → `true`/`false` |
| `OUTRA_RENDA_VALOR` | decimal | Valor da outra renda (R$) | — |
| `TEMPO_ULTIMO_EMPREGO_MESES` | inteiro | Tempo no último emprego (meses) | — |
| `TRABALHANDO_ATUALMENTE` | booleano | Está empregado atualmente | "Sim"/"Não" → `true`/`false` |
| `ULTIMO_SALARIO` | decimal | Último salário (R$) | nulos preenchidos com a mediana |
| `QT_CARROS` | inteiro | Quantidade de carros | — |
| `VALOR_TABELA_CARROS` | decimal | Valor de tabela dos carros (R$) | — |
| `SCORE` | inteiro | Score de crédito (0 a 100) | — |

## Colunas adicionadas pelo pipeline

| Coluna | Camada | Regra |
| --- | --- | --- |
| `DATA_UPLOAD` | Bronze | Timestamp UTC da ingestão (linhagem) |
| `ARQUIVO_FONTE` | Bronze | Nome do arquivo de origem (linhagem) |
| `RENDA_TOTAL` | Silver | `ULTIMO_SALARIO + OUTRA_RENDA_VALOR` |
| `DATA_TRATAMENTO` | Silver | Timestamp UTC do tratamento |
| `FAIXA_ETARIA` | Gold | Até 20 · 21 a 30 · 31 a 45 · 46 a 55 · Maior que 55 |
| `CATEGORIA_RENDA` | Gold | Baixa (≤ 2.500) · Média-Baixa (≤ 5.000) · Média (≤ 10.000) · Média-Alta (≤ 20.000) · Alta |
| `TEM_FILHOS` | Gold | 1 se `QT_FILHOS > 0`, senão 0 |
| `capacidade_credito` | Gold | `RENDA_TOTAL × 0,3 + SCORE × 10` |

## Tabelas no PostgreSQL

Todas as colunas são publicadas em minúsculas. A carga recria as tabelas em uma única transação.

| Tabela | Origem | Grão | Chaves |
| --- | --- | --- | --- |
| `clientes_credito` | Silver | 1 linha por cliente | PK `codigo_cliente` |
| `analise_clientes` | Gold | 1 linha por cliente | PK `codigo_cliente`, FK → `clientes_credito` |
| `metricas_estado` | Gold | 1 linha por UF | PK `uf` |
| `ativos_patrimonio` | Gold | 1 linha por faixa etária | PK `faixa_etaria` |

## Regras de qualidade (quality gate)

Executadas sobre a Gold antes da carga. Regras `error` bloqueiam a publicação no banco.

| Regra | Colunas | Severidade |
| --- | --- | --- |
| Dataset não vazio | — | error |
| Não nulo | `CODIGO_CLIENTE`, `UF`, `IDADE`, `ULTIMO_SALARIO`, `RENDA_TOTAL`, `SCORE` | error |
| Chave única | `CODIGO_CLIENTE` | error |
| Intervalo válido | idade 18–120, score 0–100, valores monetários não negativos etc. | error |
| Domínio de UF | `UF` entre as 27 UFs brasileiras | error |
| Outra renda com valor | quem declara `OUTRA_RENDA` deve ter `OUTRA_RENDA_VALOR > 0` | warning |

O resultado de cada execução é salvo em `data/reports/quality_report.json`.
