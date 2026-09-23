.PHONY: help install db-up db-down run run-local analytics ml test lint format airflow-up clean

PYTHON ?= python3

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Cria o .venv e instala o projeto com dependências de desenvolvimento
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev,notebook]"

db-up: ## Sobe o PostgreSQL local (Docker)
	docker compose up -d --wait postgres

db-down: ## Derruba os containers
	docker compose --profile airflow down

run: db-up ## Executa o pipeline completo, incluindo a carga no PostgreSQL
	.venv/bin/credit-pipeline run

run-local: ## Executa o pipeline sem banco (bronze -> silver -> gold -> quality)
	.venv/bin/credit-pipeline run --skip-load

analytics: ## Roda as consultas de sql/ no Spark SQL
	.venv/bin/credit-pipeline analytics

ml: ## Treina e avalia o modelo de score
	.venv/bin/credit-pipeline ml

test: ## Roda os testes unitários e ponta a ponta
	.venv/bin/pytest

lint: ## Verifica estilo e erros comuns
	.venv/bin/ruff check src tests dags

format: ## Formata o código
	.venv/bin/ruff check --fix src tests dags

airflow-up: ## Sobe Airflow (com Java) + PostgreSQL em http://localhost:8080 (admin/admin)
	docker compose --profile airflow up -d --build

clean: ## Remove as saídas geradas
	rm -rf data/bronze data/silver data/gold data/reports spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
