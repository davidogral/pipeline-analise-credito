"""Configuração central do pipeline: onde ficam as camadas e como conectar no banco.

Há dois modos de armazenamento das camadas:

- ``parquet`` (padrão): data lake local em ``data/``, usado no desenvolvimento, nos
  testes e no Airflow;
- ``delta``: tabelas Delta no Unity Catalog (``<catalog>.bronze.dados_brutos`` etc.),
  usado no Databricks.

Tudo pode vir de variáveis de ambiente ou dos argumentos da CLI.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

STORAGES = ("parquet", "delta")


@dataclass(frozen=True)
class Layer:
    """Uma tabela de uma camada do medalhão, ex.: Layer("silver", "dados_limpos")."""

    schema: str
    name: str


BRONZE = Layer("bronze", "dados_brutos")
SILVER = Layer("silver", "dados_limpos")


def gold(name: str) -> Layer:
    return Layer("gold", name)


@dataclass(frozen=True)
class Paths:
    """Resolve onde cada camada é lida e gravada, conforme o modo de armazenamento."""

    data_dir: Path
    storage: str = "parquet"
    catalog: str = "workspace"

    def __post_init__(self):
        if self.storage not in STORAGES:
            raise ValueError(f"storage deve ser um de {STORAGES}, recebido: {self.storage!r}")

    def location(self, layer: Layer) -> Path:
        """Pasta Parquet da camada no modo local."""
        return self.data_dir / layer.schema / layer.name

    def table(self, layer: Layer) -> str:
        """Nome completo da tabela Delta no Unity Catalog."""
        return f"{self.catalog}.{layer.schema}.{layer.name}"

    @property
    def raw_file(self) -> Path:
        return self.data_dir / "raw" / "dados_credito.xlsx"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    @classmethod
    def from_env(
        cls, data_dir: str | None = None, storage: str | None = None, catalog: str | None = None
    ) -> Paths:
        """Argumentos explícitos têm prioridade sobre as variáveis de ambiente."""
        return cls(
            data_dir=Path(data_dir or os.getenv("PIPELINE_DATA_DIR", PROJECT_ROOT / "data")),
            storage=storage or os.getenv("PIPELINE_STORAGE", "parquet"),
            catalog=catalog or os.getenv("PIPELINE_CATALOG", "workspace"),
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: str
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=os.getenv("PGHOST", "localhost"),
            port=os.getenv("PGPORT", "5432"),
            dbname=os.getenv("PGDATABASE", "pipeline"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
        )
