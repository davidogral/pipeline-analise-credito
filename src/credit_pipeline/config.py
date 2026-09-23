"""Configuração central do pipeline: caminhos das camadas e conexão com o banco.

Tudo é resolvido a partir de variáveis de ambiente, com defaults que funcionam
rodando o projeto localmente a partir da raiz do repositório.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Paths:
    """Localização de cada camada do data lake local."""

    data_dir: Path

    @property
    def raw_file(self) -> Path:
        return self.data_dir / "raw" / "dados_credito.xlsx"

    @property
    def bronze_path(self) -> Path:
        return self.data_dir / "bronze" / "dados_brutos"

    @property
    def silver_path(self) -> Path:
        return self.data_dir / "silver" / "dados_limpos"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def reports_dir(self) -> Path:
        return self.data_dir / "reports"

    @classmethod
    def from_env(cls) -> Paths:
        return cls(data_dir=Path(os.getenv("PIPELINE_DATA_DIR", PROJECT_ROOT / "data")))


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
