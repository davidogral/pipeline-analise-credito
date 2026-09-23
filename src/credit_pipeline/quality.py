"""Validação de qualidade da camada Gold.

Funciona como um quality gate: as regras com severidade "error" interrompem o
pipeline antes da carga no banco, e todas as regras geram um relatório em JSON
e um gráfico de completude por coluna.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from credit_pipeline.config import Paths  # noqa: E402
from credit_pipeline.io import read_layer_csv  # noqa: E402

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["CODIGO_CLIENTE", "UF", "IDADE", "ULTIMO_SALARIO", "RENDA_TOTAL", "SCORE"]

VALID_RANGES: dict[str, tuple[float, float]] = {
    "IDADE": (18, 120),
    "QT_FILHOS": (0, 20),
    "QT_IMOVEIS": (0, 50),
    "VL_IMOVEIS": (0, 1_000_000_000),
    "TEMPO_ULTIMO_EMPREGO_MESES": (0, 600),
    "ULTIMO_SALARIO": (0, 1_000_000),
    "QT_CARROS": (0, 20),
    "VALOR_TABELA_CARROS": (0, 2_000_000),
    "RENDA_TOTAL": (0, 2_000_000),
    "SCORE": (0, 100),
}

UFS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}


class QualityGateError(RuntimeError):
    """Levantada quando alguma regra de severidade "error" falha."""


@dataclass
class CheckResult:
    rule: str
    column: str
    severity: str
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


def run_checks(df: pd.DataFrame) -> list[CheckResult]:
    results = [CheckResult("dataset_nao_vazio", "*", "error", 0 if len(df) else 1)]

    for column in REQUIRED_COLUMNS:
        missing = int(df[column].isna().sum()) if column in df.columns else len(df)
        results.append(CheckResult("not_null", column, "error", missing))

    duplicated_keys = int(df["CODIGO_CLIENTE"].duplicated().sum())
    results.append(CheckResult("chave_unica", "CODIGO_CLIENTE", "error", duplicated_keys))

    for column, (low, high) in VALID_RANGES.items():
        values = pd.to_numeric(df[column], errors="coerce")
        out_of_range = int(((values < low) | (values > high)).sum())
        results.append(CheckResult(f"intervalo_{low}_{high}", column, "error", out_of_range))

    invalid_uf = int((~df["UF"].isin(UFS)).sum())
    results.append(CheckResult("dominio_uf", "UF", "error", invalid_uf))

    # Consistência: quem declara outra renda deve ter valor preenchido.
    outra_renda = df["OUTRA_RENDA"].astype("string").str.upper().isin(["TRUE", "SIM"])
    sem_valor = int((outra_renda & ~(df["OUTRA_RENDA_VALOR"] > 0)).sum())
    results.append(CheckResult("outra_renda_com_valor", "OUTRA_RENDA_VALOR", "warning", sem_valor))

    return results


def summarize(df: pd.DataFrame, results: list[CheckResult]) -> dict:
    total_cells = df.size
    completude = float(df.notna().sum().sum() / total_cells * 100) if total_cells else 0.0
    unicidade = float(len(df.drop_duplicates()) / len(df) * 100) if len(df) else 0.0
    return {
        "registros": len(df),
        "colunas": df.shape[1],
        "completude_pct": round(completude, 2),
        "unicidade_pct": round(unicidade, 2),
        "regras_total": len(results),
        "regras_aprovadas": sum(r.passed for r in results),
        "regras": [{**asdict(r), "passed": r.passed} for r in results],
    }


def plot_completeness(df: pd.DataFrame, output_file: Path) -> None:
    completude = (df.notna().mean() * 100).sort_values()
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(completude.index, completude.values, color="#2E7D5B")
    ax.set_xlim(0, 100)
    ax.set_xlabel("Completude (%)")
    ax.set_title("Completude dos dados por coluna (camada Gold)")
    fig.tight_layout()
    fig.savefig(output_file, dpi=120)
    plt.close(fig)


def run(paths: Paths) -> dict:
    df = read_layer_csv(paths.gold_dir / "dados_gold.csv")
    results = run_checks(df)
    report = summarize(df, results)

    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    (paths.reports_dir / "quality_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    plot_completeness(df, paths.reports_dir / "quality_report.png")

    for r in results:
        if not r.passed:
            logger.warning("Regra %s em %s falhou em %d linhas (%s)", r.rule, r.column, r.failed_rows, r.severity)
    logger.info(
        "Qualidade: %d/%d regras aprovadas, completude %.2f%%, unicidade %.2f%%",
        report["regras_aprovadas"], report["regras_total"], report["completude_pct"], report["unicidade_pct"],
    )

    errors = [r for r in results if r.severity == "error" and not r.passed]
    if errors:
        raise QualityGateError(f"{len(errors)} regra(s) críticas de qualidade falharam: {[e.rule for e in errors]}")
    return report
