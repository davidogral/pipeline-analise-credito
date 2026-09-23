"""Validação de qualidade da camada Gold.

Funciona como um quality gate: as regras com severidade "error" interrompem o
pipeline antes da carga no banco, e todas as regras geram um relatório em JSON
e um gráfico de completude por coluna.

Todas as regras são expressões de agregação avaliadas em uma única passada
sobre os dados, em vez de um `count()` por regra.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import Column, DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from credit_pipeline.config import Paths, gold  # noqa: E402
from credit_pipeline.io import read_layer, write_layer  # noqa: E402
from credit_pipeline.spark import cache, get_spark  # noqa: E402

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

UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]


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


def _count_where(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def _rules() -> list[tuple[str, str, str, Column]]:
    rules = [("not_null", c, "error", _count_where(F.col(c).isNull())) for c in REQUIRED_COLUMNS]
    rules += [
        (f"intervalo_{low}_{high}", c, "error", _count_where((F.col(c) < low) | (F.col(c) > high)))
        for c, (low, high) in VALID_RANGES.items()
    ]
    rules.append(("dominio_uf", "UF", "error", _count_where(~F.col("UF").isin(UFS) | F.col("UF").isNull())))
    # Consistência: quem declara outra renda deve ter valor preenchido.
    sem_valor = F.col("OUTRA_RENDA") & ~(F.coalesce(F.col("OUTRA_RENDA_VALOR"), F.lit(0.0)) > 0)
    rules.append(("outra_renda_com_valor", "OUTRA_RENDA_VALOR", "warning", _count_where(sem_valor)))
    return rules


def run_checks(df: DataFrame) -> tuple[list[CheckResult], dict]:
    rules = _rules()
    aggregations = [expr.alias(f"r{i}") for i, (*_, expr) in enumerate(rules)]
    aggregations += [F.count(F.col(c)).alias(f"filled::{c}") for c in df.columns]
    aggregations += [F.count("*").alias("total"), F.countDistinct("CODIGO_CLIENTE").alias("distinct_keys")]
    stats = df.agg(*aggregations).first().asDict()

    total = stats["total"]
    results = [CheckResult("dataset_nao_vazio", "*", "error", 0 if total else 1)]
    results += [
        CheckResult(rule, column, severity, int(stats[f"r{i}"] or 0))
        for i, (rule, column, severity, _) in enumerate(rules)
    ]
    results.insert(len(REQUIRED_COLUMNS) + 1, CheckResult(
        "chave_unica", "CODIGO_CLIENTE", "error", total - stats["distinct_keys"]
    ))
    completeness = {c: (stats[f"filled::{c}"] / total * 100) if total else 0.0 for c in df.columns}
    return results, {"total": total, "completude": completeness}


def summarize(df: DataFrame, results: list[CheckResult], stats: dict) -> dict:
    total = stats["total"]
    completude = sum(stats["completude"].values()) / len(stats["completude"]) if total else 0.0
    unicidade = df.dropDuplicates().count() / total * 100 if total else 0.0
    return {
        "registros": total,
        "colunas": len(df.columns),
        "completude_pct": round(completude, 2),
        "unicidade_pct": round(unicidade, 2),
        "regras_total": len(results),
        "regras_aprovadas": sum(r.passed for r in results),
        "regras": [{**asdict(r), "passed": r.passed} for r in results],
    }


def plot_completeness(completeness: dict[str, float], output_file: Path) -> None:
    ordered = sorted(completeness.items(), key=lambda item: item[1])
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh([c for c, _ in ordered], [v for _, v in ordered], color="#2E7D5B")
    ax.set_xlim(0, 100)
    ax.set_xlabel("Completude (%)")
    ax.set_title("Completude dos dados por coluna (camada Gold)")
    fig.tight_layout()
    fig.savefig(output_file, dpi=120)
    plt.close(fig)


def record_results(paths: Paths, results: list[CheckResult]) -> None:
    """No Delta, registra cada execução em gold.qualidade_execucoes para auditoria e tendência."""
    executado_em = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(executado_em, r.rule, r.column, r.severity, r.failed_rows, r.passed) for r in results]
    schema = (
        "executado_em timestamp, regra string, coluna string, severidade string, "
        "linhas_com_falha long, aprovada boolean"
    )
    write_layer(get_spark().createDataFrame(rows, schema), paths, gold("qualidade_execucoes"), mode="append")


def run(paths: Paths) -> dict:
    df = cache(read_layer(paths, gold("dados_gold")))
    results, stats = run_checks(df)
    report = summarize(df, results, stats)
    if paths.storage == "delta":
        record_results(paths, results)

    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    (paths.reports_dir / "quality_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    plot_completeness(stats["completude"], paths.reports_dir / "quality_report.png")

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
