"""Ponto de entrada de linha de comando.

Exemplos:
    credit-pipeline run                      # bronze -> silver -> gold -> quality -> load
    credit-pipeline run --skip-load          # tudo, menos a carga no PostgreSQL
    credit-pipeline run --steps bronze silver
    credit-pipeline analytics --engine postgres
    credit-pipeline ml
"""

from __future__ import annotations

import argparse
import logging
import time

from credit_pipeline import analytics, bronze, gold, load, ml, quality, silver
from credit_pipeline.config import Paths

STEPS = {
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
    "quality": quality.run,
    "load": load.run,
}

logger = logging.getLogger("credit_pipeline")


def run_steps(steps: list[str], paths: Paths) -> None:
    for step in steps:
        start = time.perf_counter()
        STEPS[step](paths)
        logger.info("Etapa %s concluída em %.2fs", step, time.perf_counter() - start)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="credit-pipeline", description="Pipeline de dados de análise de crédito")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="executa as etapas do pipeline")
    run_parser.add_argument("--steps", nargs="+", choices=list(STEPS), default=list(STEPS))
    run_parser.add_argument("--skip-load", action="store_true", help="não carrega no PostgreSQL")

    analytics_parser = sub.add_parser("analytics", help="executa as consultas de sql/")
    analytics_parser.add_argument("--engine", choices=["sqlite", "postgres"], default="sqlite")

    sub.add_parser("ml", help="treina e avalia o modelo de score")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    paths = Paths.from_env()

    if args.command == "run":
        steps = [s for s in args.steps if not (args.skip_load and s == "load")]
        run_steps(steps, paths)
    elif args.command == "analytics":
        analytics.run(paths, engine=args.engine)
    elif args.command == "ml":
        ml.run(paths)


if __name__ == "__main__":
    main()
