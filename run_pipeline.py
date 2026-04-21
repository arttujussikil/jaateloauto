#!/usr/bin/env python3
"""
run_pipeline.py
===============
Hakee datan API:sta ja ajaa kaikki
muunnokset loppuun asti.

Käyttö:
    # Aja koko putki (viimeiset 7 päivää):
    python run_pipeline.py

    # Aja tietyllä aikavälillä:
    python run_pipeline.py --days-back 14

    # Aja vain tietty vaihe:
    python run_pipeline.py --only fetch
    python run_pipeline.py --only bronze
    python run_pipeline.py --only silver
    python run_pipeline.py --only gold
    python run_pipeline.py --only visualisation

    # Käynnistä dashboard putken jälkeen:
    python run_pipeline.py --visualise
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).parent


def run_step(name: str, script: Path, extra_args: list[str] = None) -> bool:
    """
    Ajaa yhden putken vaiheen erillisessä Python-prosessissa.

    Erillinen prosessi varmistaa, että jokainen vaihe on itsenäinen
    eikä vaikuta muiden vaiheiden Python-ympäristöön.

    Args:
        name: Vaiheen nimi (lokitusta varten).
        script: Ajettavan skriptin polku.
        extra_args: Lisäargumentit skriptille.

    Returns:
        True jos onnistui (exit code 0).
    """
    cmd = [sys.executable, str(script)] + (extra_args or [])
    log.info("=== Vaihe: %s ===", name)
    log.info("Komento: %s", " ".join(cmd))

    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        log.error("Vaihe '%s' epäonnistui (exit code %d)", name, result.returncode)
        return False

    log.info("Vaihe '%s' valmis.", name)
    return True


def run_dbt() -> bool:
    """Ajaa dbt run + dbt test 06_transform-hakemistossa."""
    dbt_dir = ROOT / "06_transform"
    log.info("=== Vaihe: gold (dbt) ===")
    for dbt_cmd in (["dbt", "run"], ["dbt", "test"]):
        log.info("Komento: %s", " ".join(dbt_cmd))
        result = subprocess.run(dbt_cmd, cwd=dbt_dir, shell=True)
        if result.returncode != 0:
            log.error("dbt-komento '%s' epäonnistui (exit code %d)", " ".join(dbt_cmd), result.returncode)
            return False
    log.info("Vaihe 'gold' valmis.")
    return True


def run_visualisation() -> bool:
    """Käynnistää Streamlit-dashboardin."""
    app = ROOT / "07_visualisation" / "app.py"
    log.info("=== Vaihe: visualisation ===")
    log.info("Käynnistetään Streamlit: streamlit run %s", app)
    result = subprocess.run(["streamlit", "run", str(app)], cwd=ROOT, shell=True)
    return result.returncode == 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Aja VR-dataputki.")
    parser.add_argument("--days-back", type=int, default=7)
    parser.add_argument(
        "--only",
        choices=["fetch", "bronze", "silver", "gold", "visualisation"],
        help="Aja vain tietty vaihe",
    )
    parser.add_argument(
        "--visualise",
        action="store_true",
        help="Käynnistä Streamlit-dashboard putken jälkeen",
    )
    args = parser.parse_args()

    python_steps = {
        "fetch": (
            ROOT / "01_fetch" / "fetch_trains.py",
            ["--days-back", str(args.days_back)],
        ),
        "bronze": (ROOT / "03_bronze" / "bronze.py", ["--all"]),
        "silver": (ROOT / "04_silver" / "silver.py", []),
    }

    if args.only == "gold":
        success = run_dbt()
    elif args.only == "visualisation":
        success = run_visualisation()
    elif args.only:
        script, extra = python_steps[args.only]
        success = run_step(args.only, script, extra)
    else:
        success = True
        for step_name, (script, extra) in python_steps.items():
            if not run_step(step_name, script, extra):
                success = False
                break

        if success:
            success = run_dbt()

        if success:
            log.info("✓ Kaikki vaiheet onnistuivat!")
            if args.visualise:
                run_visualisation()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
