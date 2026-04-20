#!/usr/bin/env python3
"""
run_pipeline.py
===============
Koko dataputken orkestraattori: hakee datan API:sta ja ajaa kaikki
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

Huomio dbt:stä:
    dbt-vaihe (silver → gold) ajetaan erillisellä komennolla:
        cd 06_transform && dbt run && dbt test
    Tämä johtuu siitä, että dbt hallinnoi omaa ympäristöään.
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Aja VR-dataputki.")
    parser.add_argument("--days-back", type=int, default=7)
    parser.add_argument(
        "--only",
        choices=["fetch", "bronze", "silver"],
        help="Aja vain tietty vaihe",
    )
    args = parser.parse_args()

    steps = {
        "fetch": (
            ROOT / "01_fetch" / "fetch_trains.py",
            ["--days-back", str(args.days_back)],
        ),
        "bronze": (ROOT / "03_bronze" / "bronze.py", ["--all"]),
        "silver": (ROOT / "04_silver" / "silver.py", []),
    }

    if args.only:
        script, extra = steps[args.only]
        success = run_step(args.only, script, extra)
    else:
        success = True
        for step_name, (script, extra) in steps.items():
            if not run_step(step_name, script, extra):
                success = False
                break

        if success:
            log.info(
                "\n✓ Kaikki vaiheet onnistuivat!\n"
                "Aja seuraavaksi dbt:\n"
                "  cd 06_transform && dbt run && dbt test\n"
                "  dbt docs generate && dbt docs serve"
            )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
