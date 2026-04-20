"""
conftest.py — pytest-konfiguraatio

Lisää projektin moduulihakemistot Python-polkuun, jotta testit löytävät
`fetch_trains.py` ja `bronze.py` ilman kikkailua yksittäisissä testitiedostoissa.

pytest lukee tämän automaattisesti ennen testien suoritusta.
"""

import sys
from pathlib import Path

# Projektin juuri on kaksi tasoa ylöspäin tästä tiedostosta
ROOT = Path(__file__).parent.parent

# Lisätään moduulihakemistot Python-polkuun
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "01_fetch"))
sys.path.insert(0, str(ROOT / "03_bronze"))
sys.path.insert(0, str(ROOT / "04_silver"))
