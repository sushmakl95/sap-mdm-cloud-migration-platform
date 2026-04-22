"""Generate sample SAP MDM data for local development.

Produces anonymized CSV files in data/sample/ for:
  - MARA  (materials)
  - KNA1  (customers)
  - LFA1  (vendors)

Output shape matches raw SAP column names (MATNR, KUNNR, etc.) so the
extract → transform pipeline can be exercised end-to-end without a real SAP
instance.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

MATERIAL_TYPES = ["HAWA", "FERT", "ROH", "HALB", "DIEN"]
INDUSTRY_SECTORS = ["M", "C", "A"]
MATERIAL_GROUPS = [f"MG{i:03d}" for i in range(50)]
UNITS = ["EA", "KG", "LB", "M", "L", "BX"]
COUNTRIES = ["US", "DE", "GB", "IN", "JP", "FR", "CA", "AU", "BR"]
SAP_USERS = [f"USER{i:03d}" for i in range(20)]


def _sap_date(dt: datetime) -> str:
    """Format as SAP DATS: yyyyMMdd."""
    return dt.strftime("%Y%m%d")


def _padded_id(n: int, width: int) -> str:
    """SAP-style left-padded ID."""
    return str(n).zfill(width)


def generate_mara(out_path: Path, n: int) -> None:
    start = datetime(2018, 1, 1)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "MANDT", "MATNR", "ERSDA", "ERNAM", "LAEDA", "AENAM",
            "MTART", "MBRSH", "MATKL", "MEINS", "BRGEW", "NTGEW",
            "GEWEI", "LVORM",
        ])
        for i in range(1, n + 1):
            created_date = start + timedelta(days=random.randint(0, 2000))
            changed_date = created_date + timedelta(days=random.randint(0, 300))
            weight = round(random.uniform(0.01, 100), 3)
            w.writerow([
                "001",
                _padded_id(i, 18),
                _sap_date(created_date),
                random.choice(SAP_USERS),
                _sap_date(changed_date),
                random.choice(SAP_USERS),
                random.choice(MATERIAL_TYPES),
                random.choice(INDUSTRY_SECTORS),
                random.choice(MATERIAL_GROUPS),
                random.choice(UNITS),
                weight,
                round(weight * 0.9, 3),
                "KG",
                "X" if random.random() < 0.01 else "",
            ])


def generate_kna1(out_path: Path, n: int) -> None:
    start = datetime(2010, 1, 1)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "MANDT", "KUNNR", "NAME1", "LAND1", "ORT01", "PSTLZ",
            "STRAS", "TELF1", "TELFX", "SMTP_ADDR", "ERDAT", "ERNAM", "LOEVM",
        ])
        for i in range(1, n + 1):
            created = start + timedelta(days=random.randint(0, 5000))
            w.writerow([
                "001",
                _padded_id(i, 10),
                fake.company()[:35],
                random.choice(COUNTRIES),
                fake.city()[:35],
                fake.postcode()[:10],
                fake.street_address()[:35],
                fake.phone_number()[:16],
                fake.phone_number()[:31],
                fake.company_email()[:241],
                _sap_date(created),
                random.choice(SAP_USERS),
                "X" if random.random() < 0.005 else "",
            ])


def generate_lfa1(out_path: Path, n: int) -> None:
    start = datetime(2012, 1, 1)
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "MANDT", "LIFNR", "NAME1", "LAND1", "STCD1", "STCD2",
            "ERDAT", "LOEVM",
        ])
        for i in range(1, n + 1):
            created = start + timedelta(days=random.randint(0, 4500))
            w.writerow([
                "001",
                _padded_id(i, 10),
                fake.company()[:35],
                random.choice(COUNTRIES),
                fake.ein()[:16],
                str(fake.random_number(digits=11))[:11],
                _sap_date(created),
                "X" if random.random() < 0.01 else "",
            ])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("data/sample"))
    parser.add_argument("--mara-rows", type=int, default=5000)
    parser.add_argument("--kna1-rows", type=int, default=3000)
    parser.add_argument("--lfa1-rows", type=int, default=500)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating MARA ({args.mara_rows:,} rows)...")
    generate_mara(args.output_dir / "MARA.csv", args.mara_rows)

    print(f"Generating KNA1 ({args.kna1_rows:,} rows)...")
    generate_kna1(args.output_dir / "KNA1.csv", args.kna1_rows)

    print(f"Generating LFA1 ({args.lfa1_rows:,} rows)...")
    generate_lfa1(args.output_dir / "LFA1.csv", args.lfa1_rows)

    print(f"Done. Files in {args.output_dir}/")


if __name__ == "__main__":
    main()
