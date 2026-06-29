# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Orchestrator chạy toàn bộ pipeline BATCH theo thứ tự.

Mỗi bước chạy như một tiến trình con riêng (mỗi SparkSession sạch sẽ), theo
luồng medallion:

    BRONZE: ingest_search_logs, ingest_content_logs
    SILVER: clean_search, clean_content
    GOLD  : customer_search_trend, customer_content_profile, customer360_profile

Chạy:  python -m lakehouse.run_pipeline
       python -m lakehouse.run_pipeline --only gold        # chỉ chạy nhóm gold
       python -m lakehouse.run_pipeline --from silver      # chạy từ silver trở đi

LƯU Ý: luồng STREAMING (near-realtime) KHÔNG nằm trong orchestrator này vì nó
chạy thường trú. Khởi động riêng:
    python -m lakehouse.bronze.stream_search_logs
    python -m lakehouse.silver.stream_clean_search
"""

from __future__ import annotations

import argparse
import subprocess
import sys

# (group, module) theo đúng thứ tự phụ thuộc.
STEPS = [
    ("bronze", "lakehouse.bronze.ingest_search_logs"),
    ("bronze", "lakehouse.bronze.ingest_content_logs"),
    ("silver", "lakehouse.silver.clean_search"),
    ("silver", "lakehouse.silver.clean_content"),
    ("gold", "lakehouse.gold.customer_search_trend"),
    ("gold", "lakehouse.gold.customer_content_profile"),
    ("gold", "lakehouse.gold.customer360_profile"),
]

GROUP_ORDER = {"bronze": 0, "silver": 1, "gold": 2}


def run_module(module: str) -> None:
    print("\n" + "=" * 70)
    print(f">>> RUN: {module}")
    print("=" * 70)
    result = subprocess.run([sys.executable, "-m", module])
    if result.returncode != 0:
        raise SystemExit(f"Bước {module} lỗi (exit {result.returncode}). Dừng pipeline.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Customer360 Lakehouse batch pipeline")
    parser.add_argument("--only", choices=["bronze", "silver", "gold"], help="Chỉ chạy 1 nhóm")
    parser.add_argument("--from", dest="from_group", choices=["bronze", "silver", "gold"],
                        help="Chạy từ nhóm này trở đi")
    args = parser.parse_args()

    steps = STEPS
    if args.only:
        steps = [s for s in STEPS if s[0] == args.only]
    elif args.from_group:
        start = GROUP_ORDER[args.from_group]
        steps = [s for s in STEPS if GROUP_ORDER[s[0]] >= start]

    for _, module in steps:
        run_module(module)

    print("\n=== PIPELINE HOÀN TẤT ===")


if __name__ == "__main__":
    main()
