# -*- coding: utf-8 -*-
r"""
BRONZE (batch) - Ingest raw search parquet logs vào Delta.

Đọc các folder ngày dạng parquet (202206**, 202207**) và ghi vào bảng Delta
`bronze/search_logs` ở dạng append-only, kèm metadata ingest.

Bronze = "raw zone": giữ nguyên dữ liệu gốc, chỉ thêm cột kỹ thuật
(ingest_ts, source_file, month_label). Không làm sạch ở đây.

Chạy:  python -m lakehouse.bronze.ingest_search_logs
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .. import config
from ..common import append_delta
from ..spark_session import get_spark


def get_existing_month_paths(base_dir: Path, month_prefix: str) -> List[str]:
    if not base_dir.exists():
        raise FileNotFoundError(f"Base path không tồn tại: {base_dir}")

    paths = [
        str(base_dir / name)
        for name in os.listdir(base_dir)
        if (base_dir / name).is_dir() and name.startswith(month_prefix)
    ]
    paths = sorted(paths)
    if not paths:
        raise ValueError(f"Không tìm thấy folder nào cho prefix = {month_prefix}")
    return paths


def ingest_month(spark: SparkSession, month_label: str, prefix: str) -> int:
    paths = get_existing_month_paths(config.RAW_SEARCH_DIR, prefix)
    raw_df = (
        spark.read.parquet(*paths)
        .withColumn("month_label", F.lit(month_label))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )
    append_delta(raw_df, config.BRONZE_SEARCH, partition_by=["month_label"])
    return raw_df.count()


def main() -> None:
    spark = get_spark("C360_Bronze_Search")
    try:
        total = 0
        for label, prefix in config.MONTH_PREFIXES.items():
            n = ingest_month(spark, label, prefix)
            print(f"[bronze.search] {label} ({prefix}): {n} dòng -> {config.BRONZE_SEARCH}")
            total += n
        print(f"[bronze.search] DONE, tổng {total} dòng.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
