# -*- coding: utf-8 -*-
r"""
BRONZE (batch) - Ingest raw content JSON logs vào Delta.

Đọc các file <YYYYMMDD>.json trong khoảng ngày cấu hình, phẳng hóa `_source.*`
và append vào bảng Delta `bronze/content_logs`, kèm event_date + metadata ingest.

Chạy:  python -m lakehouse.bronze.ingest_content_logs
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .. import config
from ..common import append_delta
from ..spark_session import get_spark


def generate_range_date(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    out, cur = [], start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return out


def ingest_one_day(spark: SparkSession, input_dir: Path, day_str: str) -> int:
    json_path = input_dir / f"{day_str}.json"
    if not json_path.exists():
        print(f"[bronze.content] Bỏ qua (không có file): {json_path}")
        return 0

    raw_df = (
        spark.read.json(str(json_path)).select("_source.*")
        .withColumn("event_date", F.to_date(F.lit(day_str), "yyyyMMdd"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(str(json_path)))
    )
    append_delta(raw_df, config.BRONZE_CONTENT, partition_by=["event_date"])
    return raw_df.count()


def main() -> None:
    spark = get_spark("C360_Bronze_Content")
    try:
        days = generate_range_date(config.CONTENT_START_DATE, config.CONTENT_END_DATE)
        total = 0
        for day_str in days:
            n = ingest_one_day(spark, config.RAW_CONTENT_DIR, day_str)
            if n:
                print(f"[bronze.content] {day_str}: {n} dòng")
            total += n
        print(f"[bronze.content] DONE, tổng {total} dòng -> {config.BRONZE_CONTENT}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
