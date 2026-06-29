# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Hàm dùng chung cho mọi tầng.

Bao gồm:
  - clean_keyword():   logic làm sạch keyword (tách ra để batch & streaming dùng chung).
  - upsert_delta():    MERGE (upsert) một DataFrame vào bảng Delta theo business key.
  - overwrite_delta(): ghi đè toàn bộ một bảng Delta.
  - export_single_csv(): xuất 1 bảng Delta ra 1 file CSV phẳng (cho BI/biên bản).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from . import config


# =============================================================================
# LÀM SẠCH KEYWORD (dùng chung batch + streaming)
# =============================================================================
def clean_keyword(df: DataFrame, month_label_col_or_value) -> DataFrame:
    """Làm sạch search keyword.

    `month_label_col_or_value` có thể là:
      - một chuỗi (vd "t6"): gán cố định cho cả DataFrame, hoặc
      - một Column (vd F.col("month_label")): tính theo từng dòng (dùng cho stream).
    """
    if isinstance(month_label_col_or_value, str):
        month_label = F.lit(month_label_col_or_value)
    else:
        month_label = month_label_col_or_value

    cleaned = (
        df
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("keyword").isNotNull())
        .withColumn("user_id", F.trim(F.col("user_id").cast("string")))
        .withColumn("keyword_raw", F.col("keyword").cast("string"))
        .withColumn("keyword", F.lower(F.trim(F.col("keyword_raw"))))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), config.URL_REGEX, " "))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), r"[^0-9a-zA-ZÀ-ỹà-ỹ\s]+", " "))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), r"\s+", " "))
        .withColumn("keyword", F.trim(F.col("keyword")))
        .filter(F.col("user_id") != "")
        .filter(F.col("keyword") != "")
        .filter(F.length(F.col("keyword")) >= 2)
        .filter(~F.col("keyword").rlike(config.ONLY_DIGITS_REGEX))
        .filter(F.col("keyword").rlike(config.HAS_ALNUM_REGEX))
        .filter(~F.col("keyword").isin(*sorted(config.BAD_EXACT_KEYWORDS)))
        .withColumn("month_label", month_label)
        .select("user_id", "keyword", "keyword_raw", "month_label")
    )
    return cleaned


# =============================================================================
# GHI VÀO DELTA
# =============================================================================
def overwrite_delta(df: DataFrame, table_path: Path, partition_by: List[str] | None = None) -> None:
    """Ghi đè toàn bộ một bảng Delta (full refresh)."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(table_path))


def append_delta(df: DataFrame, table_path: Path, partition_by: List[str] | None = None) -> None:
    """Append (chỉ thêm) vào một bảng Delta - dùng cho bronze append-only."""
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(table_path))


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    table_path: Path,
    keys: List[str],
    partition_by: List[str] | None = None,
) -> None:
    """MERGE (upsert) df vào bảng Delta theo `keys`.

    - Nếu bảng chưa tồn tại: tạo mới bằng cách ghi df.
    - Nếu đã tồn tại: matched -> update toàn bộ cột, not matched -> insert.

    Đây chính là điểm mấu chốt biến pipeline thành INCREMENTAL: chỉ những bản
    ghi thay đổi mới được cập nhật, thay vì ghi đè toàn bộ như CSV cũ.
    """
    table_path_str = str(table_path)

    if not DeltaTable.isDeltaTable(spark, table_path_str):
        overwrite_delta(df, table_path, partition_by)
        return

    delta_table = DeltaTable.forPath(spark, table_path_str)
    condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    (
        delta_table.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def read_delta(spark: SparkSession, table_path: Path) -> DataFrame:
    """Đọc một bảng Delta."""
    return spark.read.format("delta").load(str(table_path))


# =============================================================================
# EXPORT CSV PHẲNG (tùy chọn, cho BI cũ / nộp báo cáo)
# =============================================================================
def export_single_csv(df: DataFrame, output_file: Path) -> None:
    """Xuất DataFrame ra đúng 1 file .csv (gộp part-file của Spark)."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = output_file.parent / f"tmp_{output_file.stem}"

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_file.exists():
        output_file.unlink()

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(temp_dir))

    part_file = None
    for name in os.listdir(temp_dir):
        if name.startswith("part-") and name.endswith(".csv"):
            part_file = temp_dir / name
            break
    if part_file is None:
        raise FileNotFoundError(f"Không tìm thấy part-file trong {temp_dir}")

    shutil.move(str(part_file), str(output_file))
    shutil.rmtree(temp_dir)
