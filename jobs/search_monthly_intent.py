# -*- coding: utf-8 -*-
r"""
Customer360 - Search Monthly Intent ETL

Mục tiêu:
1) Đọc raw parquet search logs theo folder ngày.
2) Làm sạch keyword để giảm keyword rác.
3) Tính top search keyword theo user cho tháng 6 và tháng 7.
4) Xuất ra file trung gian để map category ở bước sau.

Input:
    E:\DataSetDataEngineer\log_search\202206**\*.parquet
    E:\DataSetDataEngineer\log_search\202207**\*.parquet

Output:
    E:\DataSetDataEngineer\customer360_output\silver\search_clean_monthly.parquet
    E:\DataSetDataEngineer\customer360_output\gold\user_top_search_t6_t7.csv
    E:\DataSetDataEngineer\customer360_output\gold\content_summary_for_mapping.csv
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


# =========================
# CONFIG
# =========================
BASE_PATH = Path(r"E:\DataSetDataEngineer\log_search")
OUTPUT_BASE = Path(r"E:\DataSetDataEngineer\customer360_output")
SILVER_DIR = OUTPUT_BASE / "silver"
GOLD_DIR = OUTPUT_BASE / "gold"

MONTH_PREFIXES: Dict[str, str] = {
    "t6": "202206",
    "t7": "202207",
}

MIN_COUNT_PER_MONTH = 2
MIN_TOTAL_USERS_FOR_CONTENT = 5

USER_OUTPUT_FILE = GOLD_DIR / "user_top_search_t6_t7.csv"
CONTENT_OUTPUT_FILE = GOLD_DIR / "content_summary_for_mapping.csv"
SEARCH_CLEAN_PARQUET = SILVER_DIR / "search_clean_monthly.parquet"

BAD_EXACT_KEYWORDS = {
    "null", "none", "na", "n/a", "undefined", "unk", "unknown",
}

URL_REGEX = r"(http|https|www\.)"
ONLY_DIGITS_REGEX = r"^[0-9]+$"
HAS_ALNUM_REGEX = r".*[a-zA-ZÀ-ỹà-ỹ0-9].*"


# =========================
# SPARK
# =========================
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Customer360_Search_Monthly_Intent_ETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================
# HELPERS
# =========================
def get_existing_month_paths(base_dir: Path, month_prefix: str) -> List[str]:
    if not base_dir.exists():
        raise FileNotFoundError(f"Base path không tồn tại: {base_dir}")

    paths: List[str] = []
    for name in os.listdir(base_dir):
        full_path = base_dir / name
        if full_path.is_dir() and name.startswith(month_prefix):
            paths.append(str(full_path))

    paths = sorted(paths)
    if not paths:
        raise ValueError(f"Không tìm thấy folder nào cho tháng prefix = {month_prefix}")

    return paths


def clean_keyword(df: DataFrame, month_label: str) -> DataFrame:
    cleaned = (
        df.select("user_id", "keyword")
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("keyword").isNotNull())
        .withColumn("user_id", F.trim(F.col("user_id").cast("string")))
        .withColumn("keyword_raw", F.col("keyword").cast("string"))
        .withColumn("keyword", F.lower(F.trim(F.col("keyword_raw"))))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), URL_REGEX, " "))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), r"[^0-9a-zA-ZÀ-ỹà-ỹ\s]+", " "))
        .withColumn("keyword", F.regexp_replace(F.col("keyword"), r"\s+", " "))
        .withColumn("keyword", F.trim(F.col("keyword")))
        .filter(F.col("user_id") != "")
        .filter(F.col("keyword") != "")
        .filter(F.length(F.col("keyword")) >= 2)
        .filter(~F.col("keyword").rlike(ONLY_DIGITS_REGEX))
        .filter(F.col("keyword").rlike(HAS_ALNUM_REGEX))
        .filter(~F.col("keyword").isin(*sorted(BAD_EXACT_KEYWORDS)))
        .withColumn("month_label", F.lit(month_label))
        .select("user_id", "keyword", "keyword_raw", "month_label")
    )
    return cleaned


def aggregate_keyword_counts(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("user_id", "month_label", "keyword")
        .agg(F.count(F.lit(1)).alias("search_count"))
    )


def get_top1_keyword_by_user_month(df_counts: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("user_id", "month_label").orderBy(
        F.col("search_count").desc(),
        F.col("keyword").asc(),
    )

    return (
        df_counts.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .filter(F.col("search_count") >= MIN_COUNT_PER_MONTH)
        .select("user_id", "month_label", "keyword", "search_count")
    )


def build_user_t6_t7(top1_df: DataFrame) -> DataFrame:
    t6_df = (
        top1_df.filter(F.col("month_label") == "t6")
        .select(
            "user_id",
            F.col("keyword").alias("most_search_t6"),
            F.col("search_count").alias("count_t6"),
        )
    )

    t7_df = (
        top1_df.filter(F.col("month_label") == "t7")
        .select(
            "user_id",
            F.col("keyword").alias("most_search_t7"),
            F.col("search_count").alias("count_t7"),
        )
    )

    return (
        t6_df.join(t7_df, on="user_id", how="inner")
        .select("user_id", "most_search_t6", "count_t6", "most_search_t7", "count_t7")
    )


def build_content_summary(user_df: DataFrame) -> DataFrame:
    content_t6 = (
        user_df.groupBy(F.col("most_search_t6").alias("keyword"))
        .agg(F.countDistinct("user_id").alias("n_users_t6"))
    )

    content_t7 = (
        user_df.groupBy(F.col("most_search_t7").alias("keyword"))
        .agg(F.countDistinct("user_id").alias("n_users_t7"))
    )

    return (
        content_t6.join(content_t7, on="keyword", how="full_outer")
        .fillna(0, subset=["n_users_t6", "n_users_t7"])
        .withColumn("total_users", F.col("n_users_t6") + F.col("n_users_t7"))
        .filter(F.col("keyword").isNotNull())
        .filter(F.col("keyword") != "")
        .filter(F.col("total_users") >= MIN_TOTAL_USERS_FOR_CONTENT)
        .orderBy(F.col("total_users").desc(), F.col("keyword").asc())
    )


def safe_delete_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def write_csv_single_file(df: DataFrame, final_output_file: Path) -> None:
    parent_dir = final_output_file.parent
    base_name = final_output_file.name
    temp_dir = parent_dir / f"tmp_{base_name}"

    parent_dir.mkdir(parents=True, exist_ok=True)
    safe_delete_path(temp_dir)
    safe_delete_path(final_output_file)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(temp_dir))
    )

    part_file = None
    for file_name in os.listdir(temp_dir):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            part_file = temp_dir / file_name
            break

    if part_file is None:
        raise FileNotFoundError(f"Không tìm thấy part-file trong {temp_dir}")

    shutil.move(str(part_file), str(final_output_file))
    shutil.rmtree(temp_dir)


def main() -> None:
    spark = build_spark()

    try:
        t6_paths = get_existing_month_paths(BASE_PATH, MONTH_PREFIXES["t6"])
        t7_paths = get_existing_month_paths(BASE_PATH, MONTH_PREFIXES["t7"])

        print("=== T6 paths ===")
        for path in t6_paths:
            print(path)

        print("=== T7 paths ===")
        for path in t7_paths:
            print(path)

        df_t6_raw = spark.read.parquet(*t6_paths)
        df_t7_raw = spark.read.parquet(*t7_paths)

        df_t6_clean = clean_keyword(df_t6_raw, "t6")
        df_t7_clean = clean_keyword(df_t7_raw, "t7")
        search_clean_df = df_t6_clean.unionByName(df_t7_clean)

        safe_delete_path(SEARCH_CLEAN_PARQUET)
        SEARCH_CLEAN_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        search_clean_df.write.mode("overwrite").parquet(str(SEARCH_CLEAN_PARQUET))

        keyword_counts = aggregate_keyword_counts(search_clean_df)
        top1_df = get_top1_keyword_by_user_month(keyword_counts)
        user_result = build_user_t6_t7(top1_df)
        content_summary = build_content_summary(user_result)

        write_csv_single_file(user_result, USER_OUTPUT_FILE)
        write_csv_single_file(content_summary, CONTENT_OUTPUT_FILE)

        print("=== ETL DONE ===")
        print(f"SEARCH_CLEAN_PARQUET = {SEARCH_CLEAN_PARQUET}")
        print(f"USER_OUTPUT_FILE     = {USER_OUTPUT_FILE}")
        print(f"CONTENT_OUTPUT_FILE  = {CONTENT_OUTPUT_FILE}")
        print(f"search_clean rows    = {search_clean_df.count()}")
        print(f"user_result rows     = {user_result.count()}")
        print(f"content_summary rows = {content_summary.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
