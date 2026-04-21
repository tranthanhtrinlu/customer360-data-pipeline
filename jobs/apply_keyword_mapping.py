# -*- coding: utf-8 -*-
r"""
Customer360 - Apply Keyword Mapping

Mục tiêu:
1) Đọc output user_top_search_t6_t7.csv từ bước 01.
2) Đọc file mapping keyword_category_mapping.csv.
3) Gán category cho most_search_t6 và most_search_t7.
4) Tạo mart customer search trend để dùng cho BI / báo cáo / CV.

Input:
    E:\DataSetDataEngineer\customer360_output\gold\user_top_search_t6_t7.csv
    E:\Customer360_Project\reference\keyword_category_mapping.csv

Output:
    E:\DataSetDataEngineer\customer360_output\gold\mart_customer_search_trend.csv
    E:\DataSetDataEngineer\customer360_output\gold\mart_customer_search_trend.xlsx
    E:\DataSetDataEngineer\customer360_output\gold\category_transition_summary.csv
"""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_DIR = Path(__file__).resolve().parents[1]

BASE_OUTPUT_DIR = Path(r"E:\DataSetDataEngineer\customer360_output")

INPUT_CSV_PATH = BASE_OUTPUT_DIR / "gold" / "user_top_search_t6_t7.csv"
MAPPING_CSV_PATH = PROJECT_DIR / "reference" / "keyword_category_mapping.csv"
OUTPUT_CSV_PATH = BASE_OUTPUT_DIR / "gold" / "mart_customer_search_trend.csv"
OUTPUT_XLSX_PATH = BASE_OUTPUT_DIR / "gold" / "mart_customer_search_trend.xlsx"
TRANSITION_SUMMARY_CSV = BASE_OUTPUT_DIR / "gold" / "category_transition_summary.csv"


# =========================
# NORMALIZE + MAPPING
# =========================
def normalize_text(text: Any) -> str:
    if text is None:
        return ""

    text = str(text).strip().lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_mapping_rules(mapping_path: Path) -> List[Dict[str, Any]]:
    if not mapping_path.exists():
        raise FileNotFoundError(
            f"Không tìm thấy file mapping: {mapping_path}\n"
            "Hãy tạo file keyword_category_mapping.csv trước khi chạy bước này."
        )

    rules: List[Dict[str, Any]] = []
    seen = set()

    with mapping_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"keyword", "category"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError("File mapping phải có 2 cột: keyword, category")

        for row in reader:
            raw_keyword = row.get("keyword", "")
            raw_category = row.get("category", "Khác")

            keyword = normalize_text(raw_keyword)
            category = str(raw_category).strip() or "Khác"

            if not keyword:
                continue
            if keyword in seen:
                continue

            seen.add(keyword)
            rules.append({
                "keyword": keyword,
                "category": category,
                "length": len(keyword),
                "single_token": " " not in keyword,
            })

    rules.sort(key=lambda x: x["length"], reverse=True)
    return rules


MAPPING_RULES: List[Dict[str, Any]] = []


def categorize_content(text: Any) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return "Khác"

    for rule in MAPPING_RULES:
        keyword = rule["keyword"]

        if rule["single_token"] and len(keyword) <= 4:
            pattern = rf"(?<!\w){re.escape(keyword)}(?!\w)"
            if re.search(pattern, normalized):
                return rule["category"]
        else:
            if keyword in normalized:
                return rule["category"]

    return "Khác"


categorize_udf = F.udf(categorize_content, T.StringType())


# =========================
# SPARK
# =========================
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Customer360_Apply_Keyword_Mapping")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =========================
# ETL
# =========================
def read_input_data(spark: SparkSession) -> DataFrame:
    schema = T.StructType([
        T.StructField("user_id", T.StringType(), True),
        T.StructField("most_search_t6", T.StringType(), True),
        T.StructField("count_t6", T.StringType(), True),
        T.StructField("most_search_t7", T.StringType(), True),
        T.StructField("count_t7", T.StringType(), True),
    ])

    return (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .csv(str(INPUT_CSV_PATH))
    )


def transform_data(df: DataFrame) -> DataFrame:
    result_df = (
        df
        .withColumn("count_t6", F.col("count_t6").cast("int"))
        .withColumn("count_t7", F.col("count_t7").cast("int"))
        .withColumn("category_t6", categorize_udf(F.col("most_search_t6")))
        .withColumn("category_t7", categorize_udf(F.col("most_search_t7")))
        .withColumn(
            "keyword_changed_flag",
            F.when(F.col("most_search_t6") == F.col("most_search_t7"), F.lit("No"))
             .otherwise(F.lit("Yes"))
        )
        .withColumn(
            "category_shift_flag",
            F.when(F.col("category_t6") == F.col("category_t7"), F.lit("Unchanged"))
             .otherwise(F.lit("Changed"))
        )
        .withColumn(
            "category_transition",
            F.when(F.col("category_t6") == F.col("category_t7"), F.lit("Unchanged"))
             .otherwise(F.concat_ws(" -> ", F.col("category_t6"), F.col("category_t7")))
        )
        .select(
            "user_id",
            "most_search_t6",
            "count_t6",
            "category_t6",
            "most_search_t7",
            "count_t7",
            "category_t7",
            "keyword_changed_flag",
            "category_shift_flag",
            "category_transition",
        )
    )
    return result_df


def write_csv(df: DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(output_path, index=False, encoding="utf-8-sig")


def write_excel(df: DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()

    with pd.ExcelWriter(str(output_path), engine="openpyxl") as writer:
        pdf.to_excel(writer, index=False, sheet_name="search_trend")
        ws = writer.sheets["search_trend"]
        for column_cells in ws.columns:
            max_length = 0
            col_letter = column_cells[0].column_letter
            for cell in column_cells:
                cell_value = "" if cell.value is None else str(cell.value)
                max_length = max(max_length, len(cell_value))
            ws.column_dimensions[col_letter].width = min(max_length + 2, 50)


def build_transition_summary(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("category_transition")
        .agg(F.countDistinct("user_id").alias("total_users"))
        .orderBy(F.col("total_users").desc(), F.col("category_transition").asc())
    )


def main() -> None:
    global MAPPING_RULES
    MAPPING_RULES = load_mapping_rules(MAPPING_CSV_PATH)

    spark = build_spark()
    try:
        input_df = read_input_data(spark)
        output_df = transform_data(input_df)
        transition_df = build_transition_summary(output_df)

        write_csv(output_df, OUTPUT_CSV_PATH)
        write_excel(output_df, OUTPUT_XLSX_PATH)
        write_csv(transition_df, TRANSITION_SUMMARY_CSV)

        print(f"Đã xuất CSV : {OUTPUT_CSV_PATH}")
        print(f"Đã xuất XLSX: {OUTPUT_XLSX_PATH}")
        print(f"Đã xuất tóm tắt chuyển dịch category: {TRANSITION_SUMMARY_CSV}")
        output_df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
