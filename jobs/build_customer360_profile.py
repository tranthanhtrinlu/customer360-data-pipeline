# -*- coding: utf-8 -*-
r"""
Customer360 - Final Profile Builder

Mục tiêu:
1) Join mart_customer_search_trend với mart_customer_content_profile.
2) Do search dùng user_id, còn content dùng contract,
   file này dùng bridge table để nối 2 khóa.

Bridge file bắt buộc:
    E:\DataSetDataEngineer\customer360_project\reference\customer_key_bridge.csv

Cấu trúc bridge file:
    user_id,contract
    0151915,C001
    0231670,C002

Nếu bạn chưa có bridge table thì CHƯA thể build Customer360 cuối cùng một cách đúng dữ liệu.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


PROJECT_DIR = Path(__file__).resolve().parents[1]
BASE_OUTPUT_DIR = Path(r"E:\DataSetDataEngineer\customer360_output")

SEARCH_TREND_CSV = BASE_OUTPUT_DIR / "gold" / "mart_customer_search_trend.csv"
CONTENT_PROFILE_CSV = BASE_OUTPUT_DIR / "gold" / "mart_customer_content_profile.csv"
BRIDGE_CSV = PROJECT_DIR / "reference" / "customer_key_bridge.csv"

OUTPUT_CSV = BASE_OUTPUT_DIR / "gold" / "mart_customer360_profile.csv"
OUTPUT_PARQUET = BASE_OUTPUT_DIR / "gold" / "mart_customer360_profile.parquet"


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Customer360_Final_Profile_Builder")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_csv(spark: SparkSession, path: Path, schema: T.StructType | None = None) -> DataFrame:
    reader = (
        spark.read
        .option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
    )
    if schema is not None:
        reader = reader.schema(schema)
    return reader.csv(str(path))


def write_csv_single_file(df: DataFrame, output_path: Path) -> None:
    import os
    import shutil

    temp_dir = output_path.parent / f"tmp_{output_path.stem}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if output_path.exists():
        output_path.unlink()

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(temp_dir))

    part_file = None
    for file_name in os.listdir(temp_dir):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            part_file = temp_dir / file_name
            break

    if part_file is None:
        raise FileNotFoundError(f"Không tìm thấy part-file trong {temp_dir}")

    shutil.move(str(part_file), str(output_path))
    shutil.rmtree(temp_dir)


def main() -> None:
    if not BRIDGE_CSV.exists():
        raise FileNotFoundError(
            f"Không tìm thấy bridge file: {BRIDGE_CSV}\n"
            "Bạn cần tạo file mapping user_id <-> contract trước khi chạy bước 04."
        )

    spark = build_spark()
    try:
        search_df = read_csv(spark, SEARCH_TREND_CSV)
        content_df = read_csv(spark, CONTENT_PROFILE_CSV)
        bridge_df = read_csv(spark, BRIDGE_CSV)

        bridge_df = bridge_df.select(
            F.col("user_id").cast("string").alias("user_id"),
            F.col("contract").cast("string").alias("contract"),
        ).dropDuplicates(["user_id", "contract"])

        final_df = (
            search_df.alias("s")
            .join(bridge_df.alias("b"), on="user_id", how="left")
            .join(content_df.alias("c"), on="contract", how="left")
            .select(
                "user_id",
                "contract",
                "most_search_t6",
                "count_t6",
                "category_t6",
                "most_search_t7",
                "count_t7",
                "category_t7",
                "keyword_changed_flag",
                "category_shift_flag",
                "category_transition",
                "most_watch_type",
                "taste_profile",
                "activity_level",
                "active_days",
                "total_giai_tri",
                "total_phim_truyen",
                "total_the_thao",
                "total_thieu_nhi",
                "total_truyen_hinh",
            )
        )

        if OUTPUT_PARQUET.exists():
            import shutil
            shutil.rmtree(OUTPUT_PARQUET)
        final_df.write.mode("overwrite").parquet(str(OUTPUT_PARQUET))
        write_csv_single_file(final_df, OUTPUT_CSV)

        print(f"Đã xuất CSV    : {OUTPUT_CSV}")
        print(f"Đã xuất Parquet: {OUTPUT_PARQUET}")
        final_df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
