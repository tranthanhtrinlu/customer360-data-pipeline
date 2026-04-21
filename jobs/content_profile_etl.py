# -*- coding: utf-8 -*-
r"""
Customer360 - Content Profile ETL

Mục tiêu:
1) Đọc raw JSON content logs theo ngày.
2) Chuẩn hóa AppName -> content_type.
3) Tính tổng duration theo từng nhóm nội dung.
4) Tạo customer content profile: most_watch_type, taste_profile, activity_level.
5) Có thể ghi ra CSV/Parquet và tùy chọn đẩy MySQL.

Lưu ý:
- File này là bản refactor từ ETL_OLAP.py.
- Dùng khóa `contract` ở dữ liệu content. Nếu muốn join với user search,
  bạn cần bridge table user_id <-> contract ở bước 04.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F


INPUT_DIR = Path(r"E:\DataSetDataEngineer\log_content")
OUTPUT_DIR = Path(r"E:\DataSetDataEngineer\customer360_output\gold")
OUTPUT_CSV = OUTPUT_DIR / "mart_customer_content_profile.csv"
OUTPUT_PARQUET = OUTPUT_DIR / "mart_customer_content_profile.parquet"

START_DATE = "20220401"
END_DATE = "20220407"

MYSQL_ENABLED = False
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "etl_data"
MYSQL_TABLE = "mart_customer_content_profile"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Customer360_Content_Profile_ETL")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def convert_to_datevalue(string: str):
    return datetime.strptime(string, "%Y%m%d").date()


def convert_to_stringvalue(date_value):
    return date_value.strftime("%Y%m%d")


def generate_range_date(start_date: str, end_date: str) -> List[str]:
    start = convert_to_datevalue(start_date)
    end = convert_to_datevalue(end_date)

    date_list: List[str] = []
    current = start
    while current <= end:
        date_list.append(convert_to_stringvalue(current))
        current += timedelta(days=1)
    return date_list


def read_one_day_json(spark: SparkSession, input_dir: Path, day_str: str) -> DataFrame:
    json_path = input_dir / f"{day_str}.json"
    if not json_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file JSON: {json_path}")

    return spark.read.json(str(json_path)).select("_source.*")


def category_app_name(df: DataFrame) -> DataFrame:
    categorized = (
        df.withColumn(
            "content_type",
            F.when(F.col("AppName") == "CHANNEL", "Truyen Hinh")
             .when(F.col("AppName") == "RELAX", "Giai Tri")
             .when(F.col("AppName") == "CHILD", "Thieu Nhi")
             .when((F.col("AppName") == "FIMS") | (F.col("AppName") == "VOD"), "Phim Truyen")
             .when((F.col("AppName") == "KPLUS") | (F.col("AppName") == "SPORT"), "The Thao")
             .otherwise("Khac")
        )
        .select(
            F.col("Contract").cast("string").alias("contract"),
            "content_type",
            F.col("TotalDuration").cast("double").alias("total_duration"),
        )
        .filter(F.col("contract").isNotNull())
        .filter(F.col("contract") != "0")
        .filter(F.col("content_type") != "Khac")
        .filter(F.col("total_duration").isNotNull())
    )
    return categorized


def build_daily_profile(df: DataFrame, day_str: str) -> DataFrame:
    pivot_df = (
        df.groupBy("contract")
        .pivot("content_type", ["Giai Tri", "Phim Truyen", "The Thao", "Thieu Nhi", "Truyen Hinh"])
        .sum("total_duration")
        .fillna(0)
    )

    with_most_watch = (
        pivot_df.withColumn(
            "most_watch_value",
            F.greatest(
                F.col("Giai Tri"),
                F.col("Phim Truyen"),
                F.col("The Thao"),
                F.col("Thieu Nhi"),
                F.col("Truyen Hinh"),
            ),
        )
        .withColumn(
            "most_watch_type",
            F.when(F.col("most_watch_value") == F.col("Truyen Hinh"), "Truyen Hinh")
             .when(F.col("most_watch_value") == F.col("Phim Truyen"), "Phim Truyen")
             .when(F.col("most_watch_value") == F.col("The Thao"), "The Thao")
             .when(F.col("most_watch_value") == F.col("Thieu Nhi"), "Thieu Nhi")
             .otherwise("Giai Tri")
        )
        .drop("most_watch_value")
        .withColumn(
            "taste_profile",
            F.concat_ws(
                "-",
                F.when(F.col("Giai Tri") > 0, F.lit("Giai Tri")),
                F.when(F.col("Phim Truyen") > 0, F.lit("Phim Truyen")),
                F.when(F.col("The Thao") > 0, F.lit("The Thao")),
                F.when(F.col("Thieu Nhi") > 0, F.lit("Thieu Nhi")),
                F.when(F.col("Truyen Hinh") > 0, F.lit("Truyen Hinh")),
            ),
        )
        .withColumn("event_date", F.to_date(F.lit(day_str), "yyyyMMdd"))
    )

    return with_most_watch


def build_customer_content_profile(all_daily_profiles: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("contract")

    with_active = (
        all_daily_profiles
        .withColumn("active_days", F.count("event_date").over(window_spec))
        .withColumn(
            "activity_level",
            F.when(F.col("active_days") > 4, "High").otherwise("Low")
        )
    )

    final_df = (
        with_active.groupBy("contract")
        .agg(
            F.sum("Giai Tri").alias("total_giai_tri"),
            F.sum("Phim Truyen").alias("total_phim_truyen"),
            F.sum("The Thao").alias("total_the_thao"),
            F.sum("Thieu Nhi").alias("total_thieu_nhi"),
            F.sum("Truyen Hinh").alias("total_truyen_hinh"),
            F.first("most_watch_type", ignorenulls=True).alias("most_watch_type"),
            F.first("taste_profile", ignorenulls=True).alias("taste_profile"),
            F.first("activity_level", ignorenulls=True).alias("activity_level"),
            F.max("active_days").alias("active_days"),
        )
    )

    return final_df


def write_csv_single_file(df: DataFrame, output_path: Path) -> None:
    temp_dir = output_path.parent / f"tmp_{output_path.stem}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if temp_dir.exists():
        import shutil
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

    import shutil
    shutil.move(str(part_file), str(output_path))
    shutil.rmtree(temp_dir)


def write_to_mysql(df: DataFrame) -> None:
    jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", MYSQL_DRIVER)
        .option("dbtable", MYSQL_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .mode("overwrite")
        .save()
    )


def main() -> None:
    spark = build_spark()

    try:
        date_list = generate_range_date(START_DATE, END_DATE)
        all_profiles = []

        for day_str in date_list:
            print(f"ETL content file: {day_str}.json")
            raw_df = read_one_day_json(spark, INPUT_DIR, day_str)
            category_df = category_app_name(raw_df)
            daily_profile_df = build_daily_profile(category_df, day_str)
            all_profiles.append(daily_profile_df)

        result_df = all_profiles[0]
        for df in all_profiles[1:]:
            result_df = result_df.unionByName(df)

        final_df = build_customer_content_profile(result_df)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        if OUTPUT_PARQUET.exists():
            import shutil
            shutil.rmtree(OUTPUT_PARQUET)
        final_df.write.mode("overwrite").parquet(str(OUTPUT_PARQUET))
        write_csv_single_file(final_df, OUTPUT_CSV)

        if MYSQL_ENABLED:
            write_to_mysql(final_df)

        print(f"Đã xuất CSV    : {OUTPUT_CSV}")
        print(f"Đã xuất Parquet: {OUTPUT_PARQUET}")
        if MYSQL_ENABLED:
            print(f"Đã ghi MySQL   : {MYSQL_DATABASE}.{MYSQL_TABLE}")

        final_df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
