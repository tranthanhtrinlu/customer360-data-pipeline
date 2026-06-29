# -*- coding: utf-8 -*-
r"""
GOLD - Mart customer360_profile (Delta, upsert) - bảng cuối.

Join:
    gold/customer_search_trend (user_id)
      + reference/customer_key_bridge.csv (user_id <-> contract)
      + gold/customer_content_profile (contract)
-> MERGE (upsert) vào Delta `gold/customer360_profile` theo user_id.

Đồng thời export 1 file CSV phẳng (cho BI cũ) trong gold/_exports.

Chạy:  python -m lakehouse.gold.customer360_profile
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .. import config
from ..common import export_single_csv, read_delta, upsert_delta
from ..spark_session import get_spark

FINAL_COLS = [
    "user_id", "contract",
    "most_search_t6", "count_t6", "category_t6",
    "most_search_t7", "count_t7", "category_t7",
    "keyword_changed_flag", "category_shift_flag", "category_transition",
    "most_watch_type", "taste_profile", "activity_level", "active_days",
    "total_giai_tri", "total_phim_truyen", "total_the_thao",
    "total_thieu_nhi", "total_truyen_hinh",
]


def main() -> None:
    if not config.BRIDGE_CSV_PATH.exists():
        raise FileNotFoundError(
            f"Không tìm thấy bridge file: {config.BRIDGE_CSV_PATH}\n"
            "Cần file mapping user_id <-> contract trước khi build Customer360."
        )

    spark = get_spark("C360_Gold_Customer360")
    try:
        search_df = read_delta(spark, config.GOLD_SEARCH_TREND)
        content_df = read_delta(spark, config.GOLD_CONTENT_PROFILE)

        bridge_df = (
            spark.read.option("header", "true").csv(str(config.BRIDGE_CSV_PATH))
            .select(
                F.col("user_id").cast("string").alias("user_id"),
                F.col("contract").cast("string").alias("contract"),
            )
            .dropDuplicates(["user_id", "contract"])
        )

        final_df = (
            search_df.alias("s")
            .join(bridge_df.alias("b"), on="user_id", how="left")
            .join(content_df.alias("c"), on="contract", how="left")
            .select(*FINAL_COLS)
        )

        upsert_delta(spark, final_df, config.GOLD_CUSTOMER360, keys=config.KEY_CUSTOMER360)

        # Export CSV phẳng (tùy chọn) cho công cụ BI không đọc Delta.
        export_file = config.EXPORT_DIR / "mart_customer360_profile.csv"
        export_single_csv(read_delta(spark, config.GOLD_CUSTOMER360), export_file)

        print(f"[gold.customer360] upsert {final_df.count()} user -> {config.GOLD_CUSTOMER360}")
        print(f"[gold.customer360] export CSV -> {export_file}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
