# -*- coding: utf-8 -*-
r"""
SILVER (batch) - Làm sạch search logs từ bronze.

Đọc Delta `bronze/search_logs`, áp dụng làm sạch keyword (dùng chung với stream),
ghi đè (full refresh) vào Delta `silver/search_clean` ở mức EVENT-LEVEL
(mỗi dòng = một lượt search đã chuẩn hóa).

Việc đếm/top-keyword được làm ở tầng gold, để silver giữ dữ liệu chi tiết.

Chạy:  python -m lakehouse.silver.clean_search
"""

from __future__ import annotations

from pyspark.sql import functions as F

from .. import config
from ..common import clean_keyword, overwrite_delta, read_delta
from ..spark_session import get_spark


def main() -> None:
    spark = get_spark("C360_Silver_CleanSearch")
    try:
        bronze_df = read_delta(spark, config.BRONZE_SEARCH)
        # clean_keyword dùng month_label theo từng dòng (đã có sẵn ở bronze).
        clean_df = clean_keyword(bronze_df, F.col("month_label"))
        overwrite_delta(clean_df, config.SILVER_SEARCH_CLEAN, partition_by=["month_label"])

        print(f"[silver.search] Ghi {clean_df.count()} dòng -> {config.SILVER_SEARCH_CLEAN}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
