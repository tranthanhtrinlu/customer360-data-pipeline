# -*- coding: utf-8 -*-
r"""
SILVER (STREAMING / near-realtime) - Làm sạch search theo luồng từ bronze.

Đọc Delta `bronze/search_logs` dưới dạng STREAM (Delta hỗ trợ làm streaming
source), làm sạch keyword và ghi vào Delta `silver/search_clean`.

Dùng `foreachBatch` để thể hiện kỹ thuật upsert có thể tái sử dụng cho mọi sink:
mỗi micro-batch được làm sạch rồi append vào silver. (Có thể đổi sang MERGE nếu
muốn khử trùng lặp theo khóa.)

Đây là mắt xích thứ 2 của chuỗi realtime:
    landing parquet --stream--> bronze Delta --stream--> silver Delta

Chạy:  python -m lakehouse.silver.stream_clean_search
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .. import config
from ..common import append_delta, clean_keyword
from ..spark_session import get_spark


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Xử lý 1 micro-batch: làm sạch rồi append vào silver Delta."""
    if batch_df.rdd.isEmpty():
        return
    clean_df = clean_keyword(batch_df, F.col("month_label"))
    append_delta(clean_df, config.SILVER_SEARCH_CLEAN, partition_by=["month_label"])
    print(f"[silver.stream] batch {batch_id}: +{clean_df.count()} dòng sạch")


def build_trigger() -> dict:
    if config.STREAM_MODE == "availableNow":
        return {"availableNow": True}
    return {"processingTime": config.STREAM_TRIGGER_INTERVAL}


def main() -> None:
    spark = get_spark("C360_Silver_CleanSearch_Stream")
    try:
        bronze_stream = (
            spark.readStream
            .format("delta")
            .load(str(config.BRONZE_SEARCH))
        )

        query = (
            bronze_stream.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", config.checkpoint_path("silver_search_stream"))
            .trigger(**build_trigger())
            .start()
        )

        print(f"[silver.stream] bronze --> silver ({config.STREAM_MODE})")
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
