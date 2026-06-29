# -*- coding: utf-8 -*-
r"""
BRONZE (STREAMING / near-realtime) - Ingest search logs theo luồng.

Đây là phần "realtime" của lakehouse. Spark Structured Streaming theo dõi thư
mục landing `STREAM_LANDING_DIR`; mỗi khi có file parquet MỚI rơi vào đó, nó tự
động được nạp vào bảng Delta `bronze/search_logs` mà không cần chạy lại batch.

Cơ chế:
  - readStream.format("parquet") = file-source streaming (micro-batch).
  - checkpointLocation = ghi nhớ file nào đã xử lý -> exactly-once, không nạp trùng.
  - Trigger:
      * microbatch  -> chạy liên tục, mỗi `STREAM_TRIGGER_INTERVAL` quét file mới.
      * availableNow -> nạp hết file đang có rồi dừng (incremental batch theo lịch).

Lưu ý: file-source streaming cần biết schema trước. Ở đây ta suy ra schema từ
dữ liệu raw đang có (hoặc từ landing) bằng một lần đọc batch.

Chạy:  python -m lakehouse.bronze.stream_search_logs
Dừng:  Ctrl+C (ở chế độ microbatch).
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .. import config
from ..spark_session import get_spark


def infer_search_schema(spark: SparkSession) -> StructType:
    """Suy ra schema parquet từ landing dir; nếu trống, lấy từ raw search dir."""
    landing = config.STREAM_LANDING_DIR
    if landing.exists() and any(landing.rglob("*.parquet")):
        return spark.read.parquet(str(landing)).schema

    # Fallback: lấy schema từ một folder raw bất kỳ.
    if config.RAW_SEARCH_DIR.exists():
        for name in sorted(os.listdir(config.RAW_SEARCH_DIR)):
            sample = config.RAW_SEARCH_DIR / name
            if sample.is_dir():
                return spark.read.parquet(str(sample)).schema

    raise FileNotFoundError(
        "Không suy ra được schema: hãy đảm bảo có dữ liệu parquet ở landing hoặc raw dir."
    )


def build_trigger() -> dict:
    if config.STREAM_MODE == "availableNow":
        return {"availableNow": True}
    return {"processingTime": config.STREAM_TRIGGER_INTERVAL}


def main() -> None:
    spark = get_spark("C360_Bronze_Search_Stream")
    try:
        config.STREAM_LANDING_DIR.mkdir(parents=True, exist_ok=True)
        schema = infer_search_schema(spark)

        stream_df = (
            spark.readStream
            .schema(schema)
            .option("maxFilesPerTrigger", 5)   # giới hạn để micro-batch ổn định
            .parquet(str(config.STREAM_LANDING_DIR))
            .withColumn("month_label", F.date_format(F.current_date(), "yyyyMM"))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        query = (
            stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", config.checkpoint_path("bronze_search_stream"))
            .partitionBy("month_label")
            .trigger(**build_trigger())
            .start(str(config.BRONZE_SEARCH))
        )

        print(f"[bronze.stream] Đang theo dõi: {config.STREAM_LANDING_DIR}")
        print(f"[bronze.stream] Ghi vào      : {config.BRONZE_SEARCH}")
        print(f"[bronze.stream] Trigger      : {config.STREAM_MODE} ({config.STREAM_TRIGGER_INTERVAL})")
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
