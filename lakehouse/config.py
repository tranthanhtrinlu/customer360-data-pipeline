# -*- coding: utf-8 -*-
r"""
Customer360 Lakehouse - Cấu hình tập trung.

Toàn bộ đường dẫn, tên bảng Delta, khóa nghiệp vụ (business key) và tham số
ETL được khai báo ở đây để các tầng bronze/silver/gold dùng chung.

Kiến trúc lưu trữ (medallion) trên local filesystem, tất cả là bảng Delta:

    <LAKEHOUSE_ROOT>/
    ├── bronze/                 # dữ liệu thô, append-only
    │   ├── search_logs
    │   └── content_logs
    ├── silver/                 # đã làm sạch / chuẩn hóa
    │   ├── search_clean
    │   └── content_events
    ├── gold/                   # marts phục vụ BI / Customer360
    │   ├── customer_search_trend
    │   ├── customer_content_profile
    │   └── customer360_profile
    └── _checkpoints/           # checkpoint cho Structured Streaming
"""

from __future__ import annotations

import os
from pathlib import Path


# =============================================================================
# ROOT PATHS
# =============================================================================
# Nguồn raw (giữ nguyên như project cũ).
RAW_SEARCH_DIR = Path(os.getenv("C360_RAW_SEARCH", r"E:\DataSetDataEngineer\log_search"))
RAW_CONTENT_DIR = Path(os.getenv("C360_RAW_CONTENT", r"E:\DataSetDataEngineer\log_content"))

# Thư mục "landing" cho luồng streaming (near-realtime): nơi các file parquet
# search log mới được đẩy vào liên tục. Job streaming sẽ tự phát hiện file mới.
STREAM_LANDING_DIR = Path(
    os.getenv("C360_STREAM_LANDING", r"E:\DataSetDataEngineer\log_search_stream")
)

# Gốc lakehouse - toàn bộ bảng Delta nằm dưới đây.
LAKEHOUSE_ROOT = Path(os.getenv("C360_LAKEHOUSE_ROOT", r"E:\DataSetDataEngineer\customer360_lakehouse"))

BRONZE_DIR = LAKEHOUSE_ROOT / "bronze"
SILVER_DIR = LAKEHOUSE_ROOT / "silver"
GOLD_DIR = LAKEHOUSE_ROOT / "gold"
CHECKPOINT_DIR = LAKEHOUSE_ROOT / "_checkpoints"

# Thư mục project (để đọc các file reference: mapping, bridge).
PROJECT_DIR = Path(__file__).resolve().parents[1]
REFERENCE_DIR = PROJECT_DIR / "reference"

MAPPING_CSV_PATH = REFERENCE_DIR / "keyword_category_mapping.csv"
BRIDGE_CSV_PATH = REFERENCE_DIR / "customer_key_bridge.csv"


# =============================================================================
# DELTA TABLE PATHS (mỗi bảng = 1 thư mục Delta)
# =============================================================================
# Bronze
BRONZE_SEARCH = BRONZE_DIR / "search_logs"
BRONZE_CONTENT = BRONZE_DIR / "content_logs"

# Silver
SILVER_SEARCH_CLEAN = SILVER_DIR / "search_clean"
SILVER_CONTENT_EVENTS = SILVER_DIR / "content_events"

# Gold
GOLD_SEARCH_TREND = GOLD_DIR / "customer_search_trend"
GOLD_CONTENT_PROFILE = GOLD_DIR / "customer_content_profile"
GOLD_CUSTOMER360 = GOLD_DIR / "customer360_profile"

# Export "phẳng" cho người dùng cuối / BI không đọc được Delta (tùy chọn).
EXPORT_DIR = GOLD_DIR / "_exports"


# =============================================================================
# BUSINESS KEYS dùng cho MERGE/upsert
# =============================================================================
# Khóa định danh bản ghi ở từng bảng - quyết định cách upsert (MERGE).
KEY_SEARCH_CLEAN = ["user_id", "month_label", "keyword"]
KEY_SEARCH_TREND = ["user_id"]
KEY_CONTENT_PROFILE = ["contract"]
KEY_CUSTOMER360 = ["user_id"]


# =============================================================================
# THAM SỐ NGHIỆP VỤ (giữ nguyên ngưỡng của pipeline cũ)
# =============================================================================
MONTH_PREFIXES = {
    "t6": "202206",
    "t7": "202207",
}

MIN_COUNT_PER_MONTH = 2
MIN_TOTAL_USERS_FOR_CONTENT = 5

CONTENT_START_DATE = "20220401"
CONTENT_END_DATE = "20220407"
HIGH_ACTIVITY_DAY_THRESHOLD = 4

# Regex làm sạch keyword.
URL_REGEX = r"(http|https|www\.)"
ONLY_DIGITS_REGEX = r"^[0-9]+$"
HAS_ALNUM_REGEX = r".*[a-zA-ZÀ-ỹà-ỹ0-9].*"
BAD_EXACT_KEYWORDS = ["null", "none", "na", "n/a", "undefined", "unk", "unknown"]


# =============================================================================
# STREAMING
# =============================================================================
# Trigger cho luồng near-realtime. Có 2 chế độ:
#   - "microbatch": chạy liên tục, mỗi `STREAM_TRIGGER_INTERVAL` xử lý 1 micro-batch.
#   - "availableNow": xử lý hết dữ liệu hiện có rồi dừng (giống batch incremental).
STREAM_MODE = os.getenv("C360_STREAM_MODE", "microbatch")
STREAM_TRIGGER_INTERVAL = os.getenv("C360_STREAM_INTERVAL", "30 seconds")


def checkpoint_path(name: str) -> str:
    """Trả về đường dẫn checkpoint cho 1 stream theo tên."""
    return str(CHECKPOINT_DIR / name)
