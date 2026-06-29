# Customer360 Delta Lakehouse — Kiến trúc & Hướng dẫn

Tài liệu này mô tả bản nâng cấp project Customer360 từ pipeline "lai" (Parquet +
CSV trung gian, batch thuần) lên **data lakehouse đúng nghĩa** dùng **Delta Lake**
theo **medallion architecture (Bronze → Silver → Gold)**, có cả **batch** và một
luồng **streaming near-realtime**, chạy trên **local filesystem**.

---

## 1. Vì sao đây mới là "lakehouse đúng nghĩa"?

Pipeline cũ đã có ý tưởng lakehouse (phân tầng silver/gold, dùng Parquet) nhưng
còn thiếu các đặc tính cốt lõi của một lakehouse hiện đại:

| Tiêu chí | Bản cũ | Bản nâng cấp |
|---|---|---|
| Table format | Parquet thuần + CSV | **Delta Lake** (transaction log `_delta_log`) |
| ACID transaction | Không | **Có** (ghi nguyên tử, không lỗi nửa chừng) |
| Trung gian giữa các bước | **CSV** (mất kiểu dữ liệu) | **Bảng Delta** (giữ schema/kiểu) |
| Cập nhật | Ghi đè toàn bộ | **MERGE / upsert incremental** |
| Time travel / audit | Không | **Có** (`VERSION AS OF`, lịch sử thay đổi) |
| Schema evolution | Thủ công | **Tự động** (`mergeSchema`) |
| Realtime | Không | **Structured Streaming** (file & Delta source) |

Delta Lake = Parquet + một transaction log. Chính cái log này mang lại ACID,
upsert, time travel và khả năng dùng cùng một bảng cho cả batch lẫn streaming —
đó là định nghĩa của lakehouse.

---

## 2. Sơ đồ luồng dữ liệu

```
                 ┌──────────────── RAW (nguồn) ────────────────┐
                 │  log_search/202206**, 202207**  (*.parquet) │
                 │  log_content/<YYYYMMDD>.json                │
                 │  log_search_stream/  (landing, file mới)    │
                 └─────────────────────────────────────────────┘
                                     │
            ┌────────── BATCH ───────┴──────── STREAMING ──────────┐
            ▼                                                       ▼
   ┌─────────────────┐                                  ┌────────────────────────┐
   │ BRONZE (Delta)  │  append-only, raw + metadata     │ stream_search_logs.py  │
   │ search_logs     │◄─────────────────────────────────│ landing parquet -> Δ   │
   │ content_logs    │                                  └────────────────────────┘
   └────────┬────────┘                                              │
            ▼                                          ┌────────────────────────┐
   ┌─────────────────┐                                 │ stream_clean_search.py │
   │ SILVER (Delta)  │  đã làm sạch / chuẩn hóa         │ bronze Δ -> silver Δ    │
   │ search_clean    │◄────────────────────────────────│ (foreachBatch)         │
   │ content_events  │                                 └────────────────────────┘
   └────────┬────────┘
            ▼
   ┌──────────────────────────────────────────────┐
   │ GOLD (Delta, MERGE upsert)                    │
   │  customer_search_trend   (key: user_id)       │
   │  customer_content_profile(key: contract)      │
   │  customer360_profile     (key: user_id) ⭐    │
   │  _exports/mart_customer360_profile.csv        │
   └──────────────────────────────────────────────┘
```

---

## 3. Cấu trúc thư mục code

```
lakehouse/
├── config.py                 # đường dẫn, tên bảng, business key, tham số
├── spark_session.py          # SparkSession đã bật Delta
├── common.py                 # clean_keyword, upsert_delta, export_csv...
├── run_pipeline.py           # orchestrator chạy toàn bộ batch
│
├── bronze/
│   ├── ingest_search_logs.py     # batch: parquet -> bronze Delta
│   ├── ingest_content_logs.py    # batch: json    -> bronze Delta
│   └── stream_search_logs.py     # STREAMING: landing -> bronze Delta
│
├── silver/
│   ├── clean_search.py           # batch: bronze -> silver (làm sạch)
│   ├── clean_content.py          # batch: bronze -> silver (categorize)
│   └── stream_clean_search.py    # STREAMING: bronze -> silver (foreachBatch)
│
└── gold/
    ├── keyword_mapping.py            # helper map keyword -> category
    ├── customer_search_trend.py      # mart trend (upsert)
    ├── customer_content_profile.py   # mart content profile (upsert)
    └── customer360_profile.py        # mart cuối Customer360 (upsert) + export CSV
```

---

## 4. Cài đặt & chạy

### 4.1. Cài dependencies
```bash
pip install -r requirements_lakehouse.txt
```
> `pyspark` và `delta-spark` phải đúng cặp version (xem file requirements).
> Cần Java 8/11/17 cho Spark.

### 4.2. Chạy toàn bộ pipeline batch
```bash
# chạy từ thư mục gốc project (chứa folder lakehouse/)
python -m lakehouse.run_pipeline

# hoặc chạy từng nhóm
python -m lakehouse.run_pipeline --only gold
python -m lakehouse.run_pipeline --from silver
```

### 4.3. Chạy từng bước thủ công
```bash
python -m lakehouse.bronze.ingest_search_logs
python -m lakehouse.silver.clean_search
python -m lakehouse.gold.customer_search_trend
python -m lakehouse.gold.customer360_profile
```

### 4.4. Cấu hình đường dẫn qua biến môi trường (tùy chọn)
```bash
set C360_LAKEHOUSE_ROOT=E:\DataSetDataEngineer\customer360_lakehouse
set C360_RAW_SEARCH=E:\DataSetDataEngineer\log_search
set C360_STREAM_LANDING=E:\DataSetDataEngineer\log_search_stream
```

---

## 5. Pipeline có chạy realtime không?

**Mặc định: chạy BATCH.** Lakehouse và realtime là hai thứ độc lập — bản nâng
cấp này hỗ trợ **cả hai**:

- **Batch incremental** (khuyến nghị cho phần lớn Customer360): chạy theo lịch,
  dùng MERGE nên chỉ cập nhật bản ghi thay đổi.
- **Streaming near-realtime** (demo khả năng realtime): hai job thường trú tạo
  thành chuỗi `landing → bronze → silver` cập nhật liên tục.

### Bật luồng near-realtime
Mở 2 terminal:
```bash
# Terminal 1: nạp file parquet mới ở landing vào bronze ngay khi xuất hiện
python -m lakehouse.bronze.stream_search_logs

# Terminal 2: làm sạch bronze -> silver theo luồng
python -m lakehouse.silver.stream_clean_search
```
Sau đó thả file `*.parquet` mới vào thư mục landing (`C360_STREAM_LANDING`).
Streaming sẽ tự phát hiện và xử lý. Cập nhật gold bằng cách chạy lại nhóm gold
theo lịch (vd mỗi vài phút):
```bash
python -m lakehouse.run_pipeline --only gold
```

### Hai chế độ trigger (đổi qua biến `C360_STREAM_MODE`)
- `microbatch` (mặc định): chạy liên tục, mỗi `C360_STREAM_INTERVAL` (vd "30 seconds")
  quét dữ liệu mới → **near-realtime**.
- `availableNow`: xử lý hết dữ liệu hiện có rồi dừng → **incremental batch** theo lịch
  (giống cron) nhưng vẫn dùng cơ chế checkpoint exactly-once.

> Vì sao "near-realtime" chứ không phải "true realtime"? Spark Structured
> Streaming xử lý theo **micro-batch** (độ trễ giây), không phải từng bản ghi
> tức thời như event-streaming thuần (Flink/Kafka Streams). Với Customer360, độ
> trễ giây→phút là quá đủ.

---

## 6. Vì sao bỏ CSV trung gian?

CSV giữa các Spark job có nhiều nhược điểm: mất schema/kiểu dữ liệu (mọi thứ
thành string), không nén tốt, không ACID, không hỗ trợ upsert, dễ hỏng với ký
tự tiếng Việt/dấu phẩy. Thay bằng bảng Delta giúp: giữ đúng kiểu, nhanh hơn,
ghi nguyên tử, và **upsert incremental** thay vì ghi đè toàn bộ mỗi lần chạy.

CSV vẫn được **export ở bước cuối** (`gold/_exports/`) cho công cụ BI không đọc
được Delta — nhưng nó là *đầu ra*, không còn là *định dạng trung gian*.

---

## 7. Tận dụng tính năng Delta (gợi ý vận hành)

```sql
-- Time travel: xem mart ở phiên bản trước
SELECT * FROM delta.`.../gold/customer360_profile` VERSION AS OF 3;

-- Lịch sử thay đổi (audit)
DESCRIBE HISTORY delta.`.../gold/customer360_profile`;
```
```python
# Nén file nhỏ + dọn rác định kỳ
spark.sql("OPTIMIZE delta.`.../gold/customer360_profile`")
spark.sql("VACUUM  delta.`.../gold/customer360_profile` RETAIN 168 HOURS")
```

---

## 8. Hướng phát triển tiếp

- Thêm **Kafka** trước landing để có nguồn realtime thực thụ.
- Đăng ký bảng vào **Unity Catalog / Hive Metastore** để query bằng tên thay vì path.
- Bổ sung **kiểm thử chất lượng dữ liệu** (Great Expectations / Delta constraints).
- Triển khai lên **object storage** (S3/ADLS/GCS) khi lên cloud — code gần như giữ nguyên,
  chỉ đổi `C360_LAKEHOUSE_ROOT` thành `s3a://...`.
```
