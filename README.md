# 🏠 Customer360 ETL Data Lakehouse (PySpark + Delta Lake)

## 📌 Overview
Một **ETL data lakehouse** dựng **chân dung khách hàng 360 độ (Customer360)** cho nền tảng truyền hình/giải trí số (OTT).

Hệ thống lấy hai nguồn hành vi thô — **log tìm kiếm** và **log xem nội dung** — làm sạch, chuẩn hóa rồi tổng hợp thành một bảng duy nhất mô tả mỗi khách hàng:
- Họ **tìm gì** (top keyword theo tháng)
- Sở thích có **dịch chuyển theo thời gian** không (vd: Anime → Phim Hàn Quốc)
- Họ **xem gì** nhiều nhất, "khẩu vị" ra sao
- Mức độ **hoạt động** mạnh hay yếu

Dữ liệu được tổ chức theo **Medallion (Bronze–Silver–Gold)** và lưu bằng **Delta Lake**, nên pipeline có **ACID, cập nhật incremental (MERGE/upsert), time-travel**, và dùng **chung một bảng cho cả batch lẫn streaming**.

---

## Lakehouse
> **data lakehouse**:

| Tiêu chí | chi tiết |
|---|---|
| Table format | **Delta Lake** (transaction log) |
| ACID transaction | ✅ |
| Trung gian giữa các bước | **Bảng Delta** (giữ schema) |
| Cập nhật | **MERGE / upsert incremental** |
| Time travel / audit | ✅ (`VERSION AS OF`, `DESCRIBE HISTORY`) |
| Realtime | ✅ **Structured Streaming** (near-realtime) |

Chi tiết kiến trúc: [`docs/LAKEHOUSE_ARCHITECTURE.md`](docs/LAKEHOUSE_ARCHITECTURE.md)
Tài liệu trình bày: [`docs/INTERVIEW_PRESENTATION.md`](docs/INTERVIEW_PRESENTATION.md)

---

## 🏗️ Kiến trúc Medallion

```
   RAW                         BRONZE (Delta)        SILVER (Delta)         GOLD (Delta, upsert)
   log_search  (parquet) ───▶  search_logs   ───▶    search_clean    ───┐
   log_content (json)    ───▶  content_logs  ───▶    content_events  ───┤
   log_search_stream ──(stream)─▶ (near-realtime)                       │
                                                                        ▼
                                         customer_search_trend  (key: user_id)
                                         customer_content_profile (key: contract)
                                         customer360_profile ⭐  (key: user_id)
                                         _exports/mart_customer360_profile.csv
```

- **Bronze** — log thô, append-only, kèm metadata (ingest_ts, source_file).
- **Silver** — đã làm sạch / chuẩn hóa ở mức event-level.
- **Gold** — các data mart phục vụ BI, cập nhật bằng MERGE/upsert.

---

## ⚙️ Tech Stack
- **Apache Spark / PySpark** — engine xử lý phân tán
- **Delta Lake** — table format có ACID, upsert, time-travel
- **Spark Structured Streaming** — luồng near-realtime (file-source)
- Parquet / JSON (nguồn) • Pandas / OpenPyXL (export) • MySQL (serving, tùy chọn)
- Java 17 • Python 3.10/3.11

---

## 📂 Cấu trúc project

```
Customer360_Project/
├── lakehouse/                       # ⭐ pipeline lakehouse (mới)
│   ├── config.py                    # đường dẫn, tên bảng, business key
│   ├── spark_session.py             # SparkSession đã bật Delta
│   ├── common.py                    # clean_keyword, upsert_delta, export_csv
│   ├── run_pipeline.py              # orchestrator chạy toàn bộ batch
│   ├── bronze/
│   │   ├── ingest_search_logs.py    # batch: parquet → bronze Delta
│   │   ├── ingest_content_logs.py   # batch: json    → bronze Delta
│   │   └── stream_search_logs.py    # STREAMING: landing → bronze Delta
│   ├── silver/
│   │   ├── clean_search.py          # batch: bronze → silver (làm sạch)
│   │   ├── clean_content.py         # batch: bronze → silver (categorize)
│   │   └── stream_clean_search.py   # STREAMING: bronze → silver (foreachBatch)
│   └── gold/
│       ├── keyword_mapping.py       # helper map keyword → category
│       ├── customer_search_trend.py
│       ├── customer_content_profile.py
│       └── customer360_profile.py   # bảng Customer360 cuối + export CSV
│
├── jobs/                            # pipeline cũ (giữ lại để tham khảo)
├── reference/
│   ├── keyword_category_mapping.csv
│   └── customer_key_bridge.csv      # bridge user_id ↔ contract
├── sql/customer360_tables.sql
├── docs/
│   ├── LAKEHOUSE_ARCHITECTURE.md
│   └── INTERVIEW_PRESENTATION.md
├── requirements_lakehouse.txt       # dependencies bản lakehouse
├── requirements.txt                 # dependencies bản cũ
└── README.md
```

---


## 📊 Kết quả

Bảng cuối: `gold/customer360_profile` (Delta) + export `gold/_exports/mart_customer360_profile.csv`

Mỗi dòng là một khách hàng: top search T6/T7, category, cờ dịch chuyển sở thích (`category_transition`), loại xem nhiều nhất, khẩu vị, mức hoạt động.

**Ví dụ insight:** "Anime → Phim Hàn Quốc", "Giải trí → Tâm linh", phân biệt khách ổn định vs đổi sở thích.

**Ứng dụng business:** phân khúc khách hàng • gợi ý nội dung • cảnh báo churn • định hướng mua bản quyền theo thị hiếu.

---

## 🔄 Pipeline (7 bước batch)

| Nhóm | Job | Output (Delta) |
|------|-----|----------------|
| Bronze | ingest_search_logs / ingest_content_logs | bronze/search_logs, bronze/content_logs |
| Silver | clean_search / clean_content | silver/search_clean, silver/content_events |
| Gold | customer_search_trend | gold/customer_search_trend (upsert) |
| Gold | customer_content_profile | gold/customer_content_profile (upsert) |
| Gold | customer360_profile ⭐ | gold/customer360_profile (upsert) + CSV |

---

## 🧠 Key Features
- Làm sạch keyword nhiễu + chuẩn hóa tiếng Việt có dấu
- Rút top search intent mỗi user theo tháng (window function)
- Map keyword → category & phát hiện dịch chuyển sở thích giữa các tháng
- Hồ sơ xem nội dung: most_watch_type, taste_profile, activity_level, active_days
- Lakehouse Delta: ACID, MERGE/upsert incremental, time-travel
- Luồng streaming near-realtime (Spark Structured Streaming)

---

## ⚠️ Hạn chế đã biết
- **Bridge table** (`user_id ↔ contract`) mới ở mức mẫu → phần ghép content trong file cuối còn nhiều NULL (bài toán *identity resolution*).
- Chạy **single-node** (local mode) — code chuẩn Spark nên scale ra cụm chỉ cần đổi cấu hình.
- Streaming dùng **file-source**, chưa có Kafka → near-realtime micro-batch, chưa phải realtime từng-bản-ghi.
- Điều phối còn thủ công → định hướng tích hợp **Apache Airflow**.

---

## 💼 Resume Highlights
- Built an end-to-end **Customer360 ETL data lakehouse** using **PySpark + Delta Lake** (Medallion architecture)
- Implemented **incremental MERGE/upsert** and **near-realtime streaming** (Spark Structured Streaming)
- Designed Vietnamese text cleaning + keyword→category classification
- Detected user behavior/interest shifts across time for segmentation & recommendation

---

## 📎 Author
**Trần Thanh Trí** — Data Engineer
Focus: Big Data / ETL / Data Lakehouse / Data Pipeline
LinkedIn: https://linkedin.com/in/thanhtri0909
Email: tranthanhtri0147@example.com
