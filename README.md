# 🚀 Customer360 Interaction Analytics Pipeline with PySpark

## 📌 Overview
This project builds a **Customer360 analytics pipeline** from raw user search logs.

The system processes search data across multiple months (June & July) to:
- Identify **top search keywords per user**
- Classify keywords into **business categories**
- Detect **behavior changes over time**
- Build a **Customer360 profile**

---

## 🏗️ Pipeline

### 🔹 Final Output
`mart_customer360_profile.csv`

---

## ⚙️ Tech Stack
- Python
- PySpark
- Pandas
- OpenPyXL
- Parquet / CSV / XLSX
- SQL

---

## 📊 Main Outputs

- `mart_customer_search_trend.csv`
- `mart_customer_content_profile.csv`
- `mart_customer360_profile.csv` ⭐ (final dataset)

---

## 🔄 Run Order

```bash
python jobs/search_monthly_intent.py
python jobs/apply_keyword_mapping.py
python jobs/content_profile_etl.py
python jobs/build_customer360_profile.py

🔹 Search Trend Output
🔹 Customer360 Output
🔹 Customer360 Output
🔹 Architecture

🧠 Key Features
Cleaned noisy search keywords (Vietnamese text normalization)
Extracted top monthly search intent per user
Mapped search terms into business categories
Detected user behavior changes between months
Engineered content profile features:
most_watch_type
taste_profile
activity_level
active_days
Built a final customer-level Customer360 analytics mart

📂 Project Structure
Customer360_Project/
│
├── docs/
│   ├── architecture.png
│   ├── customer360-output.png
│   ├── run-success.png
│   └── search-trend-output.png
│
├── jobs/
│   ├── search_monthly_intent.py
│   ├── apply_keyword_mapping.py
│   ├── content_profile_etl.py
│   └── build_customer360_profile.py
│
├── reference/
│   ├── keyword_category_mapping.csv
│   └── customer_key_bridge.csv
│
├── sample_output/
│   ├── mart_customer_search_trend_sample.xlsx
│   └── mart_customer360_profile_sample.csv
│
├── sql/
│   └── customer360_tables.sql
│
├── requirements.txt
└── README.md

🚀 How to Run
1. Install dependencies
pip install -r requirements.txt

2. Run pipeline
python jobs/search_monthly_intent.py
python jobs/apply_keyword_mapping.py
python jobs/content_profile_etl.py
python jobs/build_customer360_profile.py

📈 Example Insights
Detect users changing interest from:
Anime → Phim Hàn Quốc
Giải trí → Tâm linh
Identify stable vs changing users
Segment users based on search behavior

💼 Resume Highlights
Built end-to-end Customer360 ETL pipeline using PySpark
Processed and cleaned large-scale search logs
Designed keyword classification and category mapping system
Detected user behavior changes across time
Developed customer-level analytics dataset for business insights

📎 Author
Trần Thanh Trí — Data Engineer (Entry Level)
Focus: Big Data / ETL / Data Pipeline
LinkedIn: https://linkedin.com/in/thanhtri0909
Email: tranthanhtri0147@example.com