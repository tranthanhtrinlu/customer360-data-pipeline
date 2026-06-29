# Customer360 ETL Data Lakehouse — Tài liệu trình bày phỏng vấn

> Mục tiêu của tài liệu: giúp bạn trình bày project từ A→Z ở **mức công nghệ và cơ chế**, không cần đụng tới class/hàm/code. Người nghe dù không biết lập trình vẫn hiểu bạn làm gì, ra kết quả gì, tại sao chọn cách đó.
> Bản này đã cập nhật theo kiến trúc **Data Lakehouse** mới: dùng **Delta Lake** theo **Medallion (Bronze–Silver–Gold)**, có cả **batch** lẫn **streaming near-realtime**.

---

## 0. Elevator pitch — nói trong 45 giây

> "Em xây một **ETL data lakehouse** để dựng **chân dung khách hàng 360 độ (Customer360)** cho một nền tảng truyền hình/giải trí số. Hệ thống lấy hai nguồn hành vi thô — **log tìm kiếm** và **log xem nội dung** — làm sạch, chuẩn hóa rồi tổng hợp thành một bảng duy nhất mô tả mỗi khách: họ **tìm gì**, **xem gì**, **sở thích có đổi theo thời gian không**, **hoạt động mạnh hay yếu**. Em dùng **PySpark** làm engine và **Delta Lake** làm định dạng bảng, tổ chức dữ liệu theo **Bronze–Silver–Gold (Medallion)**. Nhờ Delta, pipeline có **ACID, cập nhật incremental bằng MERGE/upsert, time-travel**, và dùng **chung một bảng cho cả batch lẫn streaming**. Hệ thống chạy **batch** là chính, kèm một luồng **streaming near-realtime** để nạp log mới liên tục. Đầu ra là các data mart (Delta + export CSV) cho BI và marketing dùng để phân khúc khách và gợi ý nội dung."

Đó là phần mở đầu. Phần dưới là toàn bộ chi tiết để bạn đào sâu khi bị hỏi tiếp.

---

## 1. Bối cảnh & bài toán business

**Customer360 là gì?** Là việc gom mọi mảnh dữ liệu về một khách hàng từ nhiều nguồn khác nhau lại thành **một hồ sơ thống nhất**. Thay vì "khách A ở hệ thống tìm kiếm" và "khách A ở hệ thống xem phim" là hai thực thể rời rạc, ta ghép lại để có cái nhìn toàn diện 360 độ.

**Dữ liệu đến từ một nền tảng OTT/truyền hình số** (dấu hiệu: CHANNEL = truyền hình, KPLUS = K+, FIMS/VOD = phim, CHILD = thiếu nhi, RELAX = giải trí). Hai nguồn hành vi:

1. **Log tìm kiếm (search log)** — mỗi lần người dùng gõ một từ khóa thì sinh một bản ghi: `user_id`, `keyword`, thời gian. Lưu dạng **Parquet** theo thư mục ngày.
2. **Log xem nội dung (content log)** — mỗi phiên xem sinh một bản ghi: hợp đồng thuê bao (`contract`), loại ứng dụng (`AppName`), **tổng thời lượng xem** (`TotalDuration`). Lưu dạng **JSON** theo ngày.

**Bài toán cần trả lời:**

- Mỗi khách quan tâm chủ đề gì (qua từ khóa)?
- Sở thích có **dịch chuyển** giữa tháng này và tháng sau không? (ví dụ T6 mê Anime, T7 chuyển sang Phim Hàn)
- Họ thực sự **xem** loại nội dung nào nhiều nhất, "khẩu vị" ra sao?
- Họ là khách **hoạt động mạnh hay yếu**?

**Giá trị business:** phân khúc khách hàng (segmentation), gợi ý nội dung (recommendation), cảnh báo rời bỏ (churn signal), và định hướng chiến lược nội dung (mua bản quyền theo thị hiếu đang dịch chuyển).

---

## 2. Bức tranh kiến trúc — Lakehouse theo Medallion (Bronze–Silver–Gold)

Đây là "xương sống" của project và là thứ nên vẽ ra giấy/bảng khi phỏng vấn.

```
                 ┌──────────────── RAW (nguồn) ────────────────┐
                 │  log_search  (parquet, T6 + T7)             │
                 │  log_content (json, theo ngày)              │
                 │  log_search_stream  (landing, file mới)     │
                 └─────────────────────────────────────────────┘
                                     │
            ┌────────── BATCH ───────┴──────── STREAMING (near-realtime)
            ▼                                                  ▼
   ┌─────────────────────┐                        ┌──────────────────────────┐
   │ BRONZE (Delta)      │  raw + metadata,       │ stream: landing parquet  │
   │  search_logs        │  append-only           │  ──▶ bronze/search_logs  │
   │  content_logs       │◄───────────────────────│  (file-source streaming) │
   └──────────┬──────────┘                        └──────────────────────────┘
              ▼                                    ┌──────────────────────────┐
   ┌─────────────────────┐                         │ stream: bronze ──▶ silver│
   │ SILVER (Delta)      │  đã làm sạch / chuẩn    │  (foreachBatch micro-    │
   │  search_clean       │  hóa, event-level       │   batch)                 │
   │  content_events     │◄────────────────────────└──────────────────────────┘
   └──────────┬──────────┘
              ▼
   ┌──────────────────────────────────────────────────┐
   │ GOLD (Delta, MERGE / upsert incremental)          │
   │  customer_search_trend    (key: user_id)          │
   │  customer_content_profile (key: contract)         │
   │  customer360_profile  ⭐  (key: user_id)          │
   │  _exports/mart_customer360_profile.csv (cho BI)   │
   └──────────────────────────────────────────────────┘
```

**Ý nghĩa 3 tầng (ẩn dụ nhà bếp):**

- **Bronze = nguyên liệu thô** vừa nhập chợ, chưa rửa. Là log gốc giữ nguyên, chỉ thêm metadata kỹ thuật (thời điểm nạp, file nguồn). Append-only.
- **Silver = nguyên liệu đã sơ chế** — rửa sạch, bỏ phần hỏng, cắt gọn. Là dữ liệu đã làm sạch/chuẩn hóa ở mức từng sự kiện (event-level).
- **Gold = món ăn đã bày ra đĩa** — sẵn sàng phục vụ. Là các data mart business dùng trực tiếp.

**Điểm khác biệt cốt lõi so với pipeline "thường": cả 3 tầng đều là bảng Delta Lake**, không phải CSV/Parquet rời rạc. Chính điều này biến nó thành **lakehouse đúng nghĩa** (xem mục 4.2).

**Tại sao chia 3 tầng?** Truy vết & sửa lỗi (sai thì lần ngược từng tầng), tái sử dụng (Silver sạch nuôi nhiều bài Gold khác nhau), và tách bạch trách nhiệm (mỗi tầng một việc, dễ bảo trì).

---

## 3. Luồng chạy — batch 7 bước + 2 luồng streaming

### 3.1. Pipeline BATCH (cách chạy chính, ra kết quả "đúng nhất")

Một orchestrator (`run_pipeline`) chạy **7 job nối tiếp** theo medallion, mỗi job là một Spark application riêng:

| Nhóm | Job | Việc làm | Ghi vào (Delta) |
|------|-----|----------|-----------------|
| Bronze | ingest_search_logs | Nạp parquet T6+T7 → bronze (append) | `bronze/search_logs` |
| Bronze | ingest_content_logs | Nạp JSON content → bronze (append) | `bronze/content_logs` |
| Silver | clean_search | Làm sạch keyword (event-level) | `silver/search_clean` |
| Silver | clean_content | Map AppName → content_type | `silver/content_events` |
| Gold | customer_search_trend | Top keyword + category + cờ dịch chuyển → **upsert** | `gold/customer_search_trend` |
| Gold | customer_content_profile | Pivot thời lượng + khẩu vị + hoạt động → **upsert** | `gold/customer_content_profile` |
| Gold | customer360_profile | Ghép qua bridge → **upsert** + export CSV | `gold/customer360_profile` |

Hai nhánh độc lập (tìm kiếm vs xem nội dung) hội tụ ở job cuối.

### 3.2. Luồng STREAMING (near-realtime, tùy chọn — để demo khả năng realtime)

Hai job thường trú tạo thành chuỗi `landing → bronze → silver`:

- **stream_search_logs**: Spark Structured Streaming theo dõi thư mục landing; có file parquet mới rơi vào là tự nạp vào `bronze/search_logs`.
- **stream_clean_search**: đọc `bronze/search_logs` như một **stream**, làm sạch theo từng micro-batch rồi ghi tiếp vào `silver/search_clean` (dùng `foreachBatch`).

Gold được làm tươi bằng cách chạy lại nhóm gold theo lịch.

---

## 4. Mổ xẻ từng công nghệ (mỗi công nghệ trả lời 4 câu hỏi)

Với mỗi công nghệ, trả lời theo khung: **(1) vai trò — (2) cơ chế/ẩn dụ — (3) tại sao chọn — (4) ưu/nhược.**

### 4.1. PySpark / Apache Spark — engine xử lý dữ liệu

**Vai trò:** "Động cơ" làm toàn bộ việc nặng — đọc log, làm sạch, nhóm, đếm, ghép bảng, ghi Delta. Mọi biến đổi dữ liệu đều chạy trên Spark.

**Cơ chế (ẩn dụ):** Đếm phiếu bầu cả nước. Một người đếm thì rất lâu (xử lý trên một máy). Spark **chia phiếu cho nhiều người đếm song song rồi cộng lại** — xử lý phân tán. Spark còn "lười thông minh": không làm ngay từng lệnh mà ghi nhớ cả kế hoạch (DAG), tối ưu lại rồi mới chạy một lần khi cần kết quả — gọi là **lazy evaluation**.

**Tại sao chọn Spark:**
- So với **Pandas** (nạp hết vào RAM một máy): log hành vi có thể hàng chục–trăm triệu dòng, Pandas hết bộ nhớ; Spark chia partition nên kham được dữ liệu lớn hơn RAM.
- So với **SQL thuần trên warehouse**: Spark linh hoạt hơn cho logic làm sạch text tiếng Việt phức tạp, và **tích hợp gốc với Delta + streaming**.
- So với **Flink** (chuyên realtime mili-giây): bài này chủ yếu **batch**, không cần độ trễ cực thấp, nên Spark là lựa chọn tự nhiên (và Spark cũng có Structured Streaming khi cần near-realtime).

**Ưu / nhược:**
- ✅ Mở rộng được (scale-out); hệ sinh thái SQL/ML/streaming; tối ưu tự động qua Catalyst.
- ❌ Khởi động nặng (chạy trên JVM), tốn warm-up — không hợp dữ liệu vài nghìn dòng.
- ❌ **Demo chạy single-node (local mode)** nên chưa khai thác hết phân tán — nhưng code viết chuẩn Spark DataFrame nên **đổi cấu hình cụm là chạy đa máy mà không viết lại logic.**

### 4.2. Delta Lake — định dạng bảng giao dịch (TRÁI TIM của bản nâng cấp)

**Vai trò:** Là **table format** cho cả 3 tầng Bronze/Silver/Gold. Đây chính là thứ biến project từ "pipeline batch + Parquet/CSV" thành **data lakehouse đúng nghĩa**.

**Cơ chế (ẩn dụ):** Delta Lake = **Parquet + một cuốn nhật ký giao dịch** (`_delta_log`). Parquet thuần giống một đống file dữ liệu rời; Delta gắn thêm cuốn sổ ghi "ai ghi cái gì, lúc nào, phiên bản mấy". Nhờ cuốn sổ này, bảng có được những thứ trước đây chỉ database mới có.

**Delta cho project này những gì (so với Parquet + CSV của bản cũ):**
- **ACID transaction:** ghi nguyên tử — không còn cảnh job chết giữa chừng để lại file hỏng/nửa vời.
- **MERGE / upsert (cập nhật incremental):** chỉ cập nhật bản ghi thay đổi thay vì ghi đè toàn bộ mỗi lần chạy (xem 4.6).
- **Time travel:** xem lại bảng ở phiên bản cũ (`VERSION AS OF`), tiện audit/rollback.
- **Schema evolution:** thêm cột không vỡ pipeline.
- **Unified batch + streaming:** **cùng một bảng** vừa làm nguồn cho batch, vừa làm nguồn/đích cho streaming — đây là nền tảng để có near-realtime.

**Tại sao Delta thay vì CSV trung gian (bản cũ):** CSV giữa các bước làm **mất schema/kiểu** (mọi thứ thành text), không nén, không ACID, không upsert, dễ vỡ với tiếng Việt có dấu/dấu phẩy. Delta sửa hết. *(Bản cũ chính là chỗ này: silver/gold dùng CSV trung gian → đã thay bằng Delta.)*

**Ưu / nhược:**
- ✅ Đem chất "database" (ACID, upsert, version) vào data lake giá rẻ trên file.
- ❌ Thêm một lớp phụ thuộc (`delta-spark`) phải khớp version với Spark; sinh file log/metadata cần dọn định kỳ (`OPTIMIZE`, `VACUUM`).
- *Họ hàng:* **Apache Iceberg**, **Apache Hudi** — cùng nhóm "lakehouse table format". Chọn Delta vì tích hợp PySpark mượt nhất và dễ chạy local.

### 4.3. Spark Structured Streaming — luồng near-realtime

**Vai trò:** Nạp log tìm kiếm mới **liên tục** từ thư mục landing vào bronze, rồi làm sạch tiếp sang silver, mà không cần chạy lại batch.

**Cơ chế (ẩn dụ):** Thay vì "mỗi tháng gom hết rồi xử lý một lần" (batch), streaming giống **băng chuyền**: dữ liệu tới tới đâu xử lý tới đó theo từng **micro-batch** (lô nhỏ vài giây). Spark dùng **checkpoint** (điểm lưu tiến độ) để nhớ đã xử lý file nào → **exactly-once**, không nạp trùng kể cả khi job khởi động lại.

**Tại sao "near-realtime" chứ không "true realtime":** Structured Streaming xử lý theo **micro-batch** (độ trễ giây), không phải từng-bản-ghi tức thời như event-streaming thuần (Flink/Kafka Streams). Với Customer360, độ trễ giây→phút là quá đủ.

**Hai chế độ trigger (đổi qua cấu hình):**
- *microbatch:* chạy liên tục, mỗi khoảng thời gian (vd 30s) quét dữ liệu mới → near-realtime.
- *availableNow:* xử lý hết dữ liệu hiện có rồi dừng → incremental batch theo lịch (giống cron) nhưng vẫn exactly-once nhờ checkpoint.

**Ưu / nhược:**
- ✅ Cùng API DataFrame với batch nên không phải học engine mới; tái dùng đúng logic làm sạch.
- ❌ Hiện dùng **file-source** (thư mục landing), chưa có message broker; muốn realtime "thật" cần thêm **Kafka** ở phía trước (đây là hướng phát triển).

### 4.4. Parquet — định dạng cột nằm bên dưới

**Vai trò:** Định dạng của **log tìm kiếm đầu vào**, và là **lớp lưu trữ vật lý bên dưới Delta** (Delta = Parquet + log).

**Cơ chế (ẩn dụ):** CSV lưu **theo dòng**; Parquet lưu **theo cột** — gom giá trị cùng cột cạnh nhau. Giống xếp sách theo thể loại thay vì theo người mượn: cần "trinh thám" thì lấy đúng kệ, khỏi lục cả thư viện.

**Tại sao Parquet:** đọc nhanh khi chỉ cần vài cột (project chỉ cần `user_id`, `keyword`), nén tốt, giữ schema/kiểu, hỗ trợ **predicate pushdown** (bỏ qua khối dữ liệu không khớp).

**Ưu / nhược:** ✅ lý tưởng cho phân tích đọc chọn cột trên dữ liệu lớn; ❌ không đọc bằng mắt thường, không hợp append từng dòng lẻ.

### 4.5. JSON — định dạng của log xem nội dung

**Vai trò:** Content log lưu JSON theo từng ngày.

**Cơ chế:** JSON **lồng nhau (nested)** kiểu xuất từ Elasticsearch (dữ liệu thật trong khối `_source`). Spark suy ra cấu trúc rồi **trải phẳng (flatten)** thành cột.

**Tại sao dùng JSON ở đây:** **không phải lựa chọn của em mà là định dạng gốc hệ thống xuất ra** — tình huống thực tế: data engineer hiếm khi chọn được format nguồn. Vì JSON nặng và parse chậm, pipeline **đọc JSON thô ở bronze rồi nhanh chóng chuyển sang Delta/Parquet** ở các tầng sau — đúng triết lý "thô để JSON, sạch để Delta".

### 4.6. MERGE / upsert — cập nhật incremental (mới so với bản cũ)

**Vai trò:** Các bảng **Gold** không còn ghi đè toàn bộ; chúng được **MERGE (upsert)** theo khóa: bản ghi đã có thì **update**, bản ghi mới thì **insert**.

**Cơ chế (ẩn dụ):** Giống cập nhật danh bạ điện thoại: tên đã có thì sửa số mới, tên chưa có thì thêm dòng — **không xóa cả danh bạ rồi nhập lại từ đầu**. Khóa upsert: `user_id` cho search_trend & customer360, `contract` cho content_profile.

**Tại sao quan trọng:** đây là thứ biến pipeline thành **incremental** thật sự — chạy lại chỉ động vào phần thay đổi, nhanh và an toàn. Đồng thời nó cho **idempotency** (chạy lại không nhân đôi dữ liệu) "miễn phí".

### 4.7. Window function — phép tính "top N trong mỗi nhóm"

**Vai trò:** (1) tìm **từ khóa được tìm nhiều nhất của mỗi user mỗi tháng** (top-1 mỗi nhóm); (2) đếm **số ngày hoạt động** của mỗi hợp đồng để phân loại High/Low.

**Cơ chế (ẩn dụ):** Xếp hạng trong từng lớp học. `group by` chỉ cho **con số tổng hợp** của cả lớp (điểm cao nhất 9.5) nhưng **không cho biết ai**. Window **chia bảng thành từng "khung"** (mỗi user-tháng một khung), sắp xếp trong khung theo số lần tìm, **đánh số thứ tự** rồi lấy hạng 1 — kèm **đầy đủ thông tin dòng**.

**Tại sao window thay vì group by:** group by gộp mất chi tiết dòng; để lấy "dòng quán quân kèm mọi thuộc tính trong mỗi nhóm", window (đánh số rồi lọc hạng 1) là cách chuẩn mực, thay cho self-join lòng vòng.

### 4.8. UDF — hàm tự định nghĩa để phân loại từ khóa thành category

**Vai trò:** Gán mỗi từ khóa vào một **danh mục** (Anime, Phim Hàn, Phim Trung, Thể thao, Tâm linh… hoặc "Khác") dựa trên bảng mapping từ khóa → category.

**Cơ chế:** Không khớp y hệt mà **chuẩn hóa** (bỏ dấu, viết thường, bỏ ký tự lạ), **ưu tiên khớp cụm dài trước** (để "phim hàn quốc" được nhận trước quy tắc chung chung), và với từ rất ngắn dùng **ranh giới từ** tránh khớp nhầm. Logic này phức tạp nên đóng gói thành **UDF**.

**Tradeoff (điểm cần nói rõ):**
- ✅ Biểu đạt được logic văn bản phức tạp mà SQL thuần khó viết.
- ❌ **UDF chậm:** Spark phải chuyển dữ liệu qua lại JVM↔Python từng dòng, và Catalyst **không nhìn vào trong UDF** (hộp đen).
- **Cải tiến nếu được hỏi:** đưa bảng mapping thành bảng nhỏ rồi **broadcast join**, hoặc dùng **hàm built-in / pandas UDF (vector hóa)** để nhanh hơn nhiều.

### 4.9. Bridge table — chìa khóa nối hai thế giới dữ liệu

**Vai trò:** **Mấu chốt kiến trúc** của Customer360. Log tìm kiếm định danh bằng `user_id`, log xem nội dung định danh bằng `contract`. **Hai nguồn không có khóa chung.** Bridge table là **bảng ánh xạ `user_id ↔ contract`** để ghép được.

**Đây là bài toán kinh điển: "Identity Resolution / Entity Resolution"** — luôn xuất hiện khi dựng Customer360 thật, vì mỗi hệ thống nội bộ có định danh riêng.

**Sự thật cần thành thật (xem mục Hạn chế):** trong demo bridge mới có vài dòng mẫu, nên đa số khách chưa ghép được sang content → các cột content trong file cuối hiện để trống (NULL).

### 4.10. CSV export + MySQL (serving layer, tùy chọn)

**CSV export:** sau khi tính xong, bảng gold cuối được **export thêm một file CSV phẳng** (`gold/_exports/`) cho công cụ BI/người dùng không đọc được Delta. Lưu ý: **đây là *đầu ra*, không còn là *định dạng trung gian*** như bản cũ.

**MySQL (tùy chọn, tắt mặc định):** mart có thể ghi vào **MySQL** qua JDBC để dashboard/app **truy vấn nhanh theo khóa**. File Delta/Parquet hợp lưu trữ & phân tích, nhưng DB quan hệ hợp tra cứu từng khách theo thời gian thực. *(Thay thế: PostgreSQL, hoặc ClickHouse/BigQuery nếu phân tích quy mô lớn.)*

### 4.11. Orchestrator (`run_pipeline`) — và định hướng Airflow

**Vai trò:** Một script điều phối chạy 7 job batch đúng thứ tự phụ thuộc, cho phép chạy từng nhóm (`--only gold`, `--from silver`). Mỗi bước là một tiến trình Spark sạch.

**Định hướng:** đây mới là điều phối "thủ công gọn". Bước tiếp theo là **Apache Airflow** để lập lịch tự động, quản lý phụ thuộc, retry khi lỗi và cảnh báo.

---

## 5. Luồng ETL chi tiết — đi qua từng bước (chỉ nói cơ chế)

### Bronze — Nạp dữ liệu thô vào lakehouse

- **Search:** quét thư mục lấy đúng các ngày T6 (`202206`) và T7 (`202207`), đọc parquet, **thêm metadata** (`month_label`, `ingest_ts`, `source_file`) rồi **append** vào `bronze/search_logs`. Không làm sạch ở đây — bronze giữ nguyên bản gốc.
- **Content:** đọc từng file JSON theo ngày, trải phẳng `_source.*`, gắn `event_date` + metadata, append vào `bronze/content_logs`.
- *(Streaming song song:* file parquet mới ở landing được luồng stream nạp thẳng vào `bronze/search_logs`.)*

> **Vì sao đáng nói:** bronze append-only + metadata nguồn là chuẩn lakehouse — luôn truy được "dòng này đến từ file nào, lúc nào".

### Silver — Làm sạch & chuẩn hóa (event-level)

**Search clean:** đọc `bronze/search_logs`, **làm sạch từ khóa** — đây là phần "bẩn nhất" và quan trọng nhất: bỏ dòng thiếu `user_id`/`keyword`; về chữ thường; gỡ URL; loại ký tự đặc biệt nhưng **giữ tiếng Việt có dấu**; nén khoảng trắng; loại từ khóa <2 ký tự, toàn số, và "rác" như `null`/`none`/`undefined`. Ghi vào `silver/search_clean` ở mức từng lượt tìm.

**Content clean:** đọc `bronze/content_logs`, **chuẩn hóa AppName → nhóm nội dung** (CHANNEL→Truyền hình, RELAX→Giải trí, CHILD→Thiếu nhi, FIMS/VOD→Phim truyện, KPLUS/SPORT→Thể thao), bỏ bản ghi rác (contract trống/"0", thời lượng thiếu). Ghi vào `silver/content_events`.

> Việc đếm/tổng hợp **không làm ở silver** — silver giữ chi tiết để nhiều bài Gold tái dùng.

### Gold — Tổng hợp thành data mart (Delta + upsert)

**1) customer_search_trend** (khóa `user_id`):
1. Đếm tần suất từ khóa theo (user, tháng); chọn **top-1 mỗi user mỗi tháng** (window), chỉ giữ từ khóa tìm ≥2 lần để khử nhiễu.
2. Ghép T6–T7 cạnh nhau theo `user_id` bằng **inner join** (chỉ giữ user có mặt **cả hai tháng** vì cần so sánh thay đổi).
3. **Gán category** cho keyword T6/T7 và tạo 3 tín hiệu insight: `keyword_changed_flag` (đổi từ khóa?), `category_shift_flag` (đổi thể loại?), `category_transition` ("Anime → Phim Hàn Quốc").
4. **MERGE upsert** vào `gold/customer_search_trend`.

**2) customer_content_profile** (khóa `contract`):
1. **Pivot** thời lượng theo 5 nhóm nội dung, theo (contract, ngày).
2. Tính `most_watch_type` (loại xem nhiều nhất), `taste_profile` (khẩu vị = mọi loại có xem), `active_days` & `activity_level` (xem >4 ngày → High).
3. **MERGE upsert** vào `gold/customer_content_profile`.

**3) customer360_profile** ⭐ (khóa `user_id`):
1. Lấy bảng search_trend làm **trục chính**, **left join** với bridge để gắn `contract`.
2. **Left join** tiếp với content_profile theo `contract`.
3. Gộp thành **một bảng duy nhất**, **MERGE upsert** vào `gold/customer360_profile`, rồi **export CSV** cho BI.

> Chọn **left join** (không inner) là chủ đích: **giữ toàn bộ khách có dữ liệu tìm kiếm**, kể cả khi chưa ghép được content — thà thông tin một nửa còn hơn loại bỏ khách.

---

## 6. Kết quả cuối cùng "đọc" ra điều gì?

Mỗi dòng bảng cuối là **một khách hàng**:

- **Tìm gì:** từ khóa top T6, T7 + số lần.
- **Thuộc thể loại nào:** category T6, T7.
- **Sở thích có đổi không:** đổi từ khóa? đổi thể loại? chuyển từ đâu sang đâu? → ví dụ thật: "Anime → Phim Hàn Quốc", "Anime → Khác", "Tâm linh → Khác".
- **Xem gì & mạnh không** (khi ghép được content): loại xem nhiều nhất, khẩu vị, mức độ hoạt động.

**Câu chuyện business:**
- Khách **đổi thể loại** → cơ hội gợi ý nội dung mới đúng hướng dịch chuyển.
- Khách **giữ thể loại + hoạt động cao** → trung thành, nên giữ chân/upsell.
- Khách **hoạt động thấp** → nguy cơ rời bỏ, cần tái kích hoạt.
- Bảng tóm tắt chuyển dịch cho thấy **dòng chảy thị hiếu của cả tệp** → định hướng mua bản quyền.

**Nhờ Delta, còn có thể:** xem lại bảng tháng trước bằng **time travel** để so sánh, và audit lịch sử thay đổi (`DESCRIBE HISTORY`).

---

## 7. Hạn chế đã biết & cách trả lời thẳng thắn

Chủ động nêu trước = ghi điểm trung thực và tư duy phản biện.

**1. Phần content trong file cuối đang trống (NULL).**
> "Bridge table `user_id ↔ contract` trong demo mới có vài dòng mẫu, trong khi search có hàng nghìn user. Hai nguồn dùng khóa định danh khác nhau, không có ánh xạ đầy đủ thì left join trả content rỗng. Đây chính là bài toán **identity resolution**; để đúng dữ liệu hoàn toàn cần bảng ánh xạ thật từ CRM/billing. Em thiết kế kiến trúc đúng để khi có bridge đầy đủ là ra kết quả ngay, nhưng thành thật là phần ghép content hiện mới ở mức khung."

**2. Khoảng thời gian hai nguồn không khớp.**
> "Search là T6–T7, content là đầu tháng 4 — dữ liệu mẫu nên thời gian chưa đồng bộ. Thực tế cần cùng cửa sổ thời gian để hồ sơ nhất quán."

**3. Chạy single-node, chưa thật sự phân tán.**
> "Spark chạy local một máy nên chưa khai thác cụm. Nhưng code chuẩn DataFrame nên chuyển sang YARN/K8s/Databricks chỉ là đổi cấu hình, không viết lại logic."

**4. Streaming là near-realtime, chưa có Kafka.**
> "Luồng streaming hiện dùng file-source (thư mục landing) và xử lý micro-batch độ trễ giây — đủ cho Customer360 nhưng chưa phải realtime từng-bản-ghi. Để realtime thật cần thêm **Kafka** trước landing. Ngoài ra `month_label` của luồng streaming lấy theo ngày hiện tại nên dữ liệu stream demo không tự rơi vào phân tích T6/T7 — nó minh họa khả năng nạp tươi, không thay thế batch."

**5. UDF phân loại — chậm trên dữ liệu lớn.**
> "UDF tiện cho logic văn bản nhưng là hộp đen với Catalyst và tốn chi phí Python↔JVM. Cải tiến: broadcast join bảng mapping hoặc pandas UDF."

**6. Export CSV bằng `coalesce(1)`.**
> "Em export một file CSV duy nhất cho tiện chia sẻ BI, việc này gom dữ liệu về một máy — chỉ an toàn vì gold đã tổng hợp nhỏ. Bản thân lakehouse vẫn lưu Delta multi-part; với khối lượng lớn em bỏ bước ép một file và để BI đọc thẳng Delta hoặc qua warehouse."

**7. Điều phối còn thủ công.**
> "7 bước batch chạy qua một orchestrator script, vẫn là kích hoạt tay. Bước tiếp theo là **Airflow** để lập lịch, quản lý phụ thuộc, retry và cảnh báo."

---

## 8. Ngân hàng câu hỏi phỏng vấn + câu trả lời mẫu (spoken)

**Q: ETL ở đây là gì, mô tả ngắn gọn?**
> "Extract — đọc log thô search (Parquet) và content (JSON) vào tầng Bronze (Delta). Transform — làm sạch ở Silver, rồi tổng hợp ở Gold: top search, gán category, pivot thời lượng, ghép qua bridge. Load — ghi các data mart dạng bảng Delta (kèm export CSV), tùy chọn nạp MySQL để phục vụ truy vấn."

**Q: Đây là data warehouse hay data lakehouse?**
> "**Lakehouse**. Dữ liệu lưu dạng file (Delta) trên storage rẻ theo medallion, schema-on-read, xử lý bằng Spark — đó là lake. Nhưng nhờ Delta có ACID, upsert, time-travel như warehouse — nên gọi là **lakehouse**: gộp ưu điểm của cả hai. MySQL chỉ là lớp serving tùy chọn ở cuối, không phải nơi xử lý chính."

**Q: Tại sao Delta Lake chứ không để Parquet thuần như bản trước?**
> "Parquet thuần không có giao dịch: job chết giữa chừng để lại file hỏng, muốn cập nhật phải ghi đè cả bảng, không có lịch sử phiên bản. Delta thêm transaction log nên có ACID, **MERGE/upsert incremental**, time-travel, và cho phép dùng **cùng một bảng cho batch lẫn streaming** — đó là điều kiện để có near-realtime."

**Q: Batch hay streaming? Tại sao?**
> "**Chủ yếu batch**, có thêm **streaming near-realtime**. Customer360 nhìn theo chu kỳ tháng/tuần nên batch vừa đơn giản vừa tiết kiệm; em dùng MERGE để batch chạy incremental. Luồng streaming (Structured Streaming, file-source) để minh họa nạp log mới liên tục độ trễ giây. Cần realtime thật thì thêm Kafka."

**Q: MERGE/upsert là gì, để làm gì?**
> "Là ghi 'có thì sửa, chưa có thì thêm' theo khóa — như cập nhật danh bạ, không xóa rồi nhập lại. Nhờ nó pipeline chạy **incremental** (chỉ động phần đổi) và **idempotent** (chạy lại không nhân đôi)."

**Q: Tại sao chia Bronze–Silver–Gold?**
> "Tách bạch trách nhiệm, dễ truy vết lỗi, tái sử dụng. Silver sạch một lần nuôi nhiều bài Gold. Sai thì lần ngược từng tầng để khoanh vùng."

**Q: Xử lý dữ liệu bẩn thế nào?**
> "Phần lớn công sức ở làm sạch từ khóa: chuẩn hóa chữ thường, gỡ URL/ký tự lạ nhưng giữ tiếng Việt có dấu, loại từ quá ngắn/toàn số/từ rác, đặt ngưỡng tần suất tối thiểu để bỏ nhiễu. Rác-vào-rác-ra nên em đầu tư mạnh khâu này, đặt ở tầng Silver."

**Q: Vì sao join T6–T7 dùng inner, còn Customer360 dùng left?**
> "Inner ở bước so sánh sở thích vì *cần* user có mặt cả hai tháng mới so được thay đổi. Left ở bước hợp nhất vì *muốn giữ* mọi khách có dữ liệu tìm kiếm, kể cả chưa ghép được content."

**Q: Window function khác group by?**
> "Group by gộp nhóm, mất chi tiết dòng, chỉ trả con số tổng hợp. Window giữ nguyên dòng nhưng tính *trong từng khung* — nhờ đó lấy được 'dòng quán quân kèm đủ thông tin' cho mỗi user-tháng."

**Q: Time travel của Delta dùng khi nào?**
> "Audit và rollback: so sánh hồ sơ khách giữa hai lần chạy, hoặc khôi phục về phiên bản tốt khi một lần chạy lỗi làm hỏng dữ liệu — `VERSION AS OF` / `DESCRIBE HISTORY`."

**Q: Nếu dữ liệu tăng 100 lần thì sao?**
> "Mở rộng Spark ra cụm. Bỏ nút thắt single-node: thay UDF bằng broadcast join, bỏ `coalesce(1)`/export một file, phân vùng theo ngày/tháng, dùng `OPTIMIZE` nén file nhỏ của Delta, và đưa Airflow vào điều phối."

**Q: Làm sao đảm bảo chạy lại không nhân đôi (idempotency)?**
> "Gold dùng **MERGE theo khóa** nên chạy lại cùng input cho cùng kết quả, không cộng dồn. Streaming dùng **checkpoint** đảm bảo exactly-once, không nạp lại file đã xử lý."

**Q: most_watch_type và taste_profile khác nhau ở đâu?**
> "most_watch_type là loại *xem nhiều nhất* (một giá trị, theo tổng thời lượng). taste_profile là *toàn bộ khẩu vị* — mọi loại từng xem. Một người most_watch là Phim Truyện nhưng taste có thể 'Phim Truyện-Thể Thao-Thiếu Nhi'."

**Q: Bridge table để làm gì và rủi ro?**
> "Nối hai hệ định danh — user_id (search) với contract (content). Rủi ro: ánh xạ thiếu/sai thì mất khách hoặc ghép nhầm. Đây là identity resolution, cần nguồn ánh xạ tin cậy từ CRM/billing trong thực tế."

---

## 9. Kịch bản nói (script) ~3 phút để mở đầu

> "Em xin trình bày project **Customer360 ETL Data Lakehouse**. Bối cảnh là một nền tảng truyền hình/giải trí số, có hai nguồn hành vi: log **tìm kiếm** và log **xem nội dung**. Mục tiêu là dựng **một hồ sơ thống nhất cho mỗi khách** để phục vụ phân khúc và gợi ý nội dung.
>
> Em tổ chức dữ liệu theo **Medallion ba tầng** và lưu bằng **Delta Lake**: tầng **Bronze** giữ log thô; **Silver** là dữ liệu đã làm sạch, chuẩn hóa; **Gold** là các bảng phân tích sẵn dùng. Điểm cốt lõi là cả ba tầng đều là bảng Delta — nên hệ thống có **ACID, cập nhật incremental bằng MERGE, time-travel**, và dùng **chung một bảng cho cả batch lẫn streaming**. Đây là điều biến nó thành **lakehouse đúng nghĩa** chứ không chỉ là một pipeline Parquet thông thường.
>
> Engine xử lý là **PySpark** — em chọn vì log hành vi có thể rất lớn, vượt khả năng một máy; Spark chia nhỏ dữ liệu, xử lý song song, tối ưu kế hoạch nhờ tính toán trì hoãn.
>
> Pipeline chính chạy **batch** qua bảy bước bronze→silver→gold. Tóm tắt: làm sạch log tìm kiếm và rút **từ khóa top mỗi khách theo tháng 6 và tháng 7**; **gán danh mục** và tạo tín hiệu cho thấy **sở thích có dịch chuyển không** — ví dụ Anime sang Phim Hàn; từ log xem, tổng hợp **thời lượng theo loại nội dung** để biết **xem gì nhiều, khẩu vị ra sao, hoạt động mạnh hay yếu**; cuối cùng **ghép hai nhánh** qua một **bridge table** nối định danh, ra **bảng Customer360**. Các bảng Gold được cập nhật bằng **MERGE/upsert** nên chạy lại chỉ động vào phần thay đổi.
>
> Ngoài batch, em có một luồng **streaming near-realtime**: Spark Structured Streaming theo dõi thư mục landing, có log parquet mới là tự nạp vào Bronze rồi làm sạch sang Silver, độ trễ cỡ giây.
>
> Kết quả là mỗi khách có một dòng mô tả đầy đủ: tìm gì, thể loại nào, sở thích có đổi không, xem gì và hoạt động ra sao — business dùng để phân nhóm, gợi ý nội dung, phát hiện nguy cơ rời bỏ.
>
> Em cũng xin nói thẳng **giới hạn hiện tại**: bridge table mới ở mức mẫu nên phần ghép content chưa đầy đủ; chạy single-node; streaming dùng file-source chưa có Kafka; điều phối còn thủ công. Hướng phát triển: hoàn thiện bảng ánh xạ định danh, thay UDF bằng broadcast join, thêm Kafka cho realtime thật, và đưa **Airflow** vào tự động điều phối."

---

### Phụ lục — bảng tra cứu nhanh "công nghệ → một câu chốt"

| Công nghệ | Một câu để chốt khi bị hỏi |
|-----------|----------------------------|
| **PySpark** | Engine xử lý phân tán, chia nhỏ chạy song song, tính toán trì hoãn để tối ưu. |
| **Delta Lake** | Parquet + transaction log → ACID, MERGE/upsert, time-travel, batch+streaming chung một bảng. Biến lake thành lakehouse. |
| **Structured Streaming** | Băng chuyền micro-batch độ trễ giây, checkpoint cho exactly-once → near-realtime. |
| **Parquet** | Lưu theo cột, nén tốt, giữ schema; là lớp file nằm dưới Delta. |
| **JSON** | Định dạng gốc của content log, lồng nhau; thô để JSON, sạch chuyển sang Delta. |
| **Medallion** | Ba tầng Bronze/Silver/Gold để tách bạch, truy vết và tái sử dụng. |
| **MERGE/upsert** | "Có thì sửa, chưa có thì thêm" theo khóa → incremental + idempotent. |
| **Window function** | Lấy "top-1 mỗi nhóm kèm đủ thông tin", thứ group by không làm trực tiếp được. |
| **UDF** | Logic phân loại text phức tạp; tradeoff là chậm, nên thay bằng broadcast join. |
| **Bridge table** | Giải identity resolution — nối user_id (search) với contract (content). |
| **CSV export / MySQL** | Lớp phục vụ: CSV cho BI đọc nhanh; MySQL cho app truy vấn theo khóa. |
| **Orchestrator → Airflow** | Hiện là script điều phối 7 bước; định hướng Airflow để lập lịch, retry, cảnh báo. |
