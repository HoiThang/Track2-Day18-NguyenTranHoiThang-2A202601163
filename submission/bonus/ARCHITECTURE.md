# Architecture Brief — LLM Observability Lakehouse @ 1B Requests/Ngày

**Topic A** · Tác giả: Nguyễn Trần Hồi Thắng · Ngày: 2026-08-18

---

## 1. Problem Statement

Một foundation-model API platform phục vụ 50+ tenants, sinh ra **1 tỉ requests/ngày** (~11.574 req/s trung bình, peak 30K req/s). Mỗi request log khoảng **5 KB** (prompt hash, completion metadata, token counts, latency, status, tenant_id, model_id, timestamps) → **5 TB raw/ngày, 150 TB/tháng**.

**Yêu cầu cứng:**

| Constraint | Target |
|---|---|
| Dashboard cost & latency theo tenant | Refresh ≤ 5 phút |
| Prompt/response đầy đủ cho incident review | Giữ 7 ngày |
| Aggregated metrics cho capacity planning | Giữ 1 năm |
| PII redaction | Trước khi bất kỳ analyst nào đọc |
| Tổng chi phí storage | **≤ $5.000/tháng** |

**Vì sao khó:** 5 TB/ngày × 30 ngày = 150 TB tại bất kỳ thời điểm nào chỉ cho raw retention. Với S3 Standard ($0.023/GB-tháng), riêng raw đã tốn ~$3.450/tháng — gần hết budget trước khi tính Silver/Gold. Bắt buộc phải lifecycle tiering rất kỹ, kết hợp Zstd compression rate cao trên JSON columns (thường đạt 8–12×), và aggressive cleanup.

---

## 2. Architecture Diagram

```
                            ┌──────────────────────────────────────────────────────────────┐
                            │                     CONTROL PLANE                            │
                            │         Apache Polaris (REST Catalog)                        │
                            │    ┌─────────┐  ┌─────────┐  ┌──────────┐                   │
                            │    │ iceberg  │  │  ACL /  │  │ OpenLine-│                   │
                            │    │ catalog  │  │  RBAC   │  │ age sink │                   │
                            │    └─────────┘  └─────────┘  └──────────┘                   │
                            └──────────────────────────────────────────────────────────────┘
                                       │              │              │
        ┌──────────────────────────────┼──────────────┼──────────────┼──────────────────┐
        │                              │   DATA PLANE │              │                  │
        │     ┌───────────┐    ┌───────┴───────┐  ┌───┴────────┐  ┌─┴──────────────┐   │
 Kafka  │     │ Flink CDC │    │   BRONZE      │  │   SILVER   │  │     GOLD       │   │
 ─────► │────►│ + PII     │───►│ raw_requests  │─►│ deduped +  │─►│ 5m agg by      │   │
 30K/s  │     │ tokenizer │    │ part: day(ts) │  │ enriched   │  │ tenant×model   │   │
        │     └───────────┘    │ Zstd, 256MB   │  │ part: day  │  │ part: day      │   │
        │                      │ target files   │  │ Z-ORDER:   │  │ ┌────────────┐ │   │
        │                      │               │  │ tenant_id  │  │ │ Trino /    │ │   │
        │                      │ 7-day TTL     │  │            │  │ │ dashboard  │ │   │
        │                      │ → S3 Express  │  │ 30-day     │  │ │ ≤5min lag  │ │   │
        │                      └───────────────┘  │ → S3 Std   │  │ └────────────┘ │   │
        │                                         └────────────┘  │ 365-day        │   │
        │    ┌──────────────────────────────────────────────────┐  │ → S3 IA        │   │
        │    │ LIFECYCLE ENGINE (S3 Intelligent-Tiering rules)  │  └────────────────┘   │
        │    │  0–7d: S3 Express One Zone (Bronze raw)          │                       │
        │    │  7d:   DELETE Bronze raw (prompt/response purge) │                       │
        │    │  0–30d: S3 Standard (Silver)                     │                       │
        │    │  30d+: S3 IA (Silver cold)                       │                       │
        │    │  0–365d: S3 IA (Gold aggregates)                 │                       │
        │    │  365d+: DELETE Gold                              │                       │
        │    └──────────────────────────────────────────────────┘                       │
        └──────────────────────────────────────────────────────────────────────────────┘

  Ingestion: Kafka → Flink (PII tokenize + schema validate) → Bronze (micro-batch 1 min)
  Query hot path: Trino on Gold (5-min refresh materialized view)
  Incident path: Trino on Silver (ad-hoc, PII de-tokenize chỉ qua audit-logged UDF)
```

---

## 3. Quyết Định Chính (≥ 5, kèm alternatives đã loại)

### 3.1 Table Format: Iceberg

**Chọn: Apache Iceberg.**

- **Loại Delta Lake** vì: Delta's `_delta_log/` là một chuỗi JSON commits tuyến tính — ở 1B rows/ngày với micro-batch mỗi phút, log này phình rất nhanh (~1.440 commits/ngày). Delta checkpoint mỗi 10 commits giúp phần nào, nhưng Iceberg's manifest-based approach phân tán metadata tốt hơn ở quy mô này. Iceberg cũng có **hidden partitioning** với `day(ts)` — analyst viết `WHERE ts > '2026-08-01'` mà không cần biết partition column, giảm query sai.
- **Loại Hudi** vì: Hudi's Copy-on-Write mode quá nặng cho write throughput 30K/s, còn Merge-on-Read thêm complexity cho compaction scheduler. Hudi mạnh về CDC/upsert nhưng use case này là append-only (log events), nên ACID semantics đơn giản của Iceberg đủ dùng. Hệ sinh thái Hudi cũng nhỏ hơn đáng kể so với Iceberg trong 2025–2026.

### 3.2 Catalog: Apache Polaris (REST Catalog)

**Chọn: Apache Polaris** (self-hosted, open-source REST catalog spec).

- **Loại Hive Metastore** vì: HMS dùng RDBMS backend (MySQL/Postgres), single-point-of-failure, không native RBAC, không hỗ trợ multi-table transactions. Ở quy mô 50+ tenants, HMS trở thành bottleneck khi nhiều engine (Trino, Flink, Spark) đồng thời đọc metadata.
- **Loại AWS Glue Catalog** vì: Glue Catalog bị vendor lock-in vào AWS; API throttle limit (số lượng `GetTable` calls/giây) dễ bị hit ở quy mô này. Nếu cần migrate sang GCP/Azure trong tương lai, Glue không portable. Polaris tuân thủ REST Catalog spec chuẩn — bất kỳ engine nào hỗ trợ Iceberg REST Catalog đều plug-in được.
- **Loại Databricks Unity Catalog** vì: Unity Catalog gắn chặt vào Databricks runtime, không phải open REST spec (mặc dù có UniForm). Khi dùng Trino/Flink ngoài Databricks, integration phức tạp hơn Polaris.

### 3.3 Partitioning: `day(ts)` + Z-ORDER by `tenant_id`

**Chọn: Hidden partition `day(ts)`, Z-ORDER clustering trên `tenant_id`.**

- **Loại `hour(ts)`** vì: 1B req/ngày ÷ 24h = ~42M req/hour → mỗi hourly partition vẫn rất lớn (~208 GB raw). Nhưng số lượng partition tăng 24× (365 × 24 = 8.760 partitions/năm vs. 365), gây metadata bloat và chậm `plan_files()`. Dashboard query thường filter theo ngày, không theo giờ, nên hourly partition không mang lại data skipping benefit tương xứng.
- **Loại `tenant_id` làm partition** vì: 50 tenants × 365 ngày = 18.250 partitions — không quá nhiều, nhưng tenant traffic rất skewed (top 3 tenants chiếm 60% traffic). Partition by tenant tạo ra small files cho long-tail tenants. Z-ORDER by `tenant_id` trên `day(ts)` partitions cho phép data skipping khi filter tenant mà không tạo partition imbalance.
- **Loại không partition (flat table)** vì: Ở 5 TB/ngày, full-table scan mỗi query sẽ đọc toàn bộ 150 TB cho 30-day range — hoàn toàn không thể chấp nhận. Partition pruning từ `day(ts)` giảm scan range xuống ≤ 5 TB cho single-day queries.

### 3.4 Compression: Zstd level 3

**Chọn: Zstd level 3** trên Parquet columns.

- **Loại Snappy** vì: Snappy nhanh hơn decompress ~15%, nhưng compression ratio chỉ đạt 2–3× trên JSON text. Zstd đạt 8–12× trên `raw_json` column (chiếm ~70% row size), tiết kiệm đáng kể storage cost. Với ràng buộc $5K/tháng, mỗi % compression thêm trực tiếp ảnh hưởng budget feasibility.
- **Loại Zstd level 9+** vì: Level 9 tăng compression chỉ thêm ~5–8% nhưng CPU encode chậm gấp 4×, gây bottleneck cho Flink ingestion pipeline ở 30K events/s. Level 3 là sweet spot: compression gần maximum, CPU cost chấp nhận được.

### 3.5 PII Handling: Tokenization tại Bronze landing

**Chọn: Format-preserving tokenization ngay trong Flink pipeline, trước khi ghi Bronze.**

- **Loại Column-level encryption (Parquet Modular Encryption)** vì: PME giữ PII dưới dạng ciphertext trong file — key rotation yêu cầu rewrite toàn bộ affected files (5 TB/ngày × 7 ngày = 35 TB mỗi lần rotate). Chi phí rewrite vượt quá budget. PME cũng chưa được hỗ trợ tốt trong Iceberg metadata layer.
- **Loại Row-level masking tại query time** vì: Masking tại query time nghĩa là PII vẫn nằm clear-text trên disk, vi phạm principle of least privilege. Một S3 misconfiguration hoặc backup leak sẽ expose PII. Tokenization at-rest đảm bảo PII không bao giờ tồn tại dạng plain trong lakehouse.

### 3.6 Ingestion Engine: Apache Flink (micro-batch 1 phút)

**Chọn: Apache Flink** streaming micro-batch, commit mỗi 60 giây vào Iceberg.

- **Loại Spark Structured Streaming** vì: Spark Structured Streaming trên Iceberg có latency tối thiểu ~2–5 phút do checkpoint overhead, không đáp ứng dashboard refresh ≤ 5 phút nếu tính cả downstream aggregation. Flink's native Iceberg sink hỗ trợ commit interval thấp hơn (1 phút), cho phép Gold refresh đạt 5-min SLA.
- **Loại trực tiếp Kafka Connect + S3 Sink** vì: Kafka Connect S3 Sink ghi raw JSON/Avro files — không tạo Iceberg commits, không có schema enforcement, không PII tokenization inline. Cần thêm một batch job để convert sang Iceberg, tăng complexity và latency.

---

## 4. Failure Modes (≥ 3)

### 4.1 Small-File Explosion từ Micro-Batch (Day 18: OPTIMIZE/Compaction)

**Kịch bản 3h sáng:** Flink commit mỗi phút → 1.440 commits/ngày, mỗi commit tạo 1–2 Parquet files (~3.5 GB/commit). Sau 7 ngày không compaction, Bronze có ~20.000 files. Trino query planner mất 30+ giây chỉ để list files, dashboard timeout.

**Detection:** Monitoring metric: `iceberg_table.current_snapshot.total_data_files`. Alert khi vượt 5.000 files cho một partition-day.

**Rollback/Fix:** Chạy compaction job (tương đương `OPTIMIZE` trong NB02/NB06): gộp files thành target 256 MB. Nếu compaction job chính fail, fallback sang Spark batch compaction. Iceberg's **snapshot isolation** đảm bảo readers không bị ảnh hưởng trong khi compaction chạy — đây là ACID guarantee mà data lake thuần không có.

### 4.2 PII Tokenization Key Bị Compromise

**Kịch bản 3h sáng:** Security team phát hiện tokenization key bị leak. Toàn bộ tokens trong Bronze/Silver có thể bị reverse.

**Detection:** Key rotation audit log + intrusion detection trên KMS (AWS CloudTrail).

**Rollback:** (1) Immediately rotate key trong KMS. (2) Re-tokenize Bronze 7 ngày gần nhất với key mới — bằng Flink batch job đọc từ Iceberg **time travel** (snapshot trước leak), tokenize lại, ghi overwrite. Iceberg time travel (tương tự NB03 `load_table().scan(snapshot_id=...)`) cho phép đọc exact state tại thời điểm trước compromise. (3) Silver/Gold downstream tự động refresh từ Bronze mới. (4) `expire_snapshots` để xoá snapshots chứa old tokens.

### 4.3 Late-Arriving Events Gây Sai Gold Aggregates

**Kịch bản 3h sáng:** Một tenant gateway gặp network partition, buffer 2 giờ events, sau đó flush tất cả cùng lúc. Gold table cho ngày hôm qua (đã "closed") bỗng thiếu data, dashboard hiển thị latency p95 sai.

**Detection:** Watermark monitoring: so sánh `max(event_ts)` vs `max(ingest_ts)` trong Bronze. Khi gap > 10 phút, trigger alert.

**Rollback:** (1) Late events vẫn được ghi vào Bronze theo `event_ts` partition (idempotent write với `request_id` dedup). (2) Gold aggregation job chạy lại cho affected partition-day, **MERGE** upsert (giống NB03 MERGE 100K rows) để update chứ không duplicate. (3) Iceberg **time travel** cho phép audit: so sánh Gold snapshot trước và sau recompute để chứng minh correction là chính xác.

### 4.4 Orphan Files Sau Failed Compaction

**Kịch bản 3h sáng:** Compaction job crash giữa chừng — đã ghi output files mới nhưng chưa commit Iceberg metadata. Files mồ côi chiếm storage nhưng không ai dùng.

**Detection:** Orphan file detection (tương tự NB06 Job 4): list S3 objects và diff với Iceberg manifest entries. Metric: `orphan_bytes > 10 GB` → alert.

**Rollback:** (1) Orphan sweep job xoá files không nằm trong bất kỳ snapshot/manifest nào. (2) Retry compaction. Iceberg's snapshot model đảm bảo: nếu commit chưa xảy ra, readers không bao giờ thấy partial output — **không cần rollback table state**, chỉ cần dọn files.

---

## 5. Ước Lượng Chi Phí Back-of-Envelope

### Storage

| Layer | Raw size/ngày | Compression (Zstd) | Retention | Avg stored | Tier | $/GB-tháng | $/tháng |
|---|---|---|---|---|---|---|---|
| Bronze | 5 TB | 8× → 625 GB | 7 ngày | 625×7/30 ≈ **146 GB** | S3 Express | $0.0160 | **$2.3** |
| Silver | 5 TB (enriched) | 10× → 500 GB | 30 ngày | **500 GB** | S3 Standard | $0.0230 | **$11.5** |
| Silver (cold) | — | — | 30→365d | 500×11 ≈ **5.5 TB** | S3 IA | $0.0125 | **$68.8** |
| Gold (agg) | ~2 GB/ngày | 5× → 0.4 GB | 365 ngày | 0.4×365 ≈ **146 GB** | S3 IA | $0.0125 | **$1.8** |
| Metadata (Iceberg) | — | — | — | ~5 GB | S3 Std | $0.0230 | **$0.1** |

**Tổng storage: ~$84.5/tháng**

### Compute

| Component | Instance/Service | Giờ/tháng | $/giờ | $/tháng |
|---|---|---|---|---|
| Flink (ingestion) | 4× m6i.2xlarge (8 vCPU, 32 GB) | 24×30×4 | $0.384 | **$1.106** |
| Trino (query) | 3× r6i.2xlarge (scale 0→3) | avg 12h/ngày×30×3 | $0.504 | **$545** |
| Compaction jobs | 2× m6i.4xlarge, 2h/ngày | 2×30×2 | $0.768 | **$92** |
| Polaris catalog | 1× m6i.xlarge | 24×30 | $0.192 | **$138** |
| Kafka (MSK) | 3 brokers, m5.large | managed | — | **$450** |

**Tổng compute: ~$2.331/tháng**

### S3 API costs

- PUT requests: 1.440 commits/ngày × 30 ngày × $5/1M = **$0.2/tháng** (negligible)
- GET requests: ~100K queries/ngày × 30 ngày × $0.4/1M = **$1.2/tháng**

### Tổng chi phí

| Category | $/tháng |
|---|---|
| Storage | $84.5 |
| Compute | $2,331 |
| S3 API | $1.4 |
| **Tổng** | **~$2,417/tháng** |

> **Storage budget constraint met:** $84.5/tháng << $5,000/tháng cap. Khoảng cách lớn nhờ Zstd compression 8–12× trên JSON payload và aggressive 7-day Bronze TTL. Nếu cần giảm tổng chi phí hơn nữa, Flink cluster có thể chạy trên Spot Instances (tiết kiệm ~60% compute).

---

## 6. MVP Một Tuần — Slice Nhỏ Nhất Shippable

### Tuần 1: Bronze Landing + PII Tokenization + Dashboard Proof

**Mục tiêu:** Chứng minh pipeline end-to-end hoạt động trên 1 tenant, 10K req/s.

| Ngày | Deliverable |
|---|---|
| 1–2 | Setup Iceberg catalog (Polaris local mode), tạo Bronze table với schema `(request_id, ts, tenant_id, model_id, raw_json_tokenized)`, partition `day(ts)`. Viết PII tokenization UDF trong Python. |
| 3–4 | Flink job: đọc từ Kafka topic, apply tokenization, ghi Bronze mỗi 60s. Verify `_delta_log` equivalent (Iceberg snapshots) có commit mỗi phút. |
| 5 | Silver dedup job: `MERGE`-based dedup trên `request_id`. Verify Silver < Bronze row count. |
| 6 | Gold aggregation: 5-min window aggregate `(tenant_id, model_id, date)` → `(p50_latency, p95_latency, total_cost, error_rate)`. Trino query chạy được. |
| 7 | Dashboard (Grafana/Superset) đọc Gold table. Demo: ingest 1M events → dashboard refresh trong 5 phút. Compaction chạy 1 lần, before/after file count reported. |

**Thành công nếu:** Dashboard hiển thị latency p95 cho 1 tenant, Gold table có ≥ 1 ngày dữ liệu, PII không xuất hiện dưới dạng plain-text trong bất kỳ layer nào, compaction giảm ≥ 10× file count.

---

*Tài liệu này là deliverable chính. PoC code tại `submission/bonus/poc/poc_pii_tokenize.py`.*
