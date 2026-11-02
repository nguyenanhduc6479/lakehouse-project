# Lộ trình triển khai

Thứ tự xây dựng data platform được mô tả trong [dataplatform-design.md](dataplatform-design.md).
Mỗi phase kết thúc bằng một thứ nhìn thấy được là đang chạy. Chỉ bắt đầu phase mới khi các mục
**Hoàn thành khi** của phase trước đã đạt. Bản tiếng Anh: [roadmap.md](roadmap.md).

| # | Phase | Trạng thái |
|---|---|---|
| 1 | Dọn sạch | Xong |
| 2 | Nguồn | Đang làm |
| 3 | CDC | Chưa bắt đầu |
| 4 | Bronze | Chưa bắt đầu |
| 5 | Silver | Chưa bắt đầu |
| 6 | Gold | Chưa bắt đầu |
| 7 | Search + API | Chưa bắt đầu |
| 8 | Bảo trì + kịch bản kiểm thử | Chưa bắt đầu |

```
generator → source-db ─(2)─→ Debezium → Kafka ─(3)─→ Iceberg bronze ─(4)─→ silver ─(5)─→ gold ─(6)─→ Superset
                                                                              └──────────→ ES → API ─(7)─→ web page
```

---

## Phase 1 — Dọn sạch *(xong)*

**Mục tiêu:** bắt đầu lại, chỉ giữ phần hạ tầng dùng lại được.

- Đã xóa pipeline OHLCV vnstock cũ (Spark job, ingest, DAG, test) và các tài liệu cũ.
- Đã gỡ DataHub; chuyển Schema Registry sang `compose/infra.yml` (cần cho Avro).
- Giữ lại: Postgres, Kafka + Zookeeper, Schema Registry, Elasticsearch, object storage (nay là RustFS), Polaris, Spark,
  Trino, Airflow, Grafana/Loki/Alloy.
- Dựng lại từ volume trống (`docker compose down -v`); cả hai instance Postgres dùng `postgres:16`.
- Gia cố hạ tầng (2026-11-02): chuyển sang Docker Engine cài thẳng trong WSL; MinIO → RustFS
  (biến `S3_*`, một bucket `data-platform`); gỡ `spark-thrift` và dbt-spark; pin phiên bản mọi
  image; `TZ` cho mọi service chạy lâu dài; transaction log của Kafka ở RF 1; tắt event log của
  Spark và sửa `spark.master` (trước đó job submit qua REST chạy `local[*]`); Grafana hiện log của
  mọi container (file log json-file của Docker, khởi động chờ Alloy) và của các job Spark.
- Đã kiểm tra: đủ 21 service; mọi service gọi được nhau bằng tên trên network `lakehouse`
  (TCP + mức ứng dụng: các connection của Airflow, OAuth2 của Polaris, gửi/nhận Kafka, ghi Schema
  Registry); đã sửa `trino_conn` (thiếu user → HTTP 401).

## Phase 2 — Nguồn *(đang làm)*

**Mục tiêu:** một database OLTP Postgres thay đổi liên tục và sẵn sàng cho CDC.

- [x] Instance `source-db` (`compose/source.yml`): database `source-db`, port host 5433,
  `wal_level=logical`, `max_slot_wal_keep_size=1GB`.
- [ ] `source/schema.sql`: schema `shop` gồm `users`, `orders`, `order_details` (design §3.1) —
  trạng thái dùng `varchar` + `CHECK`, `line_total` là cột thường, trigger cập nhật `updated_at`,
  `ON DELETE CASCADE` trên `order_details.order_id`.
- [ ] Cấu hình CDC (design §3.2), áp dụng bằng một job one-shot idempotent `source-db-setup`:
  `REPLICA IDENTITY FULL` cho ba bảng, publication `dbz_publication`, role `debezium`
  (`REPLICATION` + `SELECT` trên `shop`, mật khẩu `SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD`).
- [ ] `generator.py` (Python + Faker + psycopg, `--rate` event/giây): tạo đơn hàng trong một
  transaction, chuyển trạng thái đơn, cập nhật/khóa user, thỉnh thoảng xóa đơn.
- [ ] Service Compose `generator` trong profile `generator` (bật/tắt bằng tay).

**Hoàn thành khi:**
- `docker compose --profile generator up -d generator` → số dòng trong `shop.*` tăng liên tục khi
  xem bằng DataGrip / `psql`.
- `SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication'` liệt kê đủ ba bảng.
- `\du debezium` có thuộc tính `Replication`.

## Phase 3 — CDC

**Mục tiêu:** mọi thay đổi trong `source-db` đến được Kafka dưới dạng event Avro.

- [ ] Image Kafka Connect dựa trên `confluentinc/cp-kafka-connect:7.6.0` + plugin Debezium Postgres
  (chọn phiên bản hỗ trợ Kafka 3.6).
- [ ] Module mới `compose/connect.yml`: service `kafka-connect` (REST 8083, chỉ trong mạng nội bộ),
  các topic nội bộ với replication factor 1, `config.providers=env` (`EnvVarConfigProvider`) để
  cấu hình connector đọc secret từ `.env`.
- [ ] Job one-shot `connect-setup`: đăng ký Debezium connector (design §4.2) qua Connect REST API —
  idempotent, kiểm tra mọi HTTP code.

**Hoàn thành khi:**
- Connector ở trạng thái `RUNNING` (`GET /connectors/source-shop/status`).
- Schema Registry có các subject `src.shop.users-key/-value`, `src.shop.orders-*`,
  `src.shop.order_details-*`.
- `kafka-avro-console-consumer` trên `src.shop.orders` hiện các event snapshot (`op = r`), tiếp
  theo là các event `c` / `u` / `d` đang diễn ra.
- `pg_replication_slots` trên `source-db` cho thấy `dbz_slot` đang active.

## Phase 4 — Bronze

**Mục tiêu:** event thay đổi được ghi thành các bảng Iceberg chỉ-append, query được từ Trino.

- [ ] Build file zip Iceberg Kafka Connect sink từ source của Iceberg (multi-stage build trong
  Dockerfile của Connect — sink không được phát hành sẵn). Chọn phiên bản Iceberg có REST client hỗ
  trợ thuộc tính OAuth2 `scope`.
- [ ] Đăng ký sink connector (design §5): SMT `InsertField` thêm `_topic` / `_partition` /
  `_offset` / `_ingested_at`, định tuyến tĩnh ba topic, tự tạo bảng + schema evolution, partition
  `day(_ingested_at)`, `format-version=2`, cấu hình Polaris + S3 (RustFS).

**Hoàn thành khi:**
- `SHOW TABLES FROM lakehouse.bronze` trong Trino → `users_cdc`, `orders_cdc`, `order_details_cdc`.
- `SELECT op, after.order_id, source.lsn FROM lakehouse.bronze.orders_cdc ORDER BY source.lsn DESC LIMIT 20`
  trả về các event gần nhất, và có dòng mới khoảng mỗi phút (commit interval).
- `SELECT * FROM lakehouse.bronze."orders_cdc$snapshots"` cho thấy mỗi lần commit là một snapshot
  `append`.
- Spark (Iceberg 1.5.2) đọc được bảng bronze qua `helper.get_spark`.

## Phase 5 — Silver

**Mục tiêu:** bản sao có lịch sử (SCD Type 2) của nguồn trong Postgres `data-platform`.

- [ ] DDL trong `data-platform`: schema `silver` và `staging`, các bảng `silver.users` /
  `silver.orders` / `silver.order_details` (design §6.3), partial unique index
  `(<key>) WHERE is_current`, bảng `silver.etl_checkpoint`.
- [ ] `spark/jobs/job_silver.py`: đọc incremental từ snapshot trong checkpoint, chuyển event thành
  phiên bản (mỗi khóa chỉ lấy event cuối trong một transaction, dùng `lead` để tính `valid_to`), ghi
  `staging.<table>` + `staging.batch`. Logic transform viết thành hàm thuần, có test pytest.
- [ ] Credential Postgres cho các service Spark (JDBC tới `data-platform`); Airflow đã có sẵn
  `postgres_conn` trỏ tới `data-platform`, nên không cần thêm connection mới.
- [ ] Task `silver_apply`: trong một transaction, thêm cột mới, chạy hai câu SCD2 (design §6.4),
  đẩy checkpoint lên.
- [ ] DAG `platform_pipeline` (`schedule=None`): `silver_spark` → `silver_apply`.

**Hoàn thành khi:**
- Trigger DAG thì `silver.*` có dữ liệu; một đơn hàng có nhiều phiên bản với `valid_from` /
  `valid_to` khớp nhau và đúng một bản `is_current`.
- Trigger lại khi không có event mới thì không có gì thay đổi (idempotent).
- Đơn hàng bị xóa kết thúc bằng một phiên bản hiện hành có `is_deleted = true`.
- Test Spark transform chạy qua trong container `spark-master`.

## Phase 6 — Gold

**Mục tiêu:** mô hình phân tích trong ClickHouse và dashboard trong Superset.

- [ ] Service ClickHouse (module mới), named collection trỏ tới `data-platform`.
- [ ] Thêm dbt-clickhouse vào image Airflow (kiểm tra cài được cùng constraints
  của Airflow 2.9.2); tạo `dbt/profiles.yml` (đã bị xóa cùng dbt-spark) và viết nội dung cho
  `dbt/dbt_project.yml`.
- [ ] Model (design §8): staging ephemeral đọc qua `postgresql()`; `dim_users`, `dim_date`,
  `fct_orders`, `fct_order_lines` (`append` + `ReplacingMergeTree(_silver_updated_at, is_deleted)`);
  mart doanh thu theo ngày và theo thành phố; macro `get_watermark`; dbt test.
- [ ] Thêm `dbt run` + `dbt test` vào `platform_pipeline`, chạy sau `silver_apply`.
- [ ] Service Superset (image + `clickhouse-connect`), kết nối tới ClickHouse, một dashboard.

**Hoàn thành khi:**
- `dbt run` và `dbt test` chạy qua trong DAG.
- `SELECT count() FROM gold.fct_orders FINAL` khớp với số đơn hiện hành, chưa bị xóa trong silver.
- Dashboard Superset hiện doanh thu theo ngày và theo thành phố.

## Phase 7 — Search + API

**Mục tiêu:** một trang web tìm kiếm đơn hàng, xem lịch sử và thống kê.

- [ ] Task `sync_es` trong `platform_pipeline` (chạy song song với dbt): mỗi đơn hàng là một
  document; dựng lại các đơn có chính nó, dòng chi tiết hoặc user thay đổi; xóa các đơn đã
  `is_deleted`; client `elasticsearch` bản 7.x.
- [ ] Service FastAPI: `GET /search?q=` (ES), `GET /orders/{id}` và `/orders/{id}/history`
  (silver), `GET /stats/...` (gold).
- [ ] Một trang HTML tĩnh do API phục vụ.

**Hoàn thành khi:**
- Tìm theo tên user hoặc tên sản phẩm ra được các đơn hàng tương ứng.
- Trang lịch sử của một đơn liệt kê các phiên bản SCD2 của nó.
- Số liệu trên trang khớp với dashboard Superset.

## Phase 8 — Bảo trì + kịch bản kiểm thử

**Mục tiêu:** chứng minh platform chạy đúng khi gặp sự cố và khi schema thay đổi.

- [ ] DAG `platform_maintenance` (`schedule=None`): compaction Iceberg / expire snapshot (không bao
  giờ vượt quá checkpoint của silver) / xóa file mồ côi; đối soát số dòng nguồn với silver; độ trễ
  replication slot.
- [ ] Chạy mọi kịch bản trong design §10 và ghi kết quả vào README.

**Hoàn thành khi:** mọi kịch bản trong design §10 đều có kết quả được ghi lại.

---

## Xuyên suốt các phase

- Pin mọi image và thư viện mới vào một phiên bản cụ thể.
- Mọi service mới phải có `logging: *default-logging` và phải chờ `alloy` (trực tiếp hoặc thông
  qua các service nó phụ thuộc), nếu không log của nó sẽ không có trên Grafana.
- Cập nhật `CLAUDE.md` và cột **Trạng thái** ở trên khi kết thúc mỗi phase.
- Dùng Grafana (3000) → "Container logs" để debug bất kỳ service nào; log của job Spark nằm
  dưới `service=spark-job`.
