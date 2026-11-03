# Lộ trình triển khai

Thứ tự xây dựng data platform được mô tả trong [dataplatform-design.md](dataplatform-design.md).
Mỗi phase kết thúc bằng một thứ nhìn thấy được là đang chạy. Chỉ bắt đầu phase mới khi các mục
**Hoàn thành khi** của phase trước đã đạt. Phase đã xong ghi lại những gì đã làm và mục
**Rút ra được**. Bản tiếng Anh: [roadmap.md](roadmap.md).

| # | Phase | Trạng thái |
|---|---|---|
| 1 | Dọn sạch | Xong |
| 2 | Nguồn | Xong |
| 3 | CDC | Xong |
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

**Rút ra được:**
- Kiểm tra cấu hình có thực sự có hiệu lực, không chỉ xem service có chạy: job Spark qua REST chạy
  `local[*]` mà không báo lỗi, Polaris chạy in-memory nếu không cấu hình persistence, và `curl -s`
  vẫn trả exit 0 khi gặp HTTP 4xx/5xx.
- Thu thập log không được phụ thuộc vào thời điểm: discovery qua Docker API bỏ sót các job one-shot
  kết thúc dưới một giây; đọc file log json-file của Docker và cho mọi service chờ Alloy thì không
  sót dòng nào.
- Pin phiên bản và chuẩn bị cho việc upstream thay đổi: MinIO ngừng phát hành image; nhờ mọi engine
  dùng chung tên biến `S3_*` nên chuyển sang RustFS chỉ phải sửa một file.
- Bind mount từng file sẽ hỏng khi editor thay file mới: dùng `up -d --force-recreate`, không dùng
  `restart`.

## Phase 2 — Nguồn *(xong)*

**Mục tiêu:** một database OLTP Postgres thay đổi liên tục và sẵn sàng cho CDC.

- [x] Instance `source-db` (`compose/source.yml`): database `source-db`, port host 5433,
  `wal_level=logical`, `max_slot_wal_keep_size=1GB`.
- [x] `source/schema.sql`: schema `shop` gồm `users`, `orders`, `order_details` (design §3.1) —
  trạng thái dùng `varchar` + `CHECK`, `line_total` là cột thường, trigger cập nhật `updated_at`,
  `ON DELETE CASCADE` trên `order_details.order_id`.
- [x] Cấu hình CDC (design §3.2) nằm trong cùng script, áp dụng bằng job one-shot idempotent
  `source-db-setup`: `REPLICA IDENTITY FULL` cho ba bảng, publication `dbz_publication`, role
  `debezium` (`REPLICATION` + `SELECT` trên `shop`, mật khẩu lấy từ
  `SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD` qua biến của psql). Đã kiểm tra 2026-11-02: chạy hai lần
  (lần hai không thay đổi gì), `debezium` đăng nhập qua network bằng mật khẩu trong `.env`, tạo được
  logical slot và decode được thay đổi (UPDATE/DELETE mang đủ dòng cũ; xóa cascade ra hai DELETE
  trong cùng một transaction).
- [x] `source/generator.py` (Python 3.12 + Faker 40.44 + psycopg 3.3, `--rate` event/giây):
  tạo đơn hàng trong một transaction, chuyển trạng thái đơn, tạo/cập nhật/khóa user, thỉnh thoảng
  xóa đơn. Các hàm thuần có unit test (`tests/test_generator.py`, chạy trên host).
- [x] Service Compose `generator` (image `custom-generator:0.1.0`) trong profile `generator` (bật/tắt
  bằng tay), tốc độ lấy từ `GENERATOR_RATE` trong `.env`. Đã kiểm tra 2026-11-02: số dòng tăng liên
  tục (~8 đơn / 10 giây ở rate 2), mọi `total_amount` bằng tổng các dòng, không để transaction treo,
  `docker compose stop` dừng gọn trong ~1 giây, log lên Grafana.

**Hoàn thành khi:**
- `docker compose --profile generator up -d generator` → số dòng trong `shop.*` tăng liên tục khi
  xem bằng DataGrip / `psql`.
- `SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication'` liệt kê đủ ba bảng
  (cộng `public.debezium_heartbeat`, thêm ở phase 3).
- `\du debezium` có thuộc tính `Replication`.

**Rút ra được:**
- CDC chỉ thấy các transaction đã commit. Kết nối psycopg thiếu `autocommit=True` giữ một
  transaction ngầm suốt lúc chạy, nên Debezium sẽ không nhận được gì cho tới khi generator thoát.
  Phải biết mỗi transaction thực sự kết thúc ở đâu.
- Ranh giới transaction ở nguồn quyết định độ nhất quán ở downstream: một đơn hàng (`orders` +
  `order_details`) là một transaction nhưng nằm ở hai topic — chấp nhận có chủ đích, silver
  eventually consistent giữa các bảng.
- Cấu hình CDC nằm ở hai mức: instance (`wal_level`, giới hạn WAL của slot — vì vậy tách
  `source-db` riêng) và bảng (replica identity, publication). Không phải cột nào cũng đi qua WAL:
  generated column không được gửi trước PostgreSQL 18, nên `line_total` là cột thường.
- Script setup chạy lại mỗi lần `up` nên phải idempotent; Postgres không có `IF NOT EXISTS` cho role
  và publication (`SELECT format(...) WHERE NOT EXISTS ... \gexec`). Secret truyền vào bằng biến
  psql, và biến này không được thay thế bên trong `DO $$`.
- Kiểm tra đúng đường đi thật: trong container Postgres đăng nhập qua `localhost` được trust, sai
  mật khẩu vẫn vào được — phải test qua network (`-h source-db`).
- Python chạy làm PID 1 sẽ bỏ qua SIGTERM nếu không tự xử lý; phải nhìn tận mắt dữ liệu sinh ra chứ
  không chỉ đếm số dòng (Faker `vi_VN.first_name()` chỉ trả về "John"/"Jane").
- `timestamptz` lưu một thời điểm tuyệt đối; múi giờ chỉ là cách client hiển thị (DataGrip hiện +00
  trong khi server là `Asia/Ho_Chi_Minh` — sửa ở client, không sửa dữ liệu).

## Phase 3 — CDC *(xong)*

**Mục tiêu:** mọi thay đổi trong `source-db` đến được Kafka dưới dạng event Avro.

- [x] Image Kafka Connect `custom-connect:7.6.0` (`connect/Dockerfile`) dựa trên
  `confluentinc/cp-kafka-connect:7.6.0` + plugin Debezium Postgres **2.7.4.Final** (tarball từ Maven
  Central, có kiểm tra SHA-1). Image gốc chạy Java 11 nên loại Debezium 3.x (cần Java 17); 2.7 chạy
  được với Kafka Connect 3.x.
- [x] Module mới `compose/connect.yml`: service `kafka-connect` (chế độ distributed, một worker, REST
  8083 chỉ trong mạng nội bộ), các topic nội bộ `_connect-configs` / `_connect-offsets` /
  `_connect-status` với replication factor 1, converter mặc định của worker là Avro,
  `config.providers=env` (`EnvVarConfigProvider`) để cấu hình connector đọc
  `${env:SOURCE_POSTGRES_DB}` và `${env:SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD}` từ biến môi trường
  của container.
- [x] Job one-shot `connect-setup` (`connect/scripts/init.sh`): `PUT` mỗi file
  `connect/connectors/<name>.json` (hiện là `source-shop.json`, design §4.2) vào
  `/connectors/<name>/config` — tạo mới hoặc cập nhật, nên chạy lại vẫn an toàn — kiểm tra mọi mã
  HTTP và báo lỗi nếu connector và task không lên `RUNNING`.
- [x] Kafka UI (`kafbat/kafka-ui:v1.5.0`, http://localhost:8082, chỉ đọc) để xem topic, event đã
  giải mã Avro, schema và trạng thái connector trên trình duyệt; `connect/scripts/watch_events.py`
  in luồng event trực tiếp trong terminal, mỗi thay đổi một dòng.
- [x] Đã kiểm tra 2026-11-03: connector + task `RUNNING`; Schema Registry có 6 subject
  (compatibility `BACKWARD`); snapshot (`op = r`) khớp chính xác với nguồn (48 / 174 / 521 dòng); có
  event `c` / `u` / `d` liên tục khi bật generator; dựng lại trạng thái từng khóa từ toàn bộ event
  ra đúng y hệt ba bảng nguồn (khóa và `updated_at`); `total_amount` là Avro `decimal(18,2)`;
  timestamp là chuỗi ISO-8601 theo UTC; xóa cascade ra 4 delete `order_details` + 1 delete `orders`
  cùng một `txId`; dừng Connect thì `dbz_slot` inactive và lag tăng dần, khởi động lại thì đọc tiếp
  từ LSN đã lưu (bỏ qua snapshot, không mất event); chạy lại `connect-setup` báo "updated"; log của
  Connect lên Grafana.
- [x] Sửa theo review (2026-11-03): `init.sh` không còn chết im lặng khi connector chưa có trạng thái
  `RUNNING` nào (`grep` trả exit 1 dưới `pipefail`); heartbeat kèm `heartbeat.action.query` ghi vào
  `public.debezium_heartbeat` (được `source/schema.sql` thêm vào publication) — slot lag lúc idle từ
  kẹt ở ~2,7 MB xuống dưới 1 KB; `watch_events.py` giải mã decimal theo tên cột.

**Hoàn thành khi:**
- Connector ở trạng thái `RUNNING` (`GET /connectors/source-shop/status`).
- Schema Registry có các subject `src.shop.users-key/-value`, `src.shop.orders-*`,
  `src.shop.order_details-*`.
- `kafka-avro-console-consumer` trên `src.shop.orders` hiện các event snapshot (`op = r`), tiếp
  theo là các event `c` / `u` / `d` đang diễn ra.
- `pg_replication_slots` trên `source-db` cho thấy `dbz_slot` đang active.

**Rút ra được:**
- Kiểm tra runtime của image gốc trước khi chọn phiên bản plugin: `java -version` trong image quyết
  định chọn Debezium 2.7 thay vì 3.x.
- Giữ secret dạng `${env:...}` trong JSON của connector: Connect chỉ thay giá trị lúc chạy, nên mật
  khẩu không bao giờ nằm trong topic `_connect-configs` hay bị REST API trả về.
- `PUT /connectors/<name>/config` là cách đăng ký connector idempotent (`POST` lỗi ở lần chạy thứ
  hai).
- Job one-shot dùng lại image của một service sẽ thừa hưởng `HEALTHCHECK` của image đó — phải tắt
  đi.
- Số message trong Kafka là số event, không phải số dòng. Cách kiểm tra CDC thuyết phục là dựng lại
  trạng thái theo từng khóa từ event rồi so với nguồn.
- Event phản ánh đúng cách ứng dụng ghi dữ liệu: generator insert đơn rồi update tổng tiền trong
  cùng transaction, nên đơn mới ra `c` + `u` cùng một `txId`; xóa cascade trải trên hai topic với
  cùng một `txId`. Silver phải gộp event theo từng khóa trong mỗi transaction.
- Offset (LSN cuối) nằm trong Kafka (`_connect-offsets`), còn slot giữ WAL: khởi động lại thì đọc
  tiếp chính xác ("previous offset ... completed snapshot" → bỏ qua snapshot). Khi Connect dừng,
  slot inactive và WAL dồn lại; kể cả khi active, `confirmed_flush_lsn` vẫn chậm tối đa một lần
  flush offset (~60 giây), nên slot lag nhỏ nhưng không bằng 0.
- `timeout`/Ctrl+C trên `docker exec` chỉ giết client phía host: process bên trong container vẫn
  chạy (hai console consumer bị bỏ quên hiện thành group đang sống trong Kafka UI). Với client chạy
  lâu, dùng `docker run --rm`, vì nó chuyển tín hiệu vào container.
- `set -euo pipefail` + `grep | wc -l`: `grep` trả exit 1 khi không khớp gì, nên cả script thoát
  mà không in gì. Phải test cả nhánh "chưa sẵn sàng", không chỉ nhánh thành công.
- Chỉ bật heartbeat của Debezium thì slot không tiến: pgoutput không gửi gì cho transaction trên bảng
  ngoài publication, nên LSN trong offset đứng yên. Cần `heartbeat.action.query` ghi vào một bảng có
  trong publication. Đo (slot lag trước/sau) thay vì tin vào tên cấu hình.
- Đừng đoán kiểu dữ liệu từ nội dung: byte thô của decimal có thể in được (167.06 = `"AB"`).
- Kafka UI mặc định dùng serde `String` (Avro hiện thành byte thô), trừ khi gắn serde
  `SchemaRegistry` cho pattern của topic.
- `kafka-avro-console-consumer` in log ra stdout (lọc các dòng bắt đầu bằng `{`) và hiển thị decimal
  Avro dưới dạng byte thô — giải mã bằng `scale` trong schema.

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
- Khi kết thúc mỗi phase: cập nhật cột **Trạng thái**, đánh dấu các mục của phase theo những gì
  thực sự đã làm và đã kiểm tra, thêm danh sách **Rút ra được**, và cập nhật `CLAUDE.md`.
- Dùng Grafana (3000) → "Container logs" để debug bất kỳ service nào; log của job Spark nằm
  dưới `service=spark-job`.
