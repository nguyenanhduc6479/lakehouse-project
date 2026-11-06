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
| 4 | Bronze | Xong |
| 5 | Silver | Xong |
| 6 | Gold | Đang làm |
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
  flush offset, nên slot lag nhỏ nhưng không bằng 0. (Từ phase 4 worker flush mỗi 10 giây thay vì
  60 giây — `CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000` — nên slot lag lúc rảnh dưới ~0,5 KB và
  Debezium gửi lại ít event trùng hơn sau khi crash.)
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

## Phase 4 — Bronze *(xong)*

**Mục tiêu:** event thay đổi được ghi thành các bảng Iceberg chỉ-append, query được từ Trino.

Các bước (bước nào xong thì ghi kết quả ngay, để theo dõi được từng bước):

| Bước | Nội dung | Trạng thái |
|---|---|---|
| 4.1 | Chọn phiên bản Iceberg cho sink | Xong 2026-11-03 |
| 4.2 | Build Iceberg sink vào `custom-connect` (Dockerfile multi-stage) | Xong 2026-11-03 |
| 4.3 | Truyền credential Polaris + S3 cho `kafka-connect`; viết `connect/connectors/iceberg-bronze.json` (design §5) | Xong 2026-11-03 |
| 4.4 | Đăng ký qua `connect-setup`, kiểm tra ba bảng bronze trong Trino | Xong 2026-11-03 |
| 4.5 | Đọc bronze từ Spark (Iceberg 1.5.2) qua `helper.get_spark` | Xong 2026-11-03 |
| 4.6 | Tài liệu, trạng thái roadmap, bài học | Xong 2026-11-03 |

### 4.1 — Phiên bản Iceberg: **1.10.2** *(xong)*

Ràng buộc: worker `kafka-connect` chạy **Java 11** (`cp-kafka-connect:7.6.0`, Zulu 11.0.21); REST
client phải gửi được OAuth2 **`scope`** mà Polaris yêu cầu (`PRINCIPAL_ROLE:ALL`); bảng tạo ra phải
đọc được bằng Iceberg **1.5.2** của Spark.

Kết quả (kiểm tra 2026-11-03 trên Maven Central và tag `apache-iceberg-1.10.2`):

- **Mức Java** — đọc trực tiếp từ class file của từng jar đã phát hành:

  | Iceberg | Bytecode của `iceberg-core` / `iceberg-kafka-connect` |
  |---|---|
  | 1.6.1 | Java 8 |
  | 1.7.2 – **1.10.2** | **Java 11** |
  | 1.11.0, 1.12.0 (mới nhất) | Java 17 — sẽ lỗi trên worker (`UnsupportedClassVersionError`) |

  Vậy **1.10.2 là bản mới nhất chạy được trên Java 11**.
- **OAuth2 scope** — `org.apache.iceberg.rest.auth.OAuth2Properties` của 1.10.2 có `scope` (cùng với
  `credential`, `oauth2-server-uri`); sink chuyển mọi thuộc tính `iceberg.catalog.*` cho catalog, nên
  `iceberg.catalog.scope=PRINCIPAL_ROLE:ALL` tới được REST client.
- **Không có bản dựng sẵn** — `iceberg-kafka-connect` (jar của connector) có trên Maven Central,
  nhưng `iceberg-kafka-connect-runtime` (file zip plugin kèm đủ dependency: Parquet, AWS SDK, REST
  catalog…) thì không. Phải build từ source: `./gradlew -x test -x integrationTest
  :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip` → file zip nằm dưới
  `kafka-connect/kafka-connect-runtime/build/distributions/` (hai biến thể; bản không có Hive
  metastore client là đủ cho REST catalog).
- **Source** — `https://archive.apache.org/dist/iceberg/apache-iceberg-1.10.2/apache-iceberg-1.10.2.tar.gz`,
  SHA-512 `7eba5c85…65b9bd` (giá trị đầy đủ trong file `.sha512` bên cạnh; Dockerfile sẽ kiểm tra).
  Bản release không kèm `gradle-wrapper.jar`; `gradlew` của Iceberg tự tải về (Gradle 8.14.3).
- **JDK để build** — build chấp nhận JDK 11, 17 hoặc 21 và luôn biên dịch với `options.release = 11`,
  nên build trên image JDK 17 vẫn ra bytecode Java 11.
- **Format** — bảng được tự tạo với `format-version=2` (design §5), Iceberg 1.5.2 của Spark đọc được;
  1.10 chỉ ghi các tính năng v3 (deletion vector, row lineage) trên bảng v3.

Rủi ro để lại cho các bước sau:

- **Phiên bản Kafka API** — 1.10.2 được biên dịch với Kafka **3.9.1**; worker cung cấp Kafka **3.6**
  (`connect-api`/`kafka-clients` là compile-only, không đóng gói kèm). Dự kiến chạy được (Connect API
  tương thích ngược), nhưng chỉ chạy thật mới chắc → 4.4.
- **Commit transactional** — sink commit offset Kafka và snapshot Iceberg bằng transactional
  producer; transaction log của broker đã ở RF 1 (phase 1), nếu không sẽ lỗi khi chỉ có một broker
  → 4.4.
- **Polaris + RustFS từ sink** — S3FileIO với path-style và endpoint RustFS, không có credential
  vending; giống cấu hình Trino và Spark đang dùng → 4.4.

### 4.2 — Build sink vào `custom-connect` *(xong)*

`connect/Dockerfile` giờ là multi-stage:

1. **`iceberg-build`** (`eclipse-temurin:17.0.20_8-jdk-noble`): tải source release 1.10.2, kiểm tra
   SHA-512, chạy `./gradlew --no-daemon -x test -x integrationTest
   :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip` rồi giải nén file zip (bằng
   `jar xf` — image JDK không có `unzip`). Các file Gradle tải về nằm trong BuildKit cache mount
   (`/root/.gradle`), không nằm trong image.
2. **Connect worker** (`cp-kafka-connect:7.6.0`): Debezium như cũ, thêm `COPY --from=iceberg-build`
   thư mục plugin vào `/usr/share/confluent-hub-components/iceberg-kafka-connect-runtime-1.10.2/`
   (owner `appuser`). Mỗi thư mục plugin có class loader riêng, nên jar của Iceberg và Debezium
   không đụng nhau.

Kết quả (2026-11-03):

- Lần build đầu **~6,5 phút** (Gradle ~5,5 phút: tải dependency và biên dịch các module Iceberg cần
  thiết). Các lần build sau dùng lại layer cache của Docker — stage này chỉ chạy lại khi đổi tham
  số phiên bản/checksum. Image `custom-connect:7.6.0`: 3,26 GB (phần lớn là image gốc của
  Confluent).
- Plugin: 257 jar, 181 MB. **Không đóng gói `kafka-clients` / `connect-api`** → sink chạy trên thư
  viện Kafka 3.6 của worker (rủi ro về phiên bản API từ 4.1 vẫn còn, chờ 4.4).
- **Quét bytecode cả 257 jar** (class-file version cao nhất của từng jar, bỏ qua
  `META-INF/versions`): Java 3–8: 235 jar, Java 11: 15 jar, **trên Java 11: không có** — các
  dependency (AWS SDK 2.33, Parquet 1.16…) cũng chạy được trên Java 11 của worker.
- Khởi động lại worker với image mới: `GET /connector-plugins` liệt kê
  `org.apache.iceberg.connect.IcebergSinkConnector 1.10.2 sink` cạnh Debezium; log cho thấy plugin
  đã được nạp (kèm các transform `DebeziumTransform`, `KafkaMetadataTransform`…); `source-shop` vẫn
  `RUNNING` (giữ offset, không snapshot lại).

### 4.3 — Cấu hình sink và credential *(xong)*

- **`connect/connectors/iceberg-bronze.json`** (connector `iceberg-bronze`): đúng như design §5, thêm
  `tasks.max=1` và ghi rõ control topic `control-iceberg` (topic điều phối của sink; tự tạo ở lần
  chạy đầu). Mọi tên thuộc tính đã được đối chiếu với tài liệu của 1.10.2
  (`docs/docs/kafka-connect.md` ở tag `apache-iceberg-1.10.2`).
- **Secret vẫn ở dạng `${env:...}`**: `compose/connect.yml` giờ truyền `POLARIS_SECRET`,
  `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` từ `.env` vào `kafka-connect`; JSON chỉ
  ghi tên biến (`"iceberg.catalog.credential": "root:${env:POLARIS_SECRET}"` — tham chiếu provider
  dùng được bên trong một chuỗi dài hơn).
- **Thứ tự khởi động**: `connect-setup` giờ chờ thêm `polaris-setup` (catalog `lakehouse` +
  namespace `bronze`; bản thân nó đã chờ RustFS tạo bucket).

Kết quả (2026-11-03), chưa đăng ký connector:

- Bên trong `kafka-connect`: đủ năm biến; xin token OAuth2 của Polaris với
  `scope=PRINCIPAL_ROLE:ALL` → HTTP 200 có `access_token`; `GET ${S3_ENDPOINT}/health` → 200.
- `PUT /connector-plugins/IcebergSinkConnector/config/validate` với file JSON →
  **`error_count = 0`** trên 48 mục được kiểm tra (connector, converter và transform `InsertField`).
- `GET /connectors` vẫn chỉ có `source-shop`. Lưu ý: lần chạy `connect-setup` tiếp theo sẽ đăng ký
  mọi file `connect/connectors/*.json`, tức là khởi động sink — đó là bước 4.4.

### 4.4 — Đăng ký và kiểm tra trong Trino *(xong)*

`docker compose up -d --force-recreate connect-setup` → `Connector iceberg-bronze created` +
`RUNNING`. Kết quả (2026-11-03):

- **Bảng**: sink tự tạo `bronze.users_cdc` / `orders_cdc` / `order_details_cdc` trong Polaris khi
  nhận record đầu tiên (`SHOW TABLES FROM lakehouse.bronze`): `before`/`after`/`source` là row lồng
  nhau, `op`, `ts_ms`, bốn cột metadata `_`, Parquet, `format_version = 2`,
  `partitioning = ARRAY['day(_ingested_at)']`, location `s3://data-platform/bronze/<table>`.
- **Đọc bù**: group `connect-iceberg-bronze` chưa có offset nên sink đọc các topic từ đầu (gồm cả
  event snapshot `r`). Lần commit có dữ liệu đầu tiên ~95 giây sau khi khởi động (hai lần commit đầu
  hết thời gian chờ 30 giây trong lúc worker còn đang nạp bảng, và không commit gì).
- **Trạng thái ổn định** (bật generator): commit mỗi ~60 giây, mỗi lần xong trong dưới 2 giây và
  ghi cả ba bảng (`valid-through` = thời điểm commit); mỗi lần commit là một snapshot `append`; mỗi
  bảng mỗi lần commit một file Parquet ~23 KB. Khi các topic không có dữ liệu mới, commit hết thời
  gian chờ sau 30 giây và không commit gì — vô hại.
- **Exactly-once, kể cả khi restart**: restart `kafka-connect` giữa lúc đang ghi; sau đó, với từng
  bảng, số dòng bronze = số message Kafka = số giá trị `_offset` khác nhau, offset liên tục từ 0
  (users 139, orders 837, order_details 1359), consumer lag 0.
- **Bronze = nguồn**: dựng lại trạng thái hiện tại từ bronze (event cuối của mỗi khóa theo
  `source.lsn`, bỏ các khóa đã xóa) ra đúng y hệt các bảng nguồn — cùng khóa và `updated_at` cho
  89 users, 401 orders, 1237 dòng chi tiết.
- Ba rủi ro còn mở từ 4.1 đã được đóng: sink build với Kafka 3.9 chạy trên worker 3.6, commit
  transactional với một broker, OAuth2 scope của Polaris + RustFS từ sink — đều chạy được.
- Hai cảnh báo lúc khởi động đã hết nhờ khai báo rõ `iceberg.catalog.rest.auth.type=oauth2` và
  `iceberg.catalog.oauth2-server-uri` (Iceberg 1.10 tự suy ra cả hai).
- Kiểu dữ liệu: tiền là `decimal(38,2)` trong bronze (sink giữ scale, không giữ precision 18 của
  Avro); các cột thời gian trong `before`/`after` vẫn là chuỗi ISO-8601; `_ingested_at` là
  `timestamp(6) with time zone` = thời điểm của record Kafka.

### 4.5 — Đọc từ Spark *(xong)*

Một script tạm (`get_spark` + `spark.table("lakehouse.bronze.<table>")`, submit bằng
`/opt/spark/bin/spark-submit --py-files /opt/spark/jobs/helper.py`) chạy trên cluster
(`spark://spark-master:7077`) với Iceberg **1.5.2**: đọc được cả ba bảng, số dòng bằng Trino,
`format-version=2`, struct lồng nhau và giá trị `Decimal` nguyên vẹn, số `op` của orders là c 249 /
d 22 / r 174 / u 392. Bảng do Iceberg 1.10.2 ghi ở v2 đọc được bằng 1.5.2.

### 4.6 — Tài liệu *(xong)*

Design §4.3 (decimal trong bronze) và §5 (JSON cuối cùng của sink, cơ chế commit, consumer group,
ý nghĩa `_ingested_at`), `docs/services.md` ("Checking bronze"), `CLAUDE.md`, roadmap này.
Dọn dẹp: hai namespace Iceberg `silver` và `gold` (do `polaris-setup` tạo từ thiết kế cũ, khi mọi
tầng đều trên Iceberg; luôn trống) đã được xóa, và `polaris/scripts/init.sh` giờ chỉ tạo `bronze`
— silver nằm trong Postgres, gold trong ClickHouse, nên catalog chỉ hiện những gì thật sự nằm trên
Iceberg (`SHOW SCHEMAS FROM lakehouse` → `bronze`, `information_schema`).

**Hoàn thành khi** (cả phase):
- `SHOW TABLES FROM lakehouse.bronze` trong Trino → `users_cdc`, `orders_cdc`, `order_details_cdc`.
- `SELECT op, after.order_id, source.lsn FROM lakehouse.bronze.orders_cdc ORDER BY source.lsn DESC LIMIT 20`
  trả về các event gần nhất, và có dòng mới khoảng mỗi phút (commit interval).
- `SELECT * FROM lakehouse.bronze."orders_cdc$snapshots"` cho thấy mỗi lần commit là một snapshot
  `append`.
- Spark (Iceberg 1.5.2) đọc được bảng bronze qua `helper.get_spark`.

### Sau phase — sửa theo review (2026-11-03)

- **Nâng Iceberg trong Spark từ 1.5.2 lên 1.10.2** (`spark/Dockerfile`: `iceberg-spark-runtime-3.5_2.12`
  + `iceberg-aws-bundle`), cùng phiên bản với sink, trước khi phase 8 cho Spark bảo trì chính các bảng
  sink đang ghi. Đã kiểm tra trên cluster (Spark 3.5.1, Java 11; Iceberg 1.10.2 build với Spark 3.5.6
  nhưng chạy được trên 3.5.1): ba bảng bronze đọc ra đúng số dòng như trước; trên bảng tạm
  `bronze._spark_check` (đã xóa sau đó) — năm lần insert, `rewrite_data_files` (5 file → 1),
  `expire_snapshots` (xóa 5 data file, 5 manifest), `remove_orphan_files` chạy thử — đều chạy được.
  `remove_orphan_files` cần `prefix_listing => true`: mặc định nó liệt kê file qua Hadoop, mà ở đây
  Hadoop không có filesystem `s3://` (`No FileSystem for scheme "s3"`).
- Đã ghi vào tài liệu (design §5): `day(_ingested_at)` cắt ngày theo UTC, nên 0h–7h giờ Việt Nam nằm ở
  partition của ngày hôm trước; sink, Trino và Spark dùng chung principal `root` của Polaris (mỗi
  engine một principal là hướng mở rộng); `connect-setup` chờ `polaris-setup`, nên catalog hỏng cũng
  chặn việc đăng ký Debezium (chấp nhận; mỗi connector một job sẽ tách được hai bên).
- **Thêm bảng nguồn — runbook đã chạy thử từ đầu đến cuối** (`docs/add-source-table.md`): bảng tạm
  `shop.test_products` có sẵn 5 dòng đi qua schema.sql → hai connector → signal snapshot (5 event `r`)
  → insert/update/delete trực tiếp → `bronze.test_products_cdc` (8 dòng, before/after đúng), rồi được
  gỡ ra theo các bước gỡ (connector, bảng bronze, topic + subject, prefix S3 còn sót, publication +
  bảng) — không còn dấu vết, pipeline chính không ảnh hưởng (30 event mới vẫn vào bronze sau đó). Cần
  thêm: bảng signal `public.debezium_signal` (trong publication) + `signal.data.collection`, và SMT
  `Filter` (`dropSignals`) vì nếu không Debezium sẽ publish bảng signal ra
  `src.public.debezium_signal`.
- **Hai lỗi của platform lộ ra nhờ bài test, đã sửa:**
  - *Token OAuth2*: Trino báo `Not authorized` với mọi lệnh gọi catalog. Access log của Polaris cho
    thấy Trino xin token lúc 16:46:40, làm mới một lần lúc 17:41:40, rồi không làm mới nữa, và bị
    HTTP 401 từ 18:41:46; sink cũng y hệt (16:25:19 → 17:20:19 → không còn gì). Polaris chấp nhận yêu
    cầu làm mới (token exchange trả token mới 1 giờ có `expires_in`), nên lỗi nằm ở việc client ngừng
    hẹn lịch làm mới. Sửa: token của Polaris có hạn 30 ngày (`max-token-generation=P30D`).
  - *Sink lúc rảnh*: sau khi khởi động lại, consumer control của sink phải join lại group mỗi phút và
    worker không bao giờ trả lời `START_COMMIT` khi các topic rảnh, nên 8 record test nằm chờ không
    được commit. Khi rảnh, Connect chỉ gọi `put()` (nơi sink đọc control topic) mỗi
    `offset.flush.interval.ms` một lần — 60 giây, lâu hơn session 45 giây. Sửa:
    `CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000`; các record đang chờ được commit ~65 giây sau mà không
    cần dữ liệu mới, và các lần commit lúc rảnh giờ xong trong ~15 giây thay vì hết thời gian chờ.
- **Sửa theo review (cùng ngày):**
  - *Khóa ký token cố định*: broker mặc định `rsa-key-pair` của Polaris (không có file khóa) sinh khóa
    mới mỗi lần khởi động, nên restart Polaris làm mọi token mất hiệu lực cùng lúc — task của sink
    thành `FAILED` và Trino báo `Not authorized` cho tới khi restart bằng tay (đã tái hiện). Polaris
    giờ dùng `token-broker.type=symmetric-key` với `symmetric-key.secret=${POLARIS_TOKEN_SIGNING_KEY}`
    (biến mới trong `.env`; tên thuộc tính đọc từ jar của 1.5.0). Đã kiểm tra: token là HS256; sau khi
    restart Polaris, Trino vẫn trả lời và sink commit 20 event mới với 0 lỗi.
  - *Độ dài id của signal*: `debezium_signal.id` là `varchar(42)` và id `snapshot-<bảng>-<timestamp>`
    trong runbook bị tràn với tên bảng dài hơn 18 ký tự. Id giờ là `snapshot-<timestamp tới µs>`
    (29 ký tự; tên bảng nằm trong `data`); đã thử với tên bảng 33 ký tự.
  - Bài học phase 3 về slot lag đã được cập nhật theo chu kỳ flush 10 giây (slot lag lúc rảnh đo được
    248–496 byte).
- Các lưu ý đầu vào cho phase 5 nằm ở mục Phase 5 bên dưới.

**Rút ra được:**
- Đọc bytecode thay vì đọc release note: quét class-file version của các jar đã phát hành (và sau
  đó của cả 257 jar đóng gói kèm) trả lời chắc chắn câu "có chạy trên Java 11 không?".
- Dependency của plugin quan trọng không kém plugin: file zip runtime đóng gói Parquet, AWS SDK,
  Hadoop… nhưng cố ý không đóng gói Kafka — nó chạy trên thư viện Kafka của worker.
- `validate` trước khi `create`: `PUT /connector-plugins/<class>/config/validate` của Connect bắt
  lỗi cấu hình mà không khởi động gì; kết nối (token Polaris, S3) kiểm tra được từ bên trong
  container worker với chính các biến môi trường đó.
- Chờ đúng điều kiện: hai lần việc kiểm tra chờ nhầm thứ (dòng commit cũ trong log, số snapshot mà
  một bảng không bao giờ đạt tới vì có lần commit bỏ qua nó). Hãy chờ đúng thứ mình cần (số dòng
  bronze = end offset của Kafka), không chờ một chỉ số đại diện.
- Commit timeout trong log không phải lỗi: sink idle hoặc vừa khởi động sẽ hết thời gian chờ và
  không commit gì; dữ liệu không bao giờ mất vì offset Kafka chỉ tiến khi commit Iceberg thành công.
- Exactly-once vào Iceberg vẫn đúng khi restart (không hụt, không trùng `_offset`); bản trùng chỉ có
  thể đến từ phía trước (Debezium at-least-once) — silver vẫn phải khử trùng.
- Stream vào Iceberg sinh nhiều file nhỏ (mỗi bảng một file mỗi lần commit): ổn cho demo, compaction
  là việc của phase 8.
- Python 3.8 trong image Spark: biểu thức trong f-string không được chứa dấu backslash.
- Mọi engine ghi hoặc bảo trì một bảng nên dùng cùng phiên bản Iceberg với bên ghi; "đọc được"
  không nói lên gì về compaction hay expire snapshot khi có bên khác đang ghi song song.
- Chạy thật từ đầu đến cuối tìm ra những thứ các phép kiểm tra ngắn bỏ sót: cả hai lỗi trên chỉ lộ
  ra sau ~2 giờ (token) hoặc sau một lần khởi động lại rồi rảnh (sink). Hãy để hệ thống chạy một
  thời gian, và đọc access log của server — nó chỉ ra lỗi token chính xác tới từng phút.
- Tham số của Spark procedure phải là hằng (`TIMESTAMP '...'`, không phải `current_timestamp()`).

## Phase 5 — Silver *(xong)*

**Mục tiêu:** bản sao có lịch sử (SCD Type 2) của nguồn trong Postgres `data-platform`.

- [x] DDL `postgres/schema.sql`, do `postgres-setup` chạy (idempotent) mỗi lần `up` (lỗi SQL thì
  job fail): schema `config` với `etl_checkpoint` (mỗi `(pipeline, table_name)` một dòng:
  `source_table`, `key_columns`, `last_snapshot_id`, `last_watermark`, `is_active`); schema
  `silver` với `users` / `orders` / `order_details` (design §6.3) — `PRIMARY KEY (<key>, _lsn)`,
  partial unique index `(<key>) WHERE _is_current`, `CHECK ((_valid_to IS NULL) = _is_current)`,
  chuỗi nguồn là `text`, không FK/CHECK trên cột nguồn; schema `staging` với `staging.batch`.
  Cột SCD2/hệ thống bắt đầu bằng `_`; có `_tx_id` (transaction nguồn, thêm khi sửa theo review ở
  dưới).
- [x] `spark/jobs/job_silver.py`, **dùng chung**: danh sách bảng và key lấy từ
  `config.etl_checkpoint`. Mỗi bảng: snapshot hiện tại lấy từ `<table>.refs` (`main`); không có
  snapshot mới = no-op; ngược lại đọc incremental Iceberg `(last_snapshot_id, current]` (NULL thì
  đọc toàn bộ); `events_to_versions` (hàm thuần, 6 test Spark) → `staging.<table>` (JDBC
  overwrite, tạo lại mỗi lần chạy); `staging.batch` (start, end, số dòng) được ghi **cuối cùng**.
  `helper.postgres_jdbc_options()`; thêm `POSTGRES_*` cho `spark-master` / `spark-worker`.
- [x] `airflow/dags/lib/silver_apply.py` (task `silver_apply`): mỗi bảng một transaction — từ chối
  batch có start ≠ checkpoint, bỏ qua batch đã áp, `ADD COLUMN` cho cột staging mới, rồi thay thế
  (version có transaction còn tiếp tục trong staging) + đóng + chèn (design §6.4) với mọi giá trị
  được cast sang kiểu của silver, đẩy checkpoint lên. Các bảng áp độc lập; task fail ở cuối nếu có
  bảng lỗi. Test: phần sinh SQL trên host (3), bản thân bước apply trên một DB Postgres nháp (9,
  marker `postgres`). Code dùng chung cho DAG nằm ở `airflow/dags/lib/` (chuyển từ
  `airflow/plugins/`; `airflow/dags/.airflowignore` để DAG processor không quét thư mục này).
- [x] DAG `airflow/dags/platform_pipeline.py`: `silver_spark` → `silver_apply` (→ `silver_verify`, thêm sau phase), `schedule=None`,
  `max_active_runs=1`.
- [x] Đã kiểm tra 2026-11-04 (chạy DAG trên stack thật):
  - lần đầu (đọc toàn bộ): 156 / 640 / 1459 phiên bản; số dòng = số cặp (key, txId) khác nhau
    trong bronze, số phiên bản hiện tại = số key, `_is_current AND NOT _is_deleted` = số dòng ở
    nguồn (93 / 426 / 1309), và mọi dòng hiện tại giống nguồn tới từng cột;
  - chạy lại khi không có gì mới: "batch already applied", không thay đổi gì;
  - sau 90 giây chạy generator: lần incremental stage 41 / 129 / 297 dòng, đóng 23 / 28 / 20
    phiên bản; mọi kiểm tra trên vẫn đúng (103 / 506 / 1566, giống hệt nguồn); không key nào có
    ≠ 1 phiên bản hiện tại, chuỗi `_valid_from` / `_valid_to` không có khoảng hở;
  - lùi checkpoint của `orders` một lần chạy: đọc lại 129 event, đóng 0, chèn 0 (idempotent);
  - `staging.batch` lỗi thời (start ≠ checkpoint): bảng đó fail, checkpoint giữ nguyên, các bảng
    khác vẫn áp;
  - ví dụ order 45: pending → paid → shipped → completed → refunded; order bị xóa kết thúc bằng
    phiên bản hiện tại `_is_deleted` mang giá trị cuối cùng của nó.

**Hoàn thành khi:**
- Trigger DAG thì `silver.*` có dữ liệu; một đơn hàng có nhiều phiên bản với `_valid_from` /
  `_valid_to` khớp nhau và đúng một bản `_is_current`.
- Trigger lại khi không có event mới thì không có gì thay đổi (idempotent).
- Đơn hàng bị xóa kết thúc bằng một phiên bản hiện hành có `_is_deleted = true`.
- Test Spark transform chạy qua trong container `spark-master`.

**Lưu ý đầu vào từ phase 4** (đã xử lý hết):
- Sắp xếp phiên bản theo `source.lsn`, không bao giờ theo `_ingested_at` (thời điểm record Kafka,
  không phải thứ tự commit).
- Tiền trong bronze là `decimal(38,2)`: cast rõ ràng sang `numeric(18,2)` khi ghi silver — làm
  trong SQL apply (`st.col::numeric(18,2)`).
- Timestamp trong `before` / `after` là chuỗi ISO-8601 (UTC, `...Z`): được parse sang
  `timestamptz` bằng cùng phép cast đó.
- Sink lúc idle không commit gì, nên thường không có snapshot mới kể từ checkpoint: job coi đó là
  no-op (`row_count = 0`, checkpoint giữ nguyên).
- Bronze có thể chứa event trùng (Debezium là at-least-once): khử trùng theo (key, lsn).

**Rút ra được:**
- **Spark JDBC chỉ append hoặc overwrite** — không UPDATE/MERGE, không có transaction chung cho
  các partition — nên SCD2 cần bảng staging và bước apply bằng SQL trên tập dữ liệu trong
  Postgres. Đó cũng là chỗ duy nhất silver và checkpoint cùng tiến lên.
- **Ghi bản mô tả "staging đang chứa gì" sau cùng, và kiểm tra nó khi apply.** `staging.batch`
  ghi sau dữ liệu, bước apply từ chối batch có start ≠ checkpoint: mọi kiểu lỗi chỉ làm silver
  chậm lại (đọc lại sau, bỏ qua nhờ `_lsn`), không bao giờ bỏ sót dữ liệu.
- **Lùi checkpoint mới là phép thử idempotent thật**: nó cho các event đã áp đi lại toàn bộ đường
  ống. So từng cột với nguồn lần này không bắt được lỗi gì, nhưng đó là phép kiểm tra sẽ bắt được
  một phép cast sai.
- **Cast sang kiểu đích bằng SQL** thay vì bắt chước kiểu trong Spark: staging giữ đúng thứ Spark
  sinh ra (timestamp dạng text có offset, `numeric(38,2)`), Postgres cast sang kiểu của silver,
  và cột nguồn mới đi vào bằng `ADD COLUMN` với kiểu của staging. Ghi timestamp Spark qua JDBC sẽ
  phụ thuộc time zone của session/JVM.
- **Snapshot ID của Iceberg là số ngẫu nhiên**: snapshot hiện tại lấy từ `<table>.refs` (`main`),
  đọc incremental đi theo chuỗi parent — không bao giờ so sánh ID.
- Dùng `source.ts_us` chứ không phải `ts_ms`: hai transaction trên cùng một key trong cùng một
  mili giây sẽ cho phiên bản độ dài bằng 0.
- Event snapshot (`op = r`) dùng chung một LSN và txId, và `_valid_from` của chúng là thời điểm
  snapshot, không phải lúc dòng thật sự được tạo — lịch sử trước snapshot đầu tiên là không biết.
- File DAG mới chưa trigger được cho đến khi scheduler serialize nó (`DagNotFound`): chờ lần quét
  thư mục DAG kế tiếp (tối đa 5 phút) hoặc chạy `airflow dags reserialize`.

**Sau phase — sửa theo review (2026-11-04):**
- **Transaction bị tách → version dài 0 (bug, đã sửa).** Gộp theo transaction chỉ đúng trong một
  batch, nhưng một lần commit của sink có thể cắt đôi các event của một transaction (bronze đã có 2
  cặp key/transaction nằm ở hai lần commit). Một lần chạy silver rơi vào giữa hai lần commit sẽ
  chèn phần đầu làm version hiện tại; lần chạy sau đóng nó với `_valid_to = _valid_from` (CHECK
  `>=` vẫn cho qua, phép kiểm khoảng hở không thấy). Cách sửa: silver giữ `_tx_id`; bước apply
  **xóa** trước version hiện tại nếu transaction của nó còn tiếp tục trong staging (câu 0, design
  §6.4), nên chuỗi version vẫn liền. Silver được dựng lại từ bronze với cột mới (cùng số dòng, giống
  hệt nguồn); thêm phép kiểm `count(*) WHERE _valid_to = _valid_from = 0` (`docs/services.md`).
- **Test bước apply SCD2 trên database thật:** `tests/test_silver_apply_db.py` (marker `postgres`,
  9 test trên DB nháp dựng từ `postgres/schema.sql`): batch đầu, cast kiểu, chạy lại cùng batch,
  lùi checkpoint, batch chồng lấn, delete, transaction bị tách, batch lỗi thời, cột mới. Test
  transaction bị tách fail với code apply cũ.
- 5 lần chạy DAG trong lúc generator đang ghi + 1 lần sau khi dừng: mọi kiểm tra đều đạt, 0 version
  dài 0, giống hệt nguồn (140 / 718 / 2198).
- Kiểu của cột mới (giới hạn đã chấp nhận, design §10): cột `timestamptz` mới ở nguồn thành `text`
  trong silver, `numeric(18,2)` thành `numeric(38,2)`; nếu cần thì sửa bằng cách map kiểu từ Avro
  schema. Đã thêm vào kịch bản phase 8, cùng với: chạy silver sau compaction bronze, và snapshot lại
  khi generator đang chạy (CHECK `_valid_to >= _valid_from` có thể fail).
- **Tham số DAG `table`** (2026-11-04): `all` (mặc định) hoặc một bảng (`users` / `silver.users`).
  `submit_and_wait(app_args=)` truyền `--table` cho `job_silver.py` qua `appArgs` của REST;
  `silver_apply` lọc theo cùng cách. Tên sai định dạng bị từ chối lúc trigger (pattern của
  `Param`); tên không tồn tại được đối chiếu với `config.etl_checkpoint` trước khi submit, nên
  fail trong ~1 giây kèm danh sách bảng active (kiểm tra trong job Spark chỉ hiện ra trong Airflow
  dưới dạng `driverState FAILED`, ~1 phút sau). Đã kiểm tra: lần chạy `users` chỉ đụng tới users;
  lần chạy `all` ngay sau đó xử lý hai bảng còn lại; silver vẫn giống hệt nguồn.
- **`staging.batch` giữ một dòng cho mỗi bảng active** (2026-11-04): job đọc bảng này, thay các
  dòng của bảng vừa xử lý (`merge_batches`, 2 test host), giữ nguyên các dòng khác cùng
  `created_at` của chúng, rồi ghi lại — trước đây lần chạy `--table` truncate mất dòng của các bảng
  khác, nên batch đang chờ của bảng khác phải đợi thêm một lần chạy Spark. Đã kiểm tra: chạy
  `users` → dòng `orders` không đổi; `all` → 3 dòng; `users` lần nữa → chỉ dòng của nó đổi. Hai cái
  bẫy gặp trên đường: `ruff check --fix` (target py311) đổi `timezone.utc` thành `datetime.UTC`, gây
  ImportError trên Python 3.8 của image Spark — sửa tận gốc bằng `per-file-target-version` `py38`
  cho `spark/jobs/**`; và `.options(**jdbc, url=...)` báo "multiple values for keyword argument" —
  ba lần chạy fail đó chỉ chứng minh cơ chế an toàn hoạt động (staging bị ghi lại, batch thì
  không, checkpoint giữ nguyên, lần sau đọc lại hết).
- **Task `silver_verify`** (2026-11-04), cuối `platform_pipeline`, cùng tham số `table`: các phép
  kiểm silver trước đây chạy tay giờ chạy mỗi lần và làm DAG fail — cho từng bảng
  `keys_without_one_current`, `zero_length_versions`, `broken_chains` = 0, và số dòng / số dòng hiện
  tại của silver = số (key, txId) khác nhau / số key khác nhau của bronze đọc **tại snapshot
  checkpoint** (Trino `FOR VERSION AS OF`), nhờ vậy phép so sánh chính xác ngay cả khi generator
  vẫn đang ghi (đã kiểm tra: chạy khi generator đang bật, tất cả OK, 3023 / 1395 / 358 dòng = bronze).
  Làm lệch `_valid_to` của một version thật 1 giây thì task fail với `broken_chains` (đã hoàn tác).
  Test: 3 trên host (SQL), 4 trên DB nháp (apply bình thường, lịch sử hỏng, lệch với bronze, trước
  lần apply đầu). Chỉ còn "silver = nguồn" là kiểm tra tay: nó chỉ đúng khi pipeline đã xử lý hết.
- **Tham số DAG `full_refresh`** (2026-11-06): dựng lại silver của một bảng từ toàn bộ bảng bronze
  (sau khi đổi logic biến đổi, hoặc để sửa silver) — thay cho cách làm tay `TRUNCATE` + reset
  checkpoint, vốn để silver trống tới lần chạy sau. `job_silver.py --full-refresh` (cờ argparse:
  nhận giá trị chuỗi `"false"` thì vẫn là truthy) đưa toàn bộ bronze vào staging, không có snapshot
  bắt đầu; `silver_apply` tự nhận ra batch đó và `DELETE` + insert lại trong một transaction, kể cả
  khi không có snapshot bronze mới; bị từ chối khi đi cùng `all` và khi bronze không có snapshot.
  DAG render template thành object Python (`render_template_as_native_obj=True`), nên cả hai tham số
  tới task theo cùng một cách, là đối số render từ `"{{ params.<name> }}"`, đúng kiểu dữ liệu (mặc
  định template render ra `"False"`, một chuỗi truthy; `silver_spark` còn kiểm tra kiểu).
  Test: thêm 4 test trên DB nháp (lịch sử hỏng được dựng lại, không có snapshot mới, lỗi thì silver
  giữ nguyên, bronze rỗng bị từ chối) + 3 test cho cờ. Đã kiểm tra: làm hỏng `silver.users` (ghi đè
  tên hiện hành, xóa 6 version cũ) → full refresh → lịch sử giống từng byte với trước đó (516 dòng),
  verify OK, = nguồn; `all` + `full_refresh` bị từ chối; lần chạy kế tiếp không có conf là cộng dồn.
- Bài học: **một luật gộp áp theo từng batch chỉ đúng nếu ranh giới batch không cắt ngang nhóm.** Ở
  đây batch bị cắt bởi các lần commit của sink, vốn không biết gì về transaction nguồn — nên luật
  phải được hoàn tất xuyên batch ở bước apply. Hãy test ranh giới, không chỉ trạng thái bình
  thường: các lần chạy thật chưa gặp nó, còn test DB thì tái hiện được bất cứ lúc nào.

## Phase 6 — Gold *(đang làm)*

**Mục tiêu:** mô hình phân tích trong ClickHouse và dashboard trong Superset.

Bước 1 (2026-11-06), theo thống nhất: dựng mọi service và nối chúng với nhau, một hai bảng gold
đầu tiên dạng **dựng lại toàn bộ** (watermark / incremental làm sau), Superset chạy và thấy được
gold — chưa làm dashboard.

- [x] **ClickHouse 26.8.36.3** (dòng LTS mới nhất), module mới `compose/clickhouse.yml`: một node,
  chỉ mở cổng host **8123** (cổng native 9000 để nội bộ: 9000 trên host là RustFS), log ra stdout
  (`clickhouse/config.d/logger.xml`), volume `clickhouse_data_vol`, TZ `Asia/Ho_Chi_Minh`. User:
  `admin` (image tạo, `CLICKHOUSE_ADMIN_PASSWORD`; dbt dùng) và `superset_reader`
  (`clickhouse/users.d/superset_reader.xml`, `GRANT SELECT ON gold.*`). Named collection
  **`silver_pg`** (`clickhouse/config.d/named_collections.xml`) → Postgres `data-platform`, schema
  `silver`, bằng role Postgres chỉ đọc mới **`clickhouse_reader`** (do `postgres-setup` tạo và cấp
  quyền: `USAGE` + `SELECT` trên `silver`, default privileges cho bảng mới; bị từ chối ở `config`
  và mọi lệnh ghi).
- [x] **dbt-core 1.12.6 + dbt-clickhouse 1.10.3** (+ `dbt-adapters` 1.24.5: dbt-clickhouse cần
  < 1.25) trong **virtualenv riêng** `/opt/airflow/dbt-venv` của image Airflow
  (`airflow/dbt-requirements.txt`) — không trộn với constraints của Airflow. `dbt/profiles.yml`
  (đọc biến môi trường), `dbt/dbt_project.yml`; `DBT_TARGET_PATH` / `DBT_LOG_PATH` đặt trong `/tmp`
  để không ghi gì vào thư mục project được bind mount.
- [x] Model đầu tiên: `stg_users` / `stg_orders` dạng ephemeral (dòng hiện tại, chưa xóa của silver
  qua `postgresql(silver_pg, ...)`, timestamp có kiểu `DateTime64(6, 'Asia/Ho_Chi_Minh')`) → bảng gold
  **`dim_users`** (không có email/phone) và **`fct_orders`** (+ `order_date` = ngày theo giờ VN),
  `MergeTree`, `materialized: table`. Test: `unique` / `not_null` + `fct_orders_matches_silver`,
  `dim_users_matches_silver` (số dòng gold = silver).
- [x] DAG: `silver_verify` → **`gold_dbt_run`** → **`gold_dbt_test`** (`BashOperator`); tham số
  `table` chỉ giới hạn các task silver.
- [x] **Superset 6.1.0** (`superset/Dockerfile`: + `clickhouse-connect` 0.15.1 — image gốc không có
  driver nào), module `compose/superset.yml`, cổng host **8088**, một service duy nhất: metadata trong
  **SQLite** trên `superset_home_vol`, và command của nó chạy `superset/scripts/init.sh` (idempotent:
  `db upgrade`, tạo admin hoặc đặt lại mật khẩu theo `.env`, `superset init`, `set-database-uri` cho
  **ClickHouse gold** bằng `superset_reader` — sau đó thay bằng `bootstrap.py`, xem phần sửa theo review bên dưới) trước server. Không Redis/Celery. *(Review
  2026-11-06: ban đầu dựng với metadata DB trên Postgres + job one-shot `superset-init`; chuyển sang
  SQLite và một service — bỏ DB/role Postgres, bớt 3 biến `.env`, bỏ `psycopg2`, bớt một container.
  SQLite không dành cho production; đủ cho một người dùng. Kiểm tra lại: volume trống healthy sau
  73 giây, khởi động lại sau 52 giây với "exists, resetting its password", SQL Lab vẫn thấy gold; DB
  và role `superset` cũ đã được xóa.)*
- [ ] Gold incremental với watermark `_silver_updated_at` (`ReplacingMergeTree`, design §8).
- [ ] Thêm model: `dim_date`, `fct_order_lines`, mart (doanh thu theo ngày / theo thành phố).
- [x] Sửa theo review (2026-11-06):
  - **Dashboard nằm trong git.** Metadata của Superset là một file SQLite trên volume, nên
    `down -v` sẽ mất mọi dataset/chart/dashboard. `superset/export-dashboards.sh` (chạy trên host)
    export chúng thành YAML thường vào `superset/dashboards/`; mỗi lần khởi động
    `superset/scripts/bootstrap.py` import các dashboard **chưa có** trong metadata DB (dashboard đã
    có thì giữ nguyên: chỉnh sửa trên UI còn sau mỗi lần bật máy — lệnh CLI `import-dashboards` gốc
    luôn ghi đè). Nó cũng thay `set-database-uri`: database **ClickHouse gold** giờ có **uuid cố
    định**, vì bản export mang password đã bị che và importer chỉ chấp nhận khi uuid của database đã
    tồn tại (với uuid ngẫu nhiên của `set-database-uri`, import trên volume rỗng lỗi "Must provide a
    password for the database" và làm Superset dừng).
  - `superset_reader` có profile ClickHouse riêng: `readonly=2` (clickhouse-connect gửi setting kèm
    truy vấn; `readonly=1` sẽ từ chối), `max_execution_time` 60 giây, `max_memory_usage` ~2 GB, cả hai
    bị chặn trần bằng constraint (truy vấn hạ được, không nâng được, không tắt được `readonly`).
  - `airflow-scheduler` không còn chờ ClickHouse: ClickHouse lỗi thì silver vẫn phải chạy;
    `gold_dbt_run` khi đó fail sau vài giây với lỗi rõ ràng (`Failed to resolve 'clickhouse'`).
  - Password của Superset được URL-encode trong URI kết nối (`.env.example` có ghi chú).
  - Không đổi: review lo `set-database-uri` fail khi ClickHouse đang khởi động — lệnh này chỉ ghi URI
    vào metadata DB, không hề kết nối.
- [ ] Dataset Superset + một dashboard (doanh thu theo ngày và theo thành phố), export vào
  `superset/dashboards/`.

**Đã kiểm tra 2026-11-06:** `clickhouse_reader` đọc được silver và không gì khác; ClickHouse đọc
silver qua `silver_pg` với giá trị và thời gian khớp (1167 = nguồn); `superset_reader` không tạo
được gì và chỉ thấy schema `gold`; Superset (REST API) liệt kê `gold.dim_users` / `gold.fct_orders`
và chạy được truy vấn SQL Lab (số đơn và doanh thu theo ngày); chạy lại init → "exists,
resetting its password". End to end: generator 1 phút → DAG 5 task `success`, `dbt run` 2 model,
`dbt test` 8/8 PASS, gold = nguồn (1217 order, 221 user). 29 container, tất cả healthy (Loki không có
healthcheck); log ClickHouse và Superset lên Loki.

**Gặp trên đường:**
- `postgresql()` trả mọi cột silver về `Nullable` (cột nguồn của silver cố ý cho phép null), mà
  ClickHouse không chấp nhận khóa partition nullable — bỏ partition (dữ liệu demo không cần) thay vì
  ép `assumeNotNull`, vì như vậy test `not_null` cũng thành vô nghĩa.
- Dùng named collection cần quyền **`NAMED COLLECTION`**, mà `access_management` không cấp, và user
  định nghĩa bằng XML (admin do image tạo) không nhận grant qua SQL được:
  `clickhouse/users.d/admin.xml` thêm `named_collection_control` cho cùng tên user — nên tên admin
  được cố định (`admin`, bỏ biến `CLICKHOUSE_ADMIN_USER`).
- `config.d/docker_related_config.xml` của image chứa `listen_host` — mount từng file cấu hình, không
  mount cả thư mục. Cảnh báo `Listen [::]... failed` là vô hại (mạng Docker không có IPv6).
- `dbt debug` fail ở bước kiểm tra `git` (image không có git) — chỉ cần khi dùng dbt packages.
- Dashboard dạng code, thử với một dashboard tạm (đã xóa): export, xóa volume Superset, khởi động
  lại → database được tạo với uuid cố định, dashboard được import, truy vấn qua dataset đã import trả
  về 1217 (password còn nguyên); đổi tên trên UI rồi khởi động lại → "all 1 dashboard(s) exist, kept
  as they are", tên mới được giữ. Healthy 60 giây sau khi khởi động trên volume rỗng.

**Hoàn thành khi:**
- `dbt run` và `dbt test` chạy qua trong DAG.
- `SELECT count() FROM gold.fct_orders FINAL` khớp với số đơn hiện hành, chưa bị xóa trong silver.
- Dashboard Superset hiện doanh thu theo ngày và theo thành phố, và tự quay lại trên một volume
  Superset rỗng (import từ `superset/dashboards/`).

## Phase 7 — Search + API

**Mục tiêu:** một trang web tìm kiếm đơn hàng, xem lịch sử và thống kê.

- [ ] Task `sync_es` trong `platform_pipeline` (chạy song song với dbt): mỗi đơn hàng là một
  document; dựng lại các đơn có chính nó, dòng chi tiết hoặc user thay đổi; xóa các đơn đã
  `_is_deleted`; client `elasticsearch` bản 7.x.
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
- [ ] Chạy mọi kịch bản trong design §10 và ghi kết quả vào README — gồm cả các kịch bản thêm
  từ review phase 5: kiểu của cột mới trong silver, chạy silver sau compaction bronze, snapshot
  lại khi generator đang chạy.

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
