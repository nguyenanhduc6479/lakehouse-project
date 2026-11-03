# Data platform — thiết kế

> Bản dịch tiếng Việt của [`dataplatform-design.md`](dataplatform-design.md). Bản tiếng Anh là bản
> gốc; nếu hai bản lệch nhau, theo bản tiếng Anh.

Một dự án để học: dựng một data platform nhỏ nhưng đầy đủ trên Docker Compose và thấy mọi thành
phần chạy từ đầu đến cuối — CDC từ nguồn OLTP, event log trong lakehouse, tầng silver lưu lịch sử,
tầng gold OLAP cho dashboard, và một search index cho web app.

**Phạm vi: demo, không phải production.** Chỉ cần chạy được và dễ theo dõi. Nghĩa là:

- Chỉ Docker Compose (không Kubernetes, không operator), chạy trên Docker Engine cài thẳng trong WSL
  (không dùng Docker Desktop).
- Nguồn là một database Postgres được ghi dữ liệu bởi một script Python generator.
- Các DAG Airflow được kích hoạt **thủ công** (`schedule=None`).
- Không HA, read replica, connection pooler, TLS hay backup.
- Silver **nhất quán sau cùng (eventually consistent) giữa các bảng** (xem §6.2).

## 1. Kiến trúc

```mermaid
flowchart TD
    GEN["generator.py"] --> SRC[("source-db: Postgres OLTP")]
    SRC -->|"WAL / logical decoding"| DBZ["Debezium on Kafka Connect"]
    DBZ -->|Avro| KAFKA[Kafka]
    DBZ -.->|"register schema"| SR[Schema Registry]
    KAFKA -->|"Iceberg sink"| BRONZE[("Bronze: Iceberg on RustFS, Polaris catalog")]
    SR -.->|"read schema"| BRONZE
    BRONZE -->|"PySpark: SCD2"| SILVER[("Silver: Postgres")]
    BRONZE -.->|"inspect / debug"| TRINO[Trino]
    SILVER -->|"batch sync"| ES[(Elasticsearch)]
    SILVER -->|"dbt-clickhouse via postgresql()"| GOLD[("Gold: ClickHouse")]
    GOLD --> SUPERSET[Superset]
    ES --> API[FastAPI]
    SILVER --> API
    GOLD --> API
    API --> WEB[Web page]
    AIRFLOW["Airflow: manual DAG"] -.->|orchestrates| SILVER
    AIRFLOW -.-> ES
    AIRFLOW -.-> GOLD
```

Dữ liệu nằm ở ba nơi, mỗi nơi một nhiệm vụ: change event thô ở **bronze** (nguồn sự thật — mọi tầng
sau đều có thể dựng lại từ đây), bản sao có lưu lịch sử ở **silver**, và các model phân tích ở
**gold**. Elasticsearch là search index được suy ra từ silver.

Mỗi tầng có đúng một công cụ truy vấn, để các SQL engine không chồng chéo nhau:

| Tầng | Lưu ở | Truy vấn bằng |
|---|---|---|
| Bronze | Iceberg trên RustFS (Polaris) | **Trino** — chỉ để kiểm tra và debug |
| Silver | Postgres | Postgres thuần (`psql`, API) |
| Gold | ClickHouse | **ClickHouse** — OLAP cho Superset và API |

Với lượng dữ liệu demo, gold hoàn toàn có thể nằm trong Postgres; ClickHouse được giữ lại có chủ
đích để học một OLAP engine dạng cột (MergeTree, background merge, `FINAL`, nạp incremental từ
nguồn bên ngoài).

## 2. Thành phần

| Thành phần | Vai trò | Ghi chú |
|---|---|---|
| `source-db` (Postgres 16) | OLTP giả lập: `users`, `orders`, `order_details` | Instance riêng (`compose/source.yml`), database `source-db`, host port 5433, `wal_level=logical` |
| `generator.py` | Liên tục insert/update/delete vào nguồn | Python + Faker + psycopg, chỉnh được tốc độ |
| Kafka Connect | Chạy Debezium (source) và Iceberg sink | Image tự build trên `confluentinc/cp-kafka-connect` (có sẵn AvroConverter) + plugin Debezium Postgres + file zip Iceberg sink (không được publish thành artifact — build từ source Iceberg trong Dockerfile multi-stage; **Iceberg 1.10.2**, bản mới nhất chạy được trên Java 11 của worker) |
| Kafka UI | Xem bên trong Kafka: topic, message đã giải mã Avro, schema, connector (chỉ đọc) | `kafbat/kafka-ui`, port host 8082 |
| Kafka (+ Zookeeper) | Vận chuyển event Avro | Đã có trong `compose/infra.yml` |
| Schema Registry | Lưu schema Avro, kiểm tra tương thích | Đã có trong `compose/infra.yml` |
| RustFS (S3) + Iceberg | Bronze: change event chỉ ghi thêm (append-only) | Đã có trong stack (`rustfs/rustfs:1.0.4`, thay cho MinIO) |
| Polaris | Iceberg REST catalog cho bronze | Đã có trong stack (OAuth2, scope `PRINCIPAL_ROLE:ALL`) |
| Trino | SQL trên bronze (kiểm tra, debug, kịch bản test) | Đã có trong stack; không nằm trên luồng dữ liệu chính |
| PySpark | Bronze → silver (SCD Type 2) | Spark cluster hiện có, submit qua REST 6066; `spark.master` được đặt trong `spark-defaults.conf` để job submit qua REST chạy trên executor thật; log của driver/executor hiện trên Grafana (`service=spark-job`) |
| Postgres (silver, Postgres 16) | Các bảng silver, đồng thời là database của web app | Database `data-platform` (`POSTGRES_DB`) trên service `postgres` hiện có, bên cạnh `airflow` và `polaris`; Airflow kết nối tới nó qua `postgres_conn` |
| ClickHouse | Gold (OLAP): lưu các bảng gold, phục vụ Superset và API | Một node |
| dbt-clickhouse | Silver → gold | Chạy trong container Airflow (`./dbt`); thêm vào `airflow/requirements.txt` ở phase gold — cần kiểm tra nó resolve được với constraints của Airflow 2.9.2 |
| Superset | Dashboard | Image tự build thêm driver `clickhouse-connect` (image gốc không có) |
| Elasticsearch 7.17 | Tìm kiếm cho web app | Đã có trong `compose/infra.yml`; Python client phải là `elasticsearch` 7.x (8.x từ chối server 7.x) |
| FastAPI | Đường đọc duy nhất của web app | ES cho tìm kiếm, silver cho chi tiết/lịch sử, gold cho số liệu |
| Airflow | Điều phối các bước batch | Chỉ kích hoạt thủ công; tự chạy các task nhẹ (SQL trên Postgres, ES sync); các transform nặng chạy trong Spark và dbt |
| Grafana + Loki + Alloy | Log của mọi container (file log json-file của Docker) và của các job Spark | Đã có trong stack; mọi service chỉ khởi động sau khi Alloy healthy, nên không sót dòng log nào |

Mọi service dùng chung một bridge network là `lakehouse` và gọi nhau bằng tên service
(`kafka:29092`, `http://polaris:8181`, `postgres:5432`, …); port trên host và các connection của
Airflow được liệt kê trong [services.md](services.md).

## 3. Nguồn

### 3.1. Schema

Ba bảng trong schema `shop` của database `source-db` (DDL ở `source/schema.sql`). Tên là `users` và
`orders` vì `user` và `order` là từ khóa dành riêng trong PostgreSQL.

| Bảng | Khóa chính | Cột chính |
|---|---|---|
| `shop.users` | `user_id` | `email` (duy nhất), `full_name`, `phone`, `city`, `status` (active / locked / closed), `created_at`, `updated_at` |
| `shop.orders` | `order_id` | `user_id`, `status` (pending / paid / shipped / completed / cancelled / refunded), `payment_method` (cod / card / bank_transfer / e_wallet), `shipping_city`, `total_amount numeric(18,2)`, `currency char(3)` (mặc định VND), `ordered_at`, `created_at`, `updated_at` |
| `shop.order_details` | `order_detail_id` | `order_id`, `line_number` (duy nhất trong mỗi đơn), `sku`, `product_name`, `category`, `quantity`, `unit_price`, `discount_amount`, `line_total` (mọi cột tiền là `numeric(18,2)`), `created_at`, `updated_at` |

Khóa chính là `bigint GENERATED ALWAYS AS IDENTITY`: generator không bao giờ tự đặt ID.

Lựa chọn thiết kế:

- Các cột trạng thái là `varchar` + `CHECK` thay vì ENUM — dễ thay đổi hơn, và tiện cho các thí
  nghiệm schema evolution.
- `line_total` (= `quantity × unit_price − discount_amount`) là cột thường do generator tính.
  Generated column không được logical replication gửi đi trước PostgreSQL 18.
- `updated_at` được duy trì bằng trigger.
- `order_details.order_id` có `ON DELETE CASCADE`, nên xóa một đơn hàng sinh ra delete trên hai
  bảng trong cùng một transaction.

### 3.2. Thiết lập CDC

- `wal_level = logical` (truyền qua `postgres -c wal_level=logical` trong compose).
- `max_slot_wal_keep_size = 1GB`: giới hạn lượng WAL mà một slot chưa được đọc có thể giữ lại, để
  Debezium bị dừng không làm đầy ổ đĩa laptop. Vượt ngưỡng, Postgres vô hiệu hóa slot và Debezium
  phải snapshot lại — bản thân đó cũng là một kịch bản test (§10).
- `REPLICA IDENTITY FULL` trên cả ba bảng, để event update/delete mang đầy đủ dòng cũ.
- Publication `dbz_publication` chỉ gồm ba bảng, được tạo bởi `source/schema.sql` (Debezium không
  được tự tạo: mặc định của nó là `FOR ALL TABLES`, đòi quyền superuser).
- Một user riêng `debezium` có `LOGIN` + `REPLICATION` và `SELECT` trên `shop` (cả các bảng tạo sau
  này, nhờ `ALTER DEFAULT PRIVILEGES`); mật khẩu là `SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD`.
- Bảng heartbeat `public.debezium_heartbeat` (một dòng, cấp `INSERT`/`UPDATE` cho `debezium`), cũng
  nằm trong publication — xem `heartbeat.action.query` ở §4.2.

Toàn bộ phần này — schema, bảng, trigger, thiết lập CDC — nằm trong **`source/schema.sql`**, được
job một lần **`source-db-setup`** (`compose/source.yml`, image `postgres:16`,
`psql -v ON_ERROR_STOP=1`) áp dụng ở mỗi lần `up`. Script idempotent (`IF NOT EXISTS`,
`CREATE OR REPLACE TRIGGER`, và `SELECT … WHERE NOT EXISTS … \gexec` cho publication và role, vì
hai loại này không có `IF NOT EXISTS`). Mật khẩu Debezium đi vào SQL dưới dạng biến của psql:
compose đưa `.env` vào container, job chạy `psql -v debezium_password="$DEBEZIUM_PASSWORD"`, và
script đọc `:'debezium_password'` (được bọc nháy an toàn; psql không thay biến bên trong
`DO $$ … $$`, nên mới dùng `\gexec`).

### 3.3. Generator

`generator.py` lặp vô hạn với tốc độ cấu hình được (`--rate` event/giây) và ngẫu nhiên:

- tạo một đơn hàng — trong **một transaction**: insert `orders`, insert 1–5 `order_details`, rồi
  update `orders.total_amount`;
- chuyển trạng thái đơn hàng tiến lên (pending → paid → shipped → completed; hoặc → cancelled;
  một đơn completed có thể chuyển sang refunded);
- cập nhật một user (thành phố, điện thoại) hoặc khóa một user;
- thỉnh thoảng xóa một đơn hàng (cascade xuống các dòng của nó).

Nó là một service Compose nằm dưới một profile, được bật/tắt thủ công
(`docker compose --profile generator up -d generator`), nên stack có thể chạy mà không sinh dữ
liệu mới.

Triển khai (`source/generator.py`, image `custom-generator`): mỗi nhịp chọn một hành động theo
tỉ trọng — tạo đơn 45 %, chuyển trạng thái đơn 30 %, sửa user 10 %, tạo user 8 %, xóa đơn 4 %,
khóa/mở khóa user 3 % — với tốc độ `GENERATOR_RATE` event/giây; lúc khởi động nó tạo trước 20 user.
Họ tên lấy từ Faker `vi_VN`; thành phố, sản phẩm (17 SKU thuộc 7 nhóm) và số điện thoại lấy từ danh
sách cố định, để dữ liệu sạch. Hai chi tiết quan trọng cho CDC: kết nối chạy ở chế độ
**autocommit**, nên mỗi hành động là một transaction được commit riêng (nếu không, transaction ngầm
của psycopg biến mọi hành động thành savepoint và không có gì được commit cho tới khi generator
dừng); và nó xử lý `SIGTERM` (chạy làm PID 1 thì mặc định sẽ bỏ qua tín hiệu này), nên
`docker compose stop` dừng gọn sau hành động đang chạy. Cách chạy — trong container hoặc trực
tiếp bằng Python — có trong [services.md](services.md#running-the-generator).

## 4. Event: Avro + Schema Registry

### 4.1. Vì sao dùng Avro

Mặc định Debezium xuất JSON kèm toàn bộ schema trong mỗi message: message lớn và không có gì ngăn
một thay đổi schema làm hỏng consumer. Với Avro, một message là dữ liệu nhị phân gọn cộng tiền tố 5
byte (magic byte + schema ID); bản thân schema chỉ lưu một lần trong registry. Registry cũng kiểm
tra mỗi schema mới so với schema trước, nên một thay đổi không tương thích bị từ chối ngay đầu
pipeline thay vì âm thầm làm hỏng dữ liệu ở downstream.

Subject dùng `TopicNameStrategy` (topic `src.shop.orders` → subject `src.shop.orders-key` và
`src.shop.orders-value`), compatibility `BACKWARD`.

### 4.2. Debezium connector

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "source-db",
  "database.port": "5432",
  "database.dbname": "${env:SOURCE_POSTGRES_DB}",
  "database.user": "debezium",
  "database.password": "${env:SOURCE_POSTGRES_DB_DEBEZIUM_PASSWORD}",
  "plugin.name": "pgoutput",
  "publication.name": "dbz_publication",
  "publication.autocreate.mode": "disabled",
  "slot.name": "dbz_slot",
  "topic.prefix": "src",
  "table.include.list": "shop.users,shop.orders,shop.order_details",
  "tombstones.on.delete": "false",
  "decimal.handling.mode": "precise",
  "heartbeat.interval.ms": "10000",
  "heartbeat.action.query": "INSERT INTO public.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

- `${env:...}`: secret lấy từ biến môi trường của container Connect (được điền từ `.env`) thông qua
  `EnvVarConfigProvider` của Kafka (worker config `config.providers=env`), nên không có mật khẩu
  nào nằm trong JSON của connector.
- `publication.autocreate.mode=disabled`: dùng publication ở §3.2, báo lỗi rõ ràng nếu thiếu.
- `tombstones.on.delete=false`: nếu không, sau mỗi delete Debezium sẽ gửi thêm một message có value
  `null`, chỉ dùng cho Kafka log compaction — thứ dự án này không dùng.
- `decimal.handling.mode=precise`: không bao giờ dùng `double` cho tiền.
- `heartbeat.interval.ms` + `heartbeat.action.query`: pgoutput bỏ qua các transaction không đụng tới
  bảng nào trong publication, nên khi `shop.*` không có thay đổi, Debezium không thấy LSN mới, không
  xác nhận được, và slot giữ lại toàn bộ WAL sinh ra trong lúc đó (đo được: lag kẹt ở ~2,7 MB). Chỉ
  bật heartbeat thì Debezium chỉ gửi lại offset cũ; action query upsert dòng của
  `public.debezium_heartbeat` mỗi 10 giây, tạo ra một thay đổi đi qua slot nên LSN tiến lên (lag về
  dưới 1 KB). Bảng nằm trong publication nhưng không có trong `table.include.list`, nên không có
  topic Kafka; bản thân heartbeat đi vào topic `__debezium-heartbeat.src`.
- `timestamptz` luôn được map thành chuỗi `ZonedTimestamp`; `time.precision.mode` chỉ ảnh hưởng
  `timestamp`/`time` không có múi giờ, thứ schema không dùng, nên giữ mặc định.
- Lần khởi động đầu, Debezium snapshot các dòng đang có (event với `op = r`), sau đó stream WAL.
- Debezium **2.7.4.Final**: image gốc `cp-kafka-connect:7.6.0` chạy Java 11, còn Debezium 3.x cần
  Java 17. File tarball của plugin tải từ Maven Central, có kiểm tra checksum, trong
  `connect/Dockerfile`.
- Generator ghi một đơn hàng bằng INSERT + UPDATE (`total_amount` được gán sau khi có các dòng chi
  tiết) trong cùng một transaction, nên một đơn mới sinh ra một event `c` và một event `u` cùng
  `txId` — lý do silver chỉ giữ event cuối của mỗi khóa trong một transaction (§6.2).

Các connector được đăng ký bởi job one-shot `connect-setup` qua Kafka Connect REST API (cùng
pattern với các job `*-setup` khác: idempotent, kiểm tra mọi mã HTTP): mỗi file
`connect/connectors/<name>.json` được `PUT` vào `/connectors/<name>/config` (tạo mới hoặc cập nhật),
rồi job chờ tới khi connector và task của nó `RUNNING`. Một job đăng ký mọi connector, nên nó chờ
mọi thứ mà bất kỳ connector nào cần (`source-db-setup` cho Debezium, `polaris-setup` cho Iceberg
sink): catalog hỏng thì Debezium cũng không được đăng ký. Chấp nhận cho demo; mỗi connector một
setup job riêng sẽ tách được hai bên.

### 4.3. Ánh xạ kiểu dữ liệu

| Postgres | Avro (qua Debezium) | Lưu ý khi đọc trong Spark |
|---|---|---|
| `bigint` | `long` | |
| `integer`, `smallint` | `int` | Avro không có kiểu short |
| `varchar`, `text` | `string` | |
| `numeric(18,2)` | `bytes` + logical type `decimal` | Chính xác, nhờ chế độ `precise`; trong bronze thành `decimal(38,2)` (sink giữ scale nhưng không giữ precision) |
| `timestamptz` | `string` (`io.debezium.time.ZonedTimestamp`, ISO-8601) | Parse thành timestamp |

### 4.4. Envelope

Toàn bộ envelope của Debezium được giữ trong bronze (`before`, `after`, `source`, `op`, `ts_ms`) —
không làm phẳng bằng `ExtractNewRecordState` — vì silver cần `op`, giá trị cũ và `source.lsn`.

### 4.5. Luồng thay đổi schema

Thêm một cột nullable ở nguồn: event tiếp theo mang schema mới, registry kiểm tra và gán ID mới, và
Iceberg sink thêm cột vào bảng bronze (`iceberg.tables.evolve-schema-enabled=true`). Job silver
mang cột mới vào bảng staging, và bước apply thêm các cột staging còn thiếu vào silver bằng
`ALTER TABLE ... ADD COLUMN` (§6.4).

Một thay đổi phá vỡ `BACKWARD` bị registry từ chối và task Debezium fail — thấy được trong status
của Kafka Connect và trong log Grafana. Ví dụ: `quantity integer → text` bị từ chối (Avro không đọc
được `int` cũ thành `string`), còn `integer → bigint` thì qua (`int → long` là phép nâng kiểu Avro
cho phép). Khôi phục thủ công: sửa nguồn, hoặc nới compatibility của subject, rồi restart task.

Lưu ý: đổi tên cột với Avro là "drop + add", điều mà `BACKWARD` cho phép. Cột cũ âm thầm thành
null — một trong các kịch bản test ở §10.

## 5. Bronze — event log trên Iceberg

Mỗi change event một dòng, chỉ ghi thêm. Một Iceberg sink connector duy nhất ghi mỗi topic vào bảng
riêng trong namespace `bronze` của Polaris catalog `lakehouse`:

| Topic | Bảng bronze |
|---|---|
| `src.shop.users` | `lakehouse.bronze.users_cdc` |
| `src.shop.orders` | `lakehouse.bronze.orders_cdc` |
| `src.shop.order_details` | `lakehouse.bronze.order_details_cdc` |

Avro được giải mã thành các cột Iceberg có kiểu (Parquet), không lưu dạng byte thô; `before`,
`after` và `source` vẫn là struct lồng nhau.

SMT `InsertField` có sẵn của Kafka thêm `_topic`, `_partition`, `_offset` và `_ingested_at`
(timestamp của Kafka record). Nó được dùng thay cho `KafkaMetadataTransform` của sink vì cái đó ghi
timestamp dạng `long` thường, trong khi partition transform `day()` của Iceberg cần timestamp thật.
Các bảng được partition theo `day(_ingested_at)`.

Cấu hình sink connector (`connect/connectors/iceberg-bronze.json`, connector `iceberg-bronze`):

```json
{
  "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
  "tasks.max": "1",
  "topics": "src.shop.users,src.shop.orders,src.shop.order_details",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",

  "transforms": "meta",
  "transforms.meta.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.meta.topic.field": "_topic",
  "transforms.meta.partition.field": "_partition",
  "transforms.meta.offset.field": "_offset",
  "transforms.meta.timestamp.field": "_ingested_at",

  "iceberg.tables": "bronze.users_cdc,bronze.orders_cdc,bronze.order_details_cdc",
  "iceberg.tables.route-field": "_topic",
  "iceberg.table.bronze.users_cdc.route-regex": "^src\\.shop\\.users$",
  "iceberg.table.bronze.orders_cdc.route-regex": "^src\\.shop\\.orders$",
  "iceberg.table.bronze.order_details_cdc.route-regex": "^src\\.shop\\.order_details$",
  "iceberg.tables.auto-create-enabled": "true",
  "iceberg.tables.evolve-schema-enabled": "true",
  "iceberg.tables.default-partition-by": "day(_ingested_at)",
  "iceberg.tables.auto-create-props.format-version": "2",
  "iceberg.control.topic": "control-iceberg",
  "iceberg.control.commit.interval-ms": "60000",

  "iceberg.catalog.type": "rest",
  "iceberg.catalog.uri": "http://polaris:8181/api/catalog",
  "iceberg.catalog.warehouse": "lakehouse",
  "iceberg.catalog.rest.auth.type": "oauth2",
  "iceberg.catalog.oauth2-server-uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
  "iceberg.catalog.credential": "root:${env:POLARIS_SECRET}",
  "iceberg.catalog.scope": "PRINCIPAL_ROLE:ALL",
  "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
  "iceberg.catalog.s3.endpoint": "${env:S3_ENDPOINT}",
  "iceberg.catalog.s3.path-style-access": "true",
  "iceberg.catalog.s3.access-key-id": "${env:S3_ACCESS_KEY}",
  "iceberg.catalog.s3.secret-access-key": "${env:S3_SECRET_KEY}",
  "iceberg.catalog.client.region": "${env:S3_REGION}"
}
```

- Routing là tĩnh: sink so `_topic` với `route-regex` của từng bảng (có neo đầu/cuối, nên mỗi topic
  map đúng vào một bảng).
- `format-version=2`: Trino 454 không đọc được bảng v3, và v2 có đủ những gì bronze cần. Spark chạy
  cùng phiên bản Iceberg với sink (**1.10.2**), vì nó sẽ compact và expire snapshot trên chính các
  bảng này trong lúc sink vẫn commit (phase 8).
- `commit.interval-ms=60000`: mặc định là 5 phút; một phút giúp demo phản hồi nhanh hơn. Mỗi lần
  commit có dữ liệu thêm một snapshot `append` và một file Parquet cho mỗi bảng mỗi ngày (~20 KB mỗi
  file ở tốc độ demo) — rất nhiều file nhỏ, phase 8 sẽ compact.
- `rest.auth.type=oauth2` + `oauth2-server-uri`: nếu không khai báo, Iceberg 1.10 tự suy ra cả hai và
  log cảnh báo rằng cơ chế tự suy ra token endpoint sẽ bị bỏ ở bản sau.
- Việc ghi vào Iceberg là exactly-once (Kafka offset được commit cùng với Iceberg snapshot; đã kiểm
  tra bằng cách restart Kafka Connect giữa lúc đang ghi — không thiếu, không trùng `_offset`). Bản
  trùng vẫn có thể xuất hiện vì bản thân Debezium là at-least-once: sau khi crash, nó gửi lại các
  event kể từ LSN được flush cuối cùng. Silver xử lý chuyện đó (§6.2).
- Cách một lần commit diễn ra: coordinator (task giữ partition 0 của topic đầu tiên) gửi
  `START_COMMIT` lên control topic `control-iceberg` mỗi chu kỳ; mỗi worker trả lời bằng danh sách
  file nó đã ghi; coordinator commit chúng thành một snapshot cho mỗi bảng. Quan sát được: khi có
  dữ liệu chảy qua, một lần commit xong trong dưới 2 giây; khi các topic không có dữ liệu mới (và
  ngay sau khi khởi động) nó chờ hết `commit.timeout-ms` (30 giây), log `Commit timeout reached` và
  commit những gì đã nhận — thường là không có gì. Vô hại: không mất gì, lần commit sau sẽ lấy tiếp.
- Consumer group trong Kafka UI: `connect-iceberg-bronze` (các topic dữ liệu; offset chỉ tiến khi
  commit Iceberg thành công), `connect-iceberg-bronze-coord` và `cg-control-*` (control topic).
- `_ingested_at` là timestamp của record Kafka, tức lúc Debezium tạo event — không phải lúc sink ghi.
  Vì vậy các dòng của lần đọc bù đầu tiên mang thời điểm gốc của chúng. Nó không nói gì về thứ tự
  commit giữa các bảng; thứ tự luôn theo `source.lsn` (§6.2).
- `day(_ingested_at)` cắt ngày theo **UTC** (Iceberg lưu `timestamptz` theo UTC), trong khi stack chạy
  `Asia/Ho_Chi_Minh` (UTC+7): event từ 0h đến 7h giờ Việt Nam nằm ở partition của ngày hôm trước.
  Silver không bị ảnh hưởng (đọc theo snapshot), nhưng khi lọc theo ngày giờ Việt Nam trong Trino
  thì phải đổi múi giờ: `WHERE _ingested_at >= TIMESTAMP '2026-11-03 00:00 Asia/Ho_Chi_Minh'`.
  Giữ nguyên có chủ đích: `day()` của Iceberg không có tham số múi giờ, và partition pruning vẫn
  hoạt động — filter như trên chỉ đọc hai partition ngày UTC chồng lên khoảng đó. Các cách khác đã
  loại: thêm cột giờ Việt Nam cần tự viết SMT (không có SMT có sẵn nào đổi múi giờ);
  `hour(_ingested_at)` khớp được ngày Việt Nam nhưng số partition gấp 24 lần. Luôn lọc theo
  `_ingested_at`, không lọc theo giá trị partition.
- Sink, Trino và Spark đều dùng principal `root` của Polaris — đơn giản và đủ cho demo. Theo nguyên
  tắc quyền tối thiểu thì mỗi engine nên có principal riêng (ví dụ sink chỉ được ghi `bronze`); đây
  là hướng mở rộng, không nằm trong kế hoạch.

Bronze truy vấn được từ Trino ngay, ví dụ
`SELECT op, after.order_id, source.lsn FROM lakehouse.bronze.orders_cdc ORDER BY source.lsn DESC LIMIT 20`.
Các cột thời gian bên trong `before`/`after` vẫn là chuỗi ISO-8601 (`ZonedTimestamp`, §4.3); silver
sẽ parse.

## 6. Silver — SCD Type 2 trên Postgres

### 6.1. LSN trong một đoạn

PostgreSQL ghi mọi thay đổi vào WAL (write-ahead log) trước khi đụng tới file dữ liệu. **LSN** (Log
Sequence Number) là vị trí byte của một thay đổi trong WAL (ví dụ `0/16B3748`): tăng nghiêm ngặt
và không phụ thuộc đồng hồ. Debezium đặt nó vào `source.lsn` của mỗi event dưới dạng số nguyên
(`bigint`), nên sắp xếp và so sánh trực tiếp được. Replication slot nhớ LSN cuối cùng Debezium đã
xác nhận; Postgres giữ toàn bộ WAL sau đó, nên Debezium bị dừng sẽ làm WAL phình ra cho tới khi đầy
đĩa.

### 6.2. Quy tắc thứ tự và nhất quán

- **Sắp xếp event theo `source.lsn` theo từng key**, không bao giờ theo timestamp. Hai transaction
  không thể đồng thời sửa cùng một dòng (row lock), nên với một key, thứ tự LSN = thứ tự commit.
- **Idempotent:** silver lưu LSN của event cuối cùng đã apply cho mỗi key và chỉ apply event mới
  hơn. Debezium là at-least-once (§5), nên bản trùng là chuyện bình thường và vô hại.
- **Một version cho mỗi key trong mỗi transaction.** Generator insert một đơn hàng rồi update
  `total_amount` của nó trong cùng transaction: hai event, cùng key, cùng thời điểm commit. Chỉ
  event cuối của một key trong một transaction (`source.txId`) trở thành version; nếu không silver
  sẽ có version độ dài bằng 0 (`valid_from = valid_to`).
- **Nhất quán sau cùng giữa các bảng.** Mỗi bảng được xử lý riêng. Ba topic di chuyển độc lập, nên
  tại một thời điểm bronze có thể có một đơn hàng mà các dòng chi tiết chưa tới; silver có thể tạm
  hiển thị nó không có dòng nào. Lần chạy kế tiếp lấp chỗ trống. Điều này được chấp nhận cho demo.
  (Nhất quán chặt theo từng transaction sẽ cần `provide.transaction.metadata` của Debezium và chỉ
  apply những transaction đã đủ số event — có thể làm bài tập sau.)
- Delete trở thành version cuối với `is_deleted = true` (giá trị lấy từ `before`).

### 6.3. Các bảng silver

Database `data-platform`, schema `silver`, mỗi bảng nguồn một bảng, ví dụ `silver.orders`:

| Cột | Ý nghĩa |
|---|---|
| các cột nguồn | `order_id`, `user_id`, `status`, … |
| `valid_from`, `valid_to` | Khoảng hiệu lực của version, lấy từ `source.ts_ms` (thời điểm commit của transaction nguồn); `valid_to` null với version hiện tại |
| `is_current` | Đúng một version hiện tại cho mỗi key |
| `is_deleted` | Key đã bị xóa ở nguồn |
| `_lsn` | LSN của event sinh ra version này |
| `_silver_updated_at` | Lần cuối silver ghi dòng này — watermark cho gold và ES (§7, §8) |

Một partial unique index `(order_id) WHERE is_current` đảm bảo "một version hiện tại" luôn đúng và
giúp các truy vấn đọc của web app nhanh.

### 6.4. Job bronze → silver

Mỗi lần chạy `job_silver.py`, cho từng bảng:

1. Chỉ đọc dữ liệu bronze mới: một Iceberg incremental read (`start-snapshot-id`, không bao gồm,
   tới snapshot hiện tại của bảng) bắt đầu từ snapshot ID đã apply cuối cùng trong
   `silver.etl_checkpoint`. Lần chạy đầu chưa có checkpoint, nên đọc toàn bộ bảng. Bronze là
   append-only, đúng thứ incremental read hỗ trợ.
2. Biến event thành các dòng version: `after` cho `c`/`u`/`r`, `before` + `is_deleted = true` cho
   `d`; chỉ giữ event cuối của mỗi key trong mỗi transaction (§6.2). Trong batch, `valid_to` =
   `valid_from` của version kế tiếp cùng key (window `lead` sắp theo `source.lsn`); version cuối
   của mỗi key là `is_current`.
3. Ghi `staging.<table>` qua JDBC với `mode=overwrite` (bảng được tạo lại mỗi lần chạy, nên nhận
   được cột nguồn mới), cộng một dòng trong `staging.batch` với tên bảng và snapshot ID đã đọc tới.
4. Bàn giao cho Airflow (`silver_apply`), trong **một transaction Postgres**: thêm các cột staging
   còn thiếu vào silver (`ALTER TABLE ... ADD COLUMN`), chạy các câu lệnh SCD2 bên dưới, và chép
   snapshot ID từ `staging.batch` sang `silver.etl_checkpoint`. Nếu có gì lỗi, không gì thay đổi,
   và lần chạy sau đọc lại đúng khoảng đó.

Bước apply SCD2 gồm hai câu lệnh, vì `MERGE` chỉ làm được một hành động cho mỗi dòng nguồn khớp,
trong khi SCD2 cần cả "đóng version cũ" lẫn "chèn version mới":

```sql
-- 1) close the current version, using the OLDEST staging event that is newer than it.
--    Staging may also hold already-applied events (re-runs, Debezium duplicates); comparing
--    only min(_lsn) of the whole batch would miss the close and leave two current versions.
WITH nxt AS (
  SELECT DISTINCT ON (st.order_id) st.order_id, st.valid_from
  FROM staging.orders st
  JOIN silver.orders s ON s.order_id = st.order_id AND s.is_current
  WHERE st._lsn > s._lsn
  ORDER BY st.order_id, st._lsn
)
UPDATE silver.orders s
SET valid_to = nxt.valid_from, is_current = false, _silver_updated_at = now()
FROM nxt
WHERE s.order_id = nxt.order_id AND s.is_current;

-- 2) insert versions not applied yet (skip anything already seen -> idempotent)
INSERT INTO silver.orders (<columns from staging>, _silver_updated_at)
SELECT <columns from staging>, now()
FROM staging.orders st
WHERE NOT EXISTS (SELECT 1 FROM silver.orders s
                  WHERE s.order_id = st.order_id AND s._lsn >= st._lsn);
```

(Câu 1: đóng version hiện tại bằng event staging **cũ nhất** mà mới hơn nó. Staging có thể chứa cả
event đã apply rồi — do chạy lại hoặc bản trùng từ Debezium; nếu chỉ so `min(_lsn)` của cả batch
thì sẽ bỏ sót việc đóng version và để lại hai version hiện tại. Câu 2: chèn các version chưa apply,
bỏ qua những gì đã thấy → idempotent.)

Danh sách cột được task apply sinh ra từ các cột của `staging`, nên theo kịp schema evolution mà
không cần sửa tay.

## 7. Elasticsearch — tìm kiếm

Một task Airflow sau bước silver dựng mỗi đơn hàng thành một document (các trường của đơn hàng + tên
và thành phố của user + tên sản phẩm của các dòng — ES không có join), bỏ các trường nhạy cảm
(email, điện thoại), và bulk-upsert với `_id` = `order_id`.

Đơn hàng nào cần dựng lại: mọi đơn hàng mà chính dòng của nó, bất kỳ dòng chi tiết nào, **hoặc user
của nó** có `_silver_updated_at` > watermark của lần sync trước — một user đổi thành phố phải làm
mới mọi document đơn hàng của user đó. Đơn hàng có version hiện tại `is_deleted` bị xóa khỏi index.
Watermark là `_silver_updated_at` lớn nhất đã xử lý, lưu trong `silver.etl_checkpoint`. Muốn đổi
cấu trúc document thì cứ tạo lại index (demo — không chuyển alias).

## 8. Gold — ClickHouse

dbt-clickhouse dựng gold thẳng từ silver mà không cần bản sao trung gian. Các staging model là
`ephemeral` và đọc silver qua table function `postgresql()` (thông tin kết nối nằm trong một
ClickHouse named collection). Các gold model là `incremental` trên `ReplacingMergeTree`:

```sql
-- models/gold/fct_orders.sql
{{ config(materialized='incremental', incremental_strategy='append',
          engine='ReplacingMergeTree(_silver_updated_at, is_deleted)',
          order_by='order_id') }}
select ..., toUInt8(is_deleted) as is_deleted, _silver_updated_at
from {{ ref('stg_orders') }}            -- current versions only (is_current)
{% if is_incremental() %}
where _silver_updated_at > {{ get_watermark(this) }}
{% endif %}
```

- `append` + `ReplacingMergeTree`: version mới của một đơn hàng chỉ việc được ghi thêm, và engine
  giữ dòng có `_silver_updated_at` cao nhất cho mỗi `order_id`. Chiến lược incremental mặc định của
  dbt-clickhouse thì lại dựng lại bảng qua một bản sao tạm ở mỗi lần chạy.
- `is_deleted` là tham số thứ hai của engine, giúp `FINAL` ẩn các đơn hàng đã bị xóa ở nguồn.
- `ReplacingMergeTree` chỉ bỏ dòng cũ trong lúc background merge, nên truy vấn phải dùng `FINAL`.
- Watermark được lấy trước bởi một macro (`run_query` trên `max(_silver_updated_at)`, với giá trị
  dự phòng `1970-01-01` cho bảng rỗng) và chèn vào như một hằng số, để ClickHouse đẩy được bộ lọc
  xuống Postgres.
- Phải là `_silver_updated_at`, không phải `updated_at` của nguồn: việc đóng một version SCD2 chỉ
  đổi `valid_to`, điều mà timestamp nguồn không phản ánh.

Model: `dim_users`, `dim_date`, `fct_orders`, `fct_order_lines` để Superset tổng hợp, cộng vài mart
(doanh thu theo ngày, theo thành phố). Dimension lấy từ SCD2: `is_current` cho trạng thái hôm nay,
hoặc join theo khoảng `valid_from`/`valid_to` cho báo cáo tại một thời điểm.

## 9. API, trang web và Airflow

**API (FastAPI):** `GET /search?q=` → Elasticsearch; `GET /orders/{id}` và
`GET /orders/{id}/history` → silver; `GET /stats/...` → gold. Mọi con số tổng hợp đều lấy từ gold,
không bao giờ tính lại từ ES, nên trang web và dashboard luôn khớp nhau. Trang web là một trang
HTML tĩnh duy nhất do API phục vụ.

**Airflow — DAG `platform_pipeline`, `schedule=None`, kích hoạt thủ công:**

1. `silver_spark` — submit `job_silver.py` qua Spark REST 6066.
2. `silver_apply` — chạy SQL SCD2 (§6.4) trên Postgres qua `postgres_conn` và đẩy checkpoint lên.
3. Song song: `sync_es` và `dbt run` + `dbt test` trên ClickHouse.

Bronze không phải là một bước Airflow: Iceberg sink ghi nó liên tục.

**DAG `platform_maintenance`, cũng thủ công:**

- Iceberg compaction / expire snapshot / xóa orphan file trên bronze (Spark procedure;
  `remove_orphan_files` cần `prefix_listing => true` — nếu không nó liệt kê file qua Hadoop, mà ở
  đây Hadoop không có filesystem `s3://`, chỉ có `s3a://`).
  **Không bao giờ expire snapshot đang lưu trong `silver.etl_checkpoint`** (hoặc bất kỳ snapshot nào
  mới hơn): lần incremental read kế tiếp bắt đầu từ đó và sẽ fail nếu nó mất. Chỉ expire những
  snapshot cũ hơn checkpoint.
- Kiểm tra số dòng nguồn so với silver: dừng generator, chờ pipeline xử lý hết, chạy DAG silver,
  rồi so `count(*)` ở nguồn với số dòng `is_current AND NOT is_deleted` trong silver.
- Độ trễ replication slot trên `source-db`:
  `SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots;`

## 10. Kịch bản test

| Kịch bản | Cho thấy điều gì |
|---|---|
| Dừng Kafka Connect một lúc rồi khởi động lại | Debezium tiếp tục đúng LSN; trong lúc đó WAL dồn lại (theo dõi slot lag) |
| Dừng Kafka Connect cho tới khi slot lag vượt `max_slot_wal_keep_size` | Slot bị vô hiệu hóa; Debezium phải snapshot lại. Silver nhận một version mới (cùng giá trị) cho mỗi dòng, và các dòng **bị xóa trong khoảng gián đoạn sẽ bị bỏ sót** — snapshot không có event delete. Cho thấy đánh đổi của việc giới hạn slot |
| Thêm một cột nullable ở nguồn khi đang chạy | Schema mới được đăng ký; bronze và silver mở rộng theo |
| Đổi kiểu không tương thích | Registry từ chối; task Debezium fail |
| Đổi tên cột | Qua được `BACKWARD`; cột cũ âm thầm thành null trong bronze |
| Restart Kafka Connect giữa chừng | Event trùng không làm hỏng silver |
| Update một dòng nhiều lần trong vài mili giây | Thứ tự LSN chính xác |
| Xóa một đơn hàng (cascade xuống các dòng) | Delete nhiều bảng được xử lý |
| Chạy silver khi đơn hàng đang đổ vào | Tạm thời có đơn hàng không có dòng, được sửa ở lần chạy sau |
| Tăng vọt tốc độ generator | Độ trễ end-to-end, khả năng đuổi kịp |
| Kiểm tra số dòng sau khi xử lý hết | Silver khớp với nguồn |

## 11. Lộ trình

Các giai đoạn, task và tiêu chí "xong khi" nằm ở [roadmap.vi.md](roadmap.vi.md)
(bản tiếng Anh: [roadmap.md](roadmap.md)).
