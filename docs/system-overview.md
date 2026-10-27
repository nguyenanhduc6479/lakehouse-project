# Lakehouse local — Tổng quan hệ thống

> Trạng thái: hạ tầng đã chạy và kiểm thử đầu-cuối ngày **2026-10-27** (WSL2 Ubuntu + Docker
> Desktop). Pipeline medallion đã viết xong nhưng **chưa chạy end-to-end** vì nguồn dữ liệu
> `vnstock` đang bị PyPI quarantine (xem [mục 11](#11-trạng-thái--hạn-chế-đã-biết)).

## 1. Hệ thống này là gì

Một **lakehouse chạy local** dựng hoàn toàn bằng Docker Compose. Lưu trữ object, catalog bảng,
xử lý, truy vấn, điều phối và quản lý metadata đều chạy trên **một mạng Docker duy nhất**
(`lakehouse`), các service gọi nhau bằng tên container.

Bài toán demo là một **pipeline medallion** cho giá cổ phiếu Việt Nam theo ngày (OHLCV):

```
vnstock (OHLCV ngày) ──ingest──▶ MinIO  s3://lakehouse/bronze/_landing/…/*.parquet
   ──job_bronze──▶ lakehouse.bronze.stock_ohlcv          (thô, khử trùng lặp)
   ──job_silver──▶ lakehouse.silver.stock_ohlcv          (làm sạch + kiểm tra chất lượng)
   ──job_gold────▶ lakehouse.gold.stock_daily_metrics    (lợi suất, MA5/MA20, biên độ)
   ──────────────▶ truy vấn bằng Trino (catalog `lakehouse`)
```

Mỗi tầng là một **bảng Apache Iceberg** thật, đăng ký trong catalog **Apache Polaris**, dữ liệu
nằm trên **MinIO**. Vì vậy Spark ghi và Trino đọc được cùng một bảng.

Cố ý giữ nhỏ: 5 mã (`FPT, VNM, ACB, HPG, MWG`), mỗi lần chạy **ghi đè toàn bộ** bảng
(`createOrReplace`) để luồng dễ theo dõi; muốn incremental thì đổi sang `MERGE INTO`.

## 2. Kiến trúc

```
                         ┌──────────────────────────┐
                         │   Airflow 2.9.2          │  điều phối: DAG lakehouse_ohlcv
                         │   webserver · scheduler  │  (18:00 thứ 2–6)
                         └─────┬──────────────┬─────┘
              ingest (Python)  │              │  submit job qua REST :6066
                               ▼              ▼
┌───────────┐  parquet  ┌────────────┐   ┌──────────────────────────────┐
│ vnstock   │──────────▶│   MinIO    │◀──│ Spark 3.5.1 cluster          │
│ (nguồn)   │           │  (S3, data)│   │ master · worker · thrift ·   │
└───────────┘           └─────▲──────┘   │ history                      │
                              │          └──────────────┬───────────────┘
                     đọc/ghi  │                         │ Iceberg REST + OAuth2
                     data file│                         ▼
                        ┌─────┴──────┐  REST+OAuth2 ┌────────────────────┐   JDBC  ┌──────────┐
                        │ Trino 454  │─────────────▶│ Polaris 1.5.0      │────────▶│ Postgres │
                        │ (truy vấn) │              │ (Iceberg catalog)  │         │    15    │
                        └────────────┘              └────────────────────┘         └────▲─────┘
                                                                                        │
   DataHub v0.12.1 (lineage/metadata) ── Kafka · Zookeeper · Elasticsearch · schema-registry ──┘
```

`docker-compose.yml` chỉ `include` 7 module trong `compose/` và khai báo mạng chung:

| Module | Service chính |
|---|---|
| `infra.yml` | Postgres 15, Zookeeper, Kafka, Elasticsearch 7.17 |
| `storage.yml` | MinIO |
| `catalog.yml` | Polaris (+ bootstrap, setup) |
| `datahub.yml` | DataHub GMS, frontend, actions, schema-registry |
| `spark.yml` | Spark master, worker, thrift, history |
| `trino.yml` | Trino |
| `airflow.yml` | Airflow init, webserver, scheduler |

## 3. Danh sách service (27 container)

### Service chạy lâu dài (17)

| Service | Image | Vai trò | Cổng host |
|---|---|---|---|
| `postgres` | `postgres:15` | DB dùng chung cho Airflow, Polaris, DataHub | 5432 |
| `minio` | `minio/minio` | Object storage S3, chứa toàn bộ data file | 9000 (API), 9001 (console) |
| `polaris` | `apache/polaris:1.5.0` | Iceberg REST catalog, OAuth2 | 8181 |
| `spark-master` | `custom-spark:3.5.1` | Spark master + REST submit | 8090 (UI), 7077 (RPC), 6066 (REST) |
| `spark-worker` | `custom-spark:3.5.1` | Executor/driver (2 core, 2 GB) | 8081 |
| `spark-thrift` | `custom-spark:3.5.1` | Hive Thrift JDBC cho dbt | 10000, 4040 (UI) |
| `spark-history` | `custom-spark:3.5.1` | Spark History Server | 18080 |
| `trino` | `trinodb/trino:454` | SQL query engine | 8080 |
| `airflow-webserver` | `custom-airflow:2.9.2` | Airflow UI | 8085 |
| `airflow-scheduler` | `custom-airflow:2.9.2` | Lập lịch + chạy task (LocalExecutor) | — |
| `zookeeper` | `confluentinc/cp-zookeeper:7.6.0` | Điều phối Kafka | — |
| `kafka` | `confluentinc/cp-kafka:7.6.0` | Message bus cho DataHub | — |
| `elasticsearch` | `elasticsearch:7.17.10` | Search + graph store cho DataHub | — |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.0` | Schema registry DataHub cần | — |
| `datahub-gms` | `linkedin/datahub-gms:v0.12.1` | API metadata | 8083 |
| `datahub-frontend-react` | `linkedin/datahub-frontend-react:v0.12.1` | UI DataHub | 9002 |
| `datahub-actions` | `acryldata/datahub-actions:v0.0.14` | Actions framework | — |

"—" = chỉ truy cập được trong mạng Docker (đã kiểm tra bằng `docker ps`). Kafka quảng bá listener
`localhost:9092` nhưng **không publish cổng**, nên từ host không kết nối được.

### Job chạy một lần (10)

Các job này chạy xong thoát mã `0`; service phía sau chờ bằng `service_completed_successfully`.

| Job | Làm gì |
|---|---|
| `postgres-setup` | Tạo role + database `airflow`, `polaris`, `datahub` (idempotent) |
| `minio-setup` | Tạo bucket `lakehouse` với prefix `bronze/ silver/ gold/` (bỏ qua nếu đã có) |
| `polaris-bootstrap` | `polaris-admin-tool bootstrap`: tạo schema + realm `POLARIS` trong Postgres |
| `polaris-setup` | Tạo/cập nhật catalog `lakehouse`, cấp quyền, tạo namespace |
| `spark-fix-permissions` | `chown 185` volume event log để Spark (UID 185) ghi được |
| `airflow-init` | `db migrate`, tạo admin, tạo 5 Airflow connection |
| `kafka-setup` | Tạo trước 5 topic DataHub |
| `datahub-elasticsearch-setup` | Tạo index Elasticsearch |
| `datahub-postgres-setup` | Tạo schema DataHub trong Postgres |
| `datahub-upgrade` | `SystemUpdate` của DataHub |

## 4. Chuỗi khởi động

Phần lớn `depends_on` dùng `service_healthy` hoặc `service_completed_successfully`, nên các
service khởi động đúng thứ tự, không chạy đua nhau:

```
postgres ─▶ postgres-setup ─┬─▶ polaris-bootstrap ─▶ polaris ─┬─▶ polaris-setup (cần cả minio-setup)
                            │                                 └─▶ trino
                            └─▶ airflow-init ─▶ airflow-webserver, airflow-scheduler
minio ─▶ minio-setup
spark-master ─▶ spark-worker, spark-history;  spark-fix-permissions + spark-master ─▶ spark-thrift
zookeeper ─▶ kafka ─┬─▶ kafka-setup ─────────────────┐
                    └─▶ schema-registry ─────────────┤
elasticsearch ─▶ datahub-elasticsearch-setup ────────┼─▶ datahub-upgrade ─▶ datahub-gms ─┬─▶ datahub-frontend-react
postgres-setup ─▶ datahub-postgres-setup ────────────┘                                  └─▶ datahub-actions
```

Lần `docker compose up -d` thứ N vẫn thành công: mọi job setup đều idempotent (đã kiểm tra).

## 5. Luồng dữ liệu medallion

| Tầng | Code | Đầu vào → đầu ra | Logic chính |
|---|---|---|---|
| **Ingest** | `airflow/plugins/ingest_ohlcv.py` | `vnstock` → `s3://lakehouse/bronze/_landing/stock_ohlcv/ingest_date=YYYY-MM-DD/data.parquet` | Kéo OHLCV từng mã (nguồn VCI), gắn `symbol`, `source`, `ingested_at`; ghi parquet qua `boto3`. Mã lỗi thì bỏ qua; không mã nào có dữ liệu thì fail. |
| **Bronze** | `spark/jobs/job_bronze.py` | landing parquet → `lakehouse.bronze.stock_ohlcv` | `time` → `trade_date`; khử trùng lặp theo `(symbol, trade_date)`, giữ bản có `ingested_at` mới nhất. Schema đọc khai báo tường minh. |
| **Silver** | `spark/jobs/job_silver.py` | bronze → `lakehouse.silver.stock_ohlcv` | Luật chất lượng: khoá không null, giá > 0, volume ≥ 0, ràng buộc nến (`high ≥ low`, `high ≥ max(open, close)`, `low ≤ min(open, close)`). Log số dòng giữ/loại; fail nếu rỗng. |
| **Gold** | `spark/jobs/job_gold.py` | silver → `lakehouse.gold.stock_daily_metrics` | `prev_close`, `daily_return`, `ma5`, `ma20`, `range_pct`. Window tính **riêng trong từng mã** theo `trade_date`. Fail nếu rỗng. |
| **Query** | Trino | bảng gold | `SELECT … FROM lakehouse.gold.stock_daily_metrics` |

Đặc điểm chung:

- **Idempotent.** Ingest ghi đè file theo `ingest_date`, nên chạy lại trong ngày không sinh rác.
  Các job Spark dùng `createOrReplace()`.
- **Bảng Iceberg** dùng format v2, partition theo `symbol`, nằm tại
  `s3://lakehouse/<namespace>/<bảng>` (location mặc định của catalog).
- **Logic transform tách thành hàm thuần** (`transform_bronze`, `clean_silver`, `build_gold`),
  nên test được bằng DataFrame dựng tay, không cần MinIO hay Polaris.

## 6. Catalog: Apache Polaris

| Khía cạnh | Cấu hình |
|---|---|
| Endpoint | `http://polaris:8181/api/catalog`, warehouse/prefix `lakehouse` |
| Xác thực | OAuth2 client-credentials `root` / `POLARIS_SECRET`, **scope `PRINCIPAL_ROLE:ALL` bắt buộc** |
| Lưu metadata | Postgres qua `POLARIS_PERSISTENCE_TYPE=relational-jdbc` (database `polaris`) |
| Storage của catalog | S3: `endpoint` = MinIO, `pathStyleAccess=true`, `stsUnavailable=true`, `region=us-east-1`, `allowedLocations=s3://lakehouse/` |
| Quyền | `catalog_admin` được cấp thêm `CATALOG_MANAGE_CONTENT` (cần cho `DROP TABLE` purge và báo cáo metrics) |
| Thuộc tính catalog | `polaris.config.drop-with-purge.enabled=true` |
| Namespace | `bronze`, `silver`, `gold`, tạo qua Iceberg REST API |
| Health | `/q/health` trên cổng 8182, **chỉ trong container** |

Vì `stsUnavailable=true`, Polaris **không cấp credential tạm** (credential vending) cho client.
Mỗi engine tự mang key MinIO của mình. Bản thân Polaris đọc/ghi file metadata bằng biến môi
trường `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.

## 7. Engine kết nối catalog như thế nào

| | Spark (`spark/jobs/common.py`) | Trino (`trino/catalog/lakehouse.properties`) |
|---|---|---|
| Catalog | `SparkCatalog`, `type=rest`, `uri`, `warehouse=lakehouse` | `iceberg.catalog.type=rest`, `rest-catalog.uri`, `warehouse` |
| OAuth2 | `credential=root:<secret>`, `scope=PRINCIPAL_ROLE:ALL` | `oauth2.credential`, `oauth2.scope` (cần Trino ≥ 454) |
| Data file | `S3FileIO` + `s3.endpoint`, `path-style-access`, key MinIO | `fs.native-s3.enabled`, `s3.endpoint`, `s3.region`, `s3.path-style-access`, key |
| Đọc landing | `fs.s3a.*` (hadoop-aws) để đọc parquet thô | — |
| Secret | env `MINIO_*`, `POLARIS_SECRET` | `${ENV:MINIO_*}`, `${ENV:POLARIS_SECRET}` |

Image `custom-spark:3.5.1` (từ `apache/spark:3.5.1`) có thêm các JAR: `hadoop-aws 3.3.4`,
`iceberg-spark-runtime-3.5_2.12 1.5.2`, `iceberg-aws-bundle 1.5.2`, `postgresql 42.6.0`,
`acryl-spark-lineage 0.2.18`, `kafka-clients 3.6.0`; và Python `pandas`, `pyarrow`, `fastparquet`.
Python trong container là **3.8.10**. `spark-defaults.conf` chỉ cấu hình event log và múi giờ;
cấu hình catalog nằm trong `common.py`.

## 8. Điều phối: Airflow

- **DAG** `lakehouse_ohlcv` (`airflow/dags/lakehouse_ohlcv_dag.py`): `ingest_landing` →
  `build_bronze` → `build_silver` → `build_gold`. Lịch `0 18 * * 1-5` (18:00 thứ 2–6, giờ
  `Asia/Ho_Chi_Minh`), `catchup=False`, `retries=1`. DAG mới tạo mặc định ở trạng thái **paused**.
- **Submit Spark** qua **Spark Standalone REST API** `spark-master:6066`
  (`airflow/plugins/spark_rest.py`), deploy mode **cluster**: driver chạy trên `spark-worker`,
  nên Airflow không cần Java hay `spark-submit`. Task chờ trạng thái driver `FINISHED` (timeout
  30 phút); gặp `FAILED`, `ERROR` hoặc `KILLED` thì fail.
- **Ingest** chạy ngay trong scheduler (`PythonOperator`), đọc `MINIO_*` từ env.
- **Connections** do `airflow/scripts/init.sh` tạo bằng code (xoá rồi tạo lại):
  `spark_rest_conn`, `minio_conn`, `postgres_conn`, `trino_conn`, `datahub_conn`.
- Image `custom-airflow:2.9.2` (từ `apache/airflow:2.9.2-python3.11`) cài `dbt-core`/`dbt-spark`
  1.8.0, các provider http/amazon/postgres/trino, `acryl-datahub[airflow]` 0.12.1.5,
  `pandas 2.1.4`, `pyarrow 14.0.2`, dùng **constraints chính thức của Airflow 2.9.2**.

## 9. Metadata & lineage: DataHub

DataHub **v0.12.1** (khớp `acryl-datahub[airflow]==0.12.1.5`), chuyển từ bản quickstart
MySQL sang dùng hạ tầng của repo:

- **Postgres** (database `datahub`), **Kafka** `kafka:29092`, **Elasticsearch** làm graph store
  (không có Neo4j). `schema-registry` được thêm vào vì DataHub bắt buộc phải có.
- `datahub-gms` chạy các consumer MAE/MCE/PE ngay bên trong. UI ở cổng 9002.
- Spark gửi lineage qua JAR `acryl-spark-lineage`, Airflow gửi qua `datahub_conn`.
  *Chưa kiểm chứng lineage thực tế trên UI.*

## 10. Cấu hình, secret và lưu trữ

**Mọi secret và tham số nằm trong `.env`** (đã gitignore, không push lên git), được inject vào
container qua `environment:`. Các nhóm biến:

| Nhóm | Biến |
|---|---|
| Postgres | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_DB_AIRFLOW`, `POSTGRES_DB_POLARIS`, `POSTGRES_DB_DATAHUB` |
| MinIO | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT`, `MINIO_BUCKET` |
| Polaris | `POLARIS_SECRET`, `POLARIS_ADMIN_USERNAME`, `POLARIS_ADMIN_PASSWORD` |
| Airflow | `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_FERNET_KEY` |
| DataHub | `DATAHUB_ADMIN_USERNAME`, `DATAHUB_ADMIN_PASSWORD`, `DATAHUB_SECRET_KEY` |

**Named volumes** (có tiền tố `lakehouse-project_`): `postgres_data_vol`, `minio_data_vol`,
`spark_events_vol`, `airflow_logs_vol`, `kafka_data_vol`, `kafka_secret_vol`,
`zookeeper_data_vol`, `zookeeper_log_vol`, `zookeeper_secret_vol`, `elasticsearch_data_vol`.
`docker compose down` giữ lại volume; muốn xoá sạch thì thêm `-v`.

**Bố cục MinIO** (bucket `lakehouse`):

```
s3://lakehouse/
├── bronze/
│   ├── _landing/stock_ohlcv/ingest_date=YYYY-MM-DD/data.parquet   ← ingest ghi
│   └── stock_ohlcv/   (data/ + metadata/ của bảng Iceberg)
├── silver/stock_ohlcv/
└── gold/stock_daily_metrics/
```

Quy ước: mọi service dùng múi giờ `Asia/Ho_Chi_Minh`. Mỗi `compose/*.yml` tự khai báo lại mạng
`lakehouse` và `volumes:` riêng để có thể merge độc lập. Script shell và comment trong `.env`
viết bằng tiếng Việt.

## 11. Trạng thái & hạn chế đã biết

**Đã kiểm thử (2026-10-27):**

- 27/27 service ổn: 17 đang chạy (16 healthy; `datahub-actions` không có healthcheck) và 10 job
  setup thoát mã 0. Chạy `up -d` lại vẫn thoát mã 0.
- Cả 10 cổng host đều trả lời.
- Spark ghi và đọc được bảng Iceberg qua Polaris xuống MinIO.
- **Trino đọc được bảng do Spark ghi**; Trino tạo bảng (CTAS) và `DROP TABLE` được.
- Restart Polaris không mất catalog hay namespace.
- Airflow nạp được DAG, không có import error, đủ 5 connection, gọi tới được Spark REST,
  Polaris, MinIO, Trino và DataHub.
- pytest: 4/4 trên host, 7/7 test Spark trong container.

**Hạn chế:**

| Vấn đề | Ảnh hưởng | Ghi chú |
|---|---|---|
| `vnstock` (và `vnai`) bị **PyPI quarantine** từ 2026-09-24 | Task `ingest_landing` sẽ lỗi; pipeline chưa chạy end-to-end | `vnstock` đã comment khỏi `airflow/requirements.txt`. Hướng xử lý: dữ liệu giả để test, hoặc gọi thẳng endpoint VCI/KBS |
| dbt chưa có model; `dbt_project.yml` rỗng | Chưa dùng được dbt | `spark-thrift` cũng chưa có cấu hình catalog Polaris |
| `container_name` cố định | Mỗi Docker host chỉ chạy được một bản của stack | |
| Python khác nhau giữa các nơi | Code job phải chạy được trên Python 3.8 | Spark 3.8, Airflow 3.11, venv host 3.11 |
| `pyarrow` lệch | Airflow 14.0.2 (bắt buộc theo constraints) và `pyproject.toml` 15.0.2 | |
| Kafka/ES không có cổng trên host | Không kết nối từ host được | Cần thêm `ports:` nếu muốn dùng |
| Lineage DataHub | Chưa kiểm chứng trên UI | |

## 12. Vận hành

Chạy **trong WSL**, tại thư mục gốc repo:

```bash
docker compose config --quiet          # kiểm tra 7 module merge hợp lệ
docker compose build                   # build custom-spark, custom-airflow
docker compose up -d                   # bật toàn bộ stack
docker compose ps -a                   # trạng thái + health
docker compose logs -f <service>
docker compose down                    # dừng (giữ volume)
```

Kiểm tra nhanh:

```bash
docker exec trino trino --execute "SHOW SCHEMAS FROM lakehouse"   # bronze gold information_schema silver
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8181/api/catalog/v1/config  # 401 = Polaris đang chạy
```

Chạy job Spark bằng tay (lưu ý `spark-submit` **không nằm trong PATH**):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --py-files /opt/spark/jobs/common.py /opt/spark/jobs/job_bronze.py   # tương tự silver, gold
```

Test:

```bash
venv/bin/python -m pytest -m "not spark"        # host (WSL), không cần Java
# test Spark: chạy trong spark-master (xem CLAUDE.md, mục Common commands)
```

Giao diện web: MinIO console `:9001` · Spark `:8090` · Spark history `:18080` · Trino `:8080` ·
Airflow `:8085` · DataHub `:9002`.

## 13. Phụ lục: những bẫy đã gặp khi dựng

| Bẫy | Cách nhận biết | Đã xử lý |
|---|---|---|
| Polaris mặc định dùng metastore **in-memory** | Log có "intended for tests only"; database `polaris` không có bảng | Bật `relational-jdbc` và thêm `polaris-bootstrap` |
| `admin-tool bootstrap` chạy lần 2 thoát mã 3 (tài liệu nói idempotent) | "already been bootstrapped" | Bọc bằng `sh`, coi lỗi này là thành công |
| Cấp lại grant đã có thì trả HTTP 500 | `duplicate key … grant_records_pkey` | `init.sh` đọc danh sách grant trước rồi mới cấp |
| Namespace tạo qua Management API thì nhận 404 | `curl -s` vẫn thoát mã 0 nên script in "created" | Dùng Iceberg REST API và kiểm tra `%{http_code}` |
| Trino 435 không đặt được OAuth2 scope | `invalid_scope` | Nâng lên Trino 454, thêm `oauth2.scope` |
| Trino không có credential S3 | `Unable to load AWS credentials` | Thêm cấu hình `fs.native-s3.*` |
| `pyarrow 15.0.2` mâu thuẫn với constraints Airflow | pip `ResolutionImpossible` | Hạ xuống 14.0.2 |
| Shell WSL mở từ trước khi bật Docker integration | `permission denied … docker.sock` | Mở shell mới hoặc chạy `newgrp docker` |
| CDN Docker Hub đứt kết nối | `httpReadSeeker … EOF` | Chạy lại lệnh |
