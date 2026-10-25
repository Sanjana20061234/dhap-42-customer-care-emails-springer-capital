# Customer Care Emails Ingestion Pipeline (DHAP-42)

This pipeline extracts a local CSV of customer care email threads, validates the records against a schema contract, cleans and transforms the data, and publishes partitioned Parquet files to MinIO S3 object storage.

The entire environment runs under Docker Compose, comprising Apache Airflow, PostgreSQL (metadata database), and MinIO.

## Architecture

```text
+-----------------------------------------------------------------------------------+
|                               Airflow DAG Tasks                                   |
|                                                                                   |
|  [ sample.csv ]                                                                   |
|         |                                                                         |
|         v                                                                         |
|  +--------------+       +-------------------+       +-------------+               |
|  |   read_csv   | ----> | validate_contract | ----> |  transform  |               |
|  +--------------+       +-------------------+       +-------------+               |
|                                                            |                      |
|                                                            v                      |
|                                                 +------------------------+        |
|                                                 | write_parquet_to_minio |        |
|                                                 +------------------------+        |
+-------------------------------------------------------------|---------------------+
                                                              |
                                                              v
+-----------------------------------------------------------------------------------+
|                                  MinIO S3                                         |
|                                                                                   |
|   Bucket: customer-care-emails                                                    |
|   Dataset Prefix: customer_care_emails/                                           |
|                                                                                   |
|   customer_care_emails/                                                           |
|     ├── email_date=2023-08-12/part-0.parquet                                      |
|     ├── email_date=2023-08-14/part-0.parquet                                      |
|     └── ... (24 daily partitions, 2,259 total rows)                               |
+-----------------------------------------------------------------------------------+
```

## Project Files

| File / Path | Purpose |
|---|---|
| `manifest.yaml` | Dataset metadata, source URL, license, row count, checksum, and status |
| `schema_contract.yaml` | Strict schema rules, column order, types, allowed categories, and constraints |
| `docker-compose.yaml` | Airflow, PostgreSQL metadata DB, MinIO server, and bucket initialization |
| `Dockerfile` | Custom Airflow image with pinned dependencies (`pandas`, `pyarrow`, `PyYAML`, `boto3`) |
| `.env.example` | Template for required environment variables with `change_me` placeholders |
| `.env` | Local environment secrets (not committed to Git) |
| `.gitignore` | Ignores `.env`, caches, temporary files, and runtime outputs |
| `dags/csv_to_parquet_minio.py` | Airflow DAG orchestrating read, validate, transform, and MinIO write |
| `dags/extraction/sample.csv` | Local copy of source dataset (2,259 rows, 13 columns) |
| `tests/make_bad_csvs.py` | Generator for 8 negative test CSV fixtures |
| `tests/data/` | Test data directory containing generated invalid CSVs |
| `README.md` | Practical documentation and operating instructions |

## Data Source

- Dataset: `rtweera/customer_care_emails` on Hugging Face: https://huggingface.co/datasets/rtweera/customer_care_emails
- License: GPL-3.0
- Total rows: 2,259 rows
- Total columns: 13 source columns

## Prerequisites

- Docker Desktop with Docker Compose v2 (Engine 24+)
- On Windows: WSL2 backend enabled
- Free ports on host:
  - `8080` (Airflow webserver)
  - `9000` (MinIO S3 API)
  - `9001` (MinIO web console)

## Environment Setup

1. Copy the example configuration to `.env`:

   PowerShell:
   ```powershell
   Copy-Item .env.example .env
   ```

   Bash:
   ```bash
   cp .env.example .env
   ```

2. Open `.env` and fill in fresh strong passwords for the placeholders marked `change_me`.
   Never commit `.env` to Git.

   Required keys in `.env`:
   - `AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5-python3.11`
   - `AIRFLOW_UID=50000`
   - `_AIRFLOW_WWW_USER_USERNAME=admin`
   - `_AIRFLOW_WWW_USER_PASSWORD=<your-strong-password>`
   - `AIRFLOW_DB_USER=airflow`
   - `AIRFLOW_DB_PASSWORD=<your-strong-password>`
   - `AIRFLOW_DB_NAME=airflow`
   - `AIRFLOW_WEBSERVER_SECRET_KEY=<your-random-secret-key>`
   - `MINIO_ROOT_USER=minioadmin42`
   - `MINIO_ROOT_PASSWORD=<your-strong-password>`
   - `MINIO_BUCKET=customer-care-emails`
   - `MINIO_API_PORT=9000`
   - `MINIO_CONSOLE_PORT=9001`

3. Generate test fixture CSVs:

   PowerShell / Bash:
   ```bash
   python tests/make_bad_csvs.py
   ```

## Starting the Stack

1. Build images and start all containers:

   PowerShell / Bash:
   ```bash
   docker compose up -d --build
   ```

   The first run builds `dhap42-airflow:2.10.5` with pinned dependencies and initializes the databases.
   The `minio-init` container exits with code 0 once the bucket `customer-care-emails` is created.
   The `airflow-init` container exits with code 0 once database migrations and admin user creation complete.

2. Verify that services are healthy:

   PowerShell / Bash:
   ```bash
   docker compose ps
   ```

   Expected output:
   - `airflow-metadata-db`: Up (healthy)
   - `minio-server`: Up (healthy)
   - `airflow-scheduler`: Up (healthy)
   - `airflow-webserver`: Up (healthy)

## Accessing the Web Consoles

- Airflow Web UI: `http://localhost:8080`
  - Username: `admin` (or the value set in `_AIRFLOW_WWW_USER_USERNAME`)
  - Password: stored in your local `.env` (`_AIRFLOW_WWW_USER_PASSWORD`)

- MinIO Console: `http://localhost:9001`
  - Username: `minioadmin42` (or the value set in `MINIO_ROOT_USER`)
  - Password: stored in your local `.env` (`MINIO_ROOT_PASSWORD`)

## Running the Pipeline

### Trigger via Web UI

1. Open `http://localhost:8080` and log in.
2. Locate `csv_to_parquet_minio`.
3. Click the Play icon and select "Trigger DAG".
4. Open the Grid view to observe all four tasks execute in order:
   `read_csv` -> `validate_contract` -> `transform` -> `write_parquet_to_minio`

### Trigger via Command Line

PowerShell / Bash:
```bash
docker exec airflow-scheduler airflow dags trigger csv_to_parquet_minio
```

Check the run status:
```bash
docker exec airflow-scheduler airflow dags list-runs -d csv_to_parquet_minio
```

Check task states:
```bash
docker exec airflow-scheduler airflow tasks states-for-dag-run csv_to_parquet_minio <run_id>
```

All 4 tasks must show `success`.

## Inspecting Parquet Output in MinIO

### Inspect via MinIO Web Console

1. Navigate to `http://localhost:9001`.
2. Open Buckets -> `customer-care-emails`.
3. Open `customer_care_emails/`.
4. Inspect the 24 daily partition folders:
   `email_date=2023-08-12/` through `email_date=2023-12-20/`.
   Each folder contains one Parquet file.

### Inspect via CLI with Python and PyArrow

Run inside the container:
```bash
docker exec airflow-scheduler python -c "
import boto3, io, os, pyarrow.parquet as pq, pyarrow as pa
s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id=os.getenv('MINIO_ROOT_USER'), aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'))
objs = [o['Key'] for o in s3.list_objects_v2(Bucket='customer-care-emails', Prefix='customer_care_emails/').get('Contents', [])]
print('Total objects:', len(objs))
tables = [pq.read_table(io.BytesIO(s3.get_object(Bucket='customer-care-emails', Key=k)['Body'].read())) for k in objs]
full = pa.concat_tables(tables)
print('Total rows:', full.num_rows)
print('Schema:\n', full.schema)
"
```

Verified values:
- Total objects: 24
- Total partitions: 24
- Total rows: 2,259
- `email_key`: unique 64-character SHA-256 hash
- `timestamp`: timezone-aware UTC `timestamp[us, tz=UTC]`
- `email_types`: `list<element: string>`
- `product_types`: `list<element: string>`
- `customer_satisfaction`: `double`
- Null counts: 0 across all columns

## Rerun Idempotency

Triggering the DAG multiple times produces identical results without accumulating duplicate records:
- The staging directory writes locally first.
- The pipeline deletes old objects under `customer_care_emails/` only before publishing new files.
- The row count remains exactly 2,259.
- The file count remains exactly 24.

## Running Negative Tests

The test suite generates 8 invalid CSVs in `tests/data/`:
1. `bad_header_renamed.csv`: header `subject` renamed to `subject_x`
2. `bad_missing_column.csv`: required column `thread_id` removed
3. `bad_customer_satisfaction.csv`: non-numeric value in `customer_satisfaction`
4. `bad_empty_non_nullable.csv`: empty whitespace in non-nullable `sender`
5. `bad_invalid_status.csv`: invalid `email_status` value (`pending`)
6. `bad_invalid_timestamp.csv`: invalid timestamp format (`invalid_timestamp_2023`)
7. `bad_malformed_list.csv`: malformed Python list syntax in `email_types`
8. `bad_duplicate_key.csv`: duplicate record producing non-unique derived `email_key`

Trigger any negative test by supplying `csv_filename`:

PowerShell / Bash:
```bash
docker exec airflow-scheduler airflow dags trigger csv_to_parquet_minio --conf "{\"csv_filename\": \"/opt/airflow/project/tests/data/bad_header_renamed.csv\"}"
```

### Expected Behavior on Failure

For every negative test:
1. `read_csv` completes successfully.
2. `validate_contract` fails immediately (`retries=0`, `AirflowFailException`).
3. `transform` and `write_parquet_to_minio` are marked `upstream_failed` and never run.
4. The MinIO bucket contents remain untouched (24 files, 2,259 rows).

Example error messages from contract enforcement:
```text
airflow.exceptions.AirflowFailException: Schema contract violation: missing=['subject'], extra=['subject_x']
airflow.exceptions.AirflowFailException: Schema contract violation: missing=['thread_id'], extra=[]
airflow.exceptions.AirflowFailException: Schema contract violation: 1 error(s) detected. First errors: Row 5: customer_satisfaction contains non-numeric value: 'not_a_number'
airflow.exceptions.AirflowFailException: Schema contract violation: 1 error(s) detected. First errors: Row 10: column 'sender' is empty or whitespace-only
airflow.exceptions.AirflowFailException: Schema contract violation: 1 error(s) detected. First errors: Row 15: invalid email_status 'pending'
airflow.exceptions.AirflowFailException: Schema contract violation: 1 error(s) detected. First errors: Row 20: timestamp parsing failed: Unknown string format: invalid_timestamp_2023
airflow.exceptions.AirflowFailException: Schema contract violation: 1 error(s) detected. First errors: Row 25: malformed email_types python list: '[' was never closed (<unknown>, line 1)
airflow.exceptions.AirflowFailException: Schema contract violation: 1 error(s) detected. First errors: Row 2259: email_key is not unique (duplicate key '372d555c...')
```

## Data Profiling Findings

During initial exploratory analysis of `customer_care_emails.csv`:

1. **Repeating Thread IDs**: Emails in a thread share the same `thread_id`. The unique primary key `email_key` is derived via SHA-256:
   `sha256(f"{thread_id}|{utc_timestamp_iso}|{sender}|{md5(cleaned_body)}")`.
   Across all 2,259 rows, all candidate keys are 100% unique.
2. **Timestamp Variations**: Timestamps appear in six distinct formats:
   - Offset without milliseconds (1,055 rows): `2023-10-26 10:02:34+00:00`
   - UTC `Z` without milliseconds (1,007 rows): `2023-10-26T10:15:22Z`
   - UTC `Z` with milliseconds (105 rows): `2023-10-26T14:32:51.924Z`
   - Offset with milliseconds (53 rows): `2023-10-26 14:32:51.953000+00:00`
   - Naive without timezone without milliseconds (35 rows): `2023-10-26 10:12:34`
   - Naive without timezone with milliseconds (4 rows): `2023-10-26T14:05:22.937563`
   All timestamps represent UTC times; timestamps without explicit timezones are interpreted as UTC.
3. **Product Name Variations**: `product_types` contains 3,562 string items across 10 capitalization variations of 5 products (`API Development`, `API Monitoring`, `Cloud Management`, `IAM Service`, `Mercury Language`). These are normalized case-insensitively using `PRODUCT_CANONICAL`.
4. **List Fields Stored as Strings**: List columns like `['inquiry']` are parsed with `ast.literal_eval` and converted to true Arrow list types `list<element: string>`.
5. **Mojibake Sequences**: 3 occurrences of `â€™` exist in message bodies; these are replaced with `’` in `clean_text`.
6. **Partition Range**: Extracted UTC dates span 24 distinct days between `2023-08-12` and `2023-12-20`. Daily partitioning `email_date=YYYY-MM-DD` produces 24 partitions.

## Stopping and Resetting

Stop the stack while preserving data volumes:

PowerShell / Bash:
```bash
docker compose down
```

Reset the local environment completely:

> [!WARNING]
> Running `docker compose down -v` permanently deletes local persistent volumes (`postgres-airflow-data` and `minio-data`).

PowerShell / Bash:
```bash
docker compose down -v
```

Restart from clean state:
```bash
docker compose up -d --build
```

## Troubleshooting

- **Port 8080, 9000, or 9001 in use**: Stop any existing processes or Docker containers using those ports (`docker ps`, `docker stop <id>`).
- **Logs tab in Airflow UI shows 403 Forbidden**: Ensure `AIRFLOW_WEBSERVER_SECRET_KEY` in `.env` is identical across webserver and scheduler services.
- **DAG does not appear in Airflow UI**: Run `docker exec airflow-scheduler airflow dags list-import-errors`. If none, wait 30 seconds for scheduler folder refresh.
- **MinIO connection error from Airflow**: Ensure the Docker Compose environment sets `AIRFLOW_CONN_MINIO_S3` pointing to `http://minio:9000` inside the Docker network.
