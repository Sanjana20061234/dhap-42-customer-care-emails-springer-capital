import ast
import hashlib
import logging
import os
import shutil
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import boto3
import dateutil.parser
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

# Base configuration paths
BASE_DIR = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow"))
PROJECT_DIR = Path(os.getenv("PROJECT_DIR", "/opt/airflow/project"))
TEMP_DIR = Path(os.getenv("TEMP_DIR", "/opt/airflow/temp"))
TEMP_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = PROJECT_DIR / "manifest.yaml"
if not MANIFEST_PATH.exists():
    MANIFEST_PATH = BASE_DIR / "project" / "manifest.yaml"
if not MANIFEST_PATH.exists():
    MANIFEST_PATH = Path("/opt/airflow/project/manifest.yaml")

SCHEMA_PATH = PROJECT_DIR / "schema_contract.yaml"
if not SCHEMA_PATH.exists():
    SCHEMA_PATH = BASE_DIR / "project" / "schema_contract.yaml"
if not SCHEMA_PATH.exists():
    SCHEMA_PATH = Path("/opt/airflow/project/schema_contract.yaml")

DEFAULT_SAMPLE_CSV = BASE_DIR / "dags" / "extraction" / "sample.csv"
if not DEFAULT_SAMPLE_CSV.exists():
    DEFAULT_SAMPLE_CSV = PROJECT_DIR / "dags" / "extraction" / "sample.csv"

PRODUCT_CANONICAL = {
    "api development": "API Development",
    "api monitoring": "API Monitoring",
    "cloud management": "Cloud Management",
    "iam service": "IAM Service",
    "mercury language": "Mercury Language",
}

def clean_text(text: Any) -> Any:
    """Clean known mojibake characters in text fields without corrupting valid text."""
    if not isinstance(text, str):
        return text
    return text.replace("â€™", "’").replace("â€œ", "“").replace("â€ ", "”").replace("â€”", "—")

def parse_utc_timestamp(val: str) -> datetime:
    """Parse timestamps supporting ISO, Z, +00:00, milliseconds, or no timezone to UTC datetime."""
    if not isinstance(val, str) or not val.strip():
        raise ValueError("Timestamp is empty or not a string")
    val_clean = val.strip()
    try:
        if val_clean.endswith("Z"):
            ts = datetime.fromisoformat(val_clean[:-1] + "+00:00")
        else:
            ts = datetime.fromisoformat(val_clean)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            else:
                ts = ts.astimezone(timezone.utc)
        return ts
    except Exception:
        ts = dateutil.parser.parse(val_clean)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return ts

def resolve_csv_path(user_filename: str) -> Path:
    """Safely resolve CSV input path and ensure it does not escape permitted directories."""
    candidate = Path(user_filename)
    if not candidate.is_absolute():
        # Check relative to BASE_DIR, PROJECT_DIR, and tests/data
        search_paths = [
            BASE_DIR / candidate,
            PROJECT_DIR / candidate,
            PROJECT_DIR / "tests" / "data" / candidate.name,
            BASE_DIR / "tests" / "data" / candidate.name,
            Path("/opt/airflow") / candidate,
            Path("/opt/airflow/project") / candidate,
        ]
        resolved = None
        for p in search_paths:
            if p.exists():
                resolved = p.resolve()
                break
        if resolved is None:
            resolved = (BASE_DIR / candidate).resolve()
    else:
        resolved = candidate.resolve()

    # Verify path is within permitted directories
    allowed_roots = [
        BASE_DIR.resolve(),
        PROJECT_DIR.resolve(),
        Path("/opt/airflow").resolve(),
    ]
    is_allowed = any(str(resolved).startswith(str(root)) for root in allowed_roots)
    if not is_allowed:
        raise AirflowFailException(f"Forbidden path traversal: '{user_filename}' resolves outside permitted directories.")

    if not resolved.exists() or not resolved.is_file():
        raise AirflowFailException(f"CSV file not found: {resolved}")

    return resolved

def get_minio_s3_config():
    """Retrieve MinIO connection settings from Airflow connection or environment variables."""
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin42")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "")
    bucket = os.getenv("MINIO_BUCKET", "customer-care-emails")

    try:
        conn = BaseHook.get_connection("minio_s3")
        if conn.login:
            access_key = conn.login
        if conn.password:
            secret_key = conn.password
        extra = conn.extra_dejson or {}
        if "endpoint_url" in extra:
            endpoint_url = extra["endpoint_url"]
        elif conn.host and conn.port:
            endpoint_url = f"http://{conn.host}:{conn.port}"
    except Exception as e:
        log.warning("Could not load connection 'minio_s3', falling back to environment: %s", e)

    return {
        "endpoint_url": endpoint_url,
        "access_key": access_key,
        "secret_key": secret_key,
        "bucket": bucket,
    }


@dag(
    dag_id="csv_to_parquet_minio",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dhap-42", "customer-care", "parquet", "minio", "csv"],
)
def csv_to_parquet_minio_pipeline():

    @task(task_id="read_csv")
    def read_csv(dag_run=None) -> Dict[str, Any]:
        run_id = getattr(dag_run, "run_id", "default_run")
        safe_run_id = "".join(c if c.isalnum() else "_" for c in str(run_id))
        
        filename = str(DEFAULT_SAMPLE_CSV)
        if dag_run and getattr(dag_run, "conf", None):
            filename = dag_run.conf.get("csv_filename", filename)

        csv_path = resolve_csv_path(filename)
        log.info("Reading CSV from safe path: %s", csv_path)

        # Read as string to preserve source values and avoid pandas type inference bypass
        df_raw = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8")
        row_count = len(df_raw)
        log.info("Read %d rows and %d columns from %s", row_count, len(df_raw.columns), csv_path.name)

        # Persist intermediate raw file
        raw_output_path = TEMP_DIR / f"raw_{safe_run_id}.parquet"
        table_raw = pa.Table.from_pandas(df_raw, preserve_index=False)
        pq.write_table(table_raw, raw_output_path)

        return {
            "raw_path": str(raw_output_path),
            "row_count": row_count,
            "columns": list(df_raw.columns),
            "source_csv": str(csv_path),
        }

    @task(task_id="validate_contract", retries=0)
    def validate_contract(raw_meta: Dict[str, Any], dag_run=None) -> Dict[str, Any]:
        raw_path = Path(raw_meta["raw_path"])
        if not raw_path.exists():
            raise AirflowFailException(f"Intermediate raw file not found: {raw_path}")

        df_raw = pq.read_table(raw_path).to_pandas()
        row_count = len(df_raw)
        log.info("Validating schema contract on %d rows from %s", row_count, raw_path.name)

        # Load contract schema
        if not SCHEMA_PATH.exists():
            raise AirflowFailException(f"Schema contract file missing at {SCHEMA_PATH}")

        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            contract = yaml.safe_load(f)

        expected_columns = contract.get("expected_columns", [])
        actual_columns = list(df_raw.columns)

        # Rule 1 & 2: Check columns and order
        missing = [c for c in expected_columns if c not in actual_columns]
        extra = [c for c in actual_columns if c not in expected_columns]
        if missing or extra:
            raise AirflowFailException(f"Schema contract violation: missing={missing}, extra={extra}")

        if actual_columns != expected_columns:
            raise AirflowFailException(
                f"Schema contract violation: Column order mismatch. Expected {expected_columns}, got {actual_columns}"
            )

        # Row-level contract validation
        errors = []
        candidate_keys = set()
        valid_ratings = {"very low", "low", "medium", "high", "very high"}
        allowed_email_types = {"inquiry", "issue", "suggestion", "other matter"}

        for i, row in df_raw.iterrows():
            # Rule 3 & 4: Non-null and non-empty string check
            for col in expected_columns:
                val = row[col]
                if val is None or val == "" or str(val).strip() == "":
                    errors.append(f"Row {i}: column '{col}' is empty or whitespace-only")

            # Rule 7: email_status
            if row["email_status"] not in ("ongoing", "completed"):
                errors.append(f"Row {i}: invalid email_status '{row['email_status']}'")

            # Rule 8: email_criticality
            if row["email_criticality"] not in ("low", "medium", "high"):
                errors.append(f"Row {i}: invalid email_criticality '{row['email_criticality']}'")

            # Rule 9: agent effectivity and efficiency
            if row["agent_effectivity"] not in valid_ratings:
                errors.append(f"Row {i}: invalid agent_effectivity '{row['agent_effectivity']}'")
            if row["agent_efficiency"] not in valid_ratings:
                errors.append(f"Row {i}: invalid agent_efficiency '{row['agent_efficiency']}'")

            # Rule 10: customer_satisfaction
            try:
                cs = float(row["customer_satisfaction"])
                if not (-1.0 <= cs <= 1.0):
                    errors.append(f"Row {i}: customer_satisfaction out of range [-1.0, 1.0]: {cs}")
            except (ValueError, TypeError):
                errors.append(f"Row {i}: customer_satisfaction contains non-numeric value: '{row['customer_satisfaction']}'")

            # Rule 6: email_types
            try:
                etypes = ast.literal_eval(row["email_types"])
                if not isinstance(etypes, list) or not all(isinstance(t, str) for t in etypes):
                    errors.append(f"Row {i}: email_types is not a valid list of strings: '{row['email_types']}'")
                else:
                    for t in etypes:
                        if t not in allowed_email_types:
                            errors.append(f"Row {i}: invalid email_type item '{t}'")
            except Exception as e:
                errors.append(f"Row {i}: malformed email_types python list: {e}")

            # Rule 11: product_types
            try:
                prods = ast.literal_eval(row["product_types"])
                if not isinstance(prods, list) or not all(isinstance(p, str) for p in prods):
                    errors.append(f"Row {i}: product_types is not a valid list of strings: '{row['product_types']}'")
                else:
                    for p in prods:
                        if p.strip().lower() not in PRODUCT_CANONICAL:
                            errors.append(f"Row {i}: invalid product_type item '{p}'")
            except Exception as e:
                errors.append(f"Row {i}: malformed product_types python list: {e}")

            # Rule 5: timestamp parsing
            ts_iso = ""
            try:
                ts = parse_utc_timestamp(row["timestamp"])
                ts_iso = ts.isoformat()
            except Exception as e:
                errors.append(f"Row {i}: timestamp parsing failed: {e}")

            # Rule 12: Derived email_key uniqueness
            if ts_iso:
                body = clean_text(row["message_body"])
                body_md5 = hashlib.md5(body.encode("utf-8")).hexdigest()
                key_raw = f"{row['thread_id']}|{ts_iso}|{row['sender']}|{body_md5}"
                key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()
                if key in candidate_keys:
                    errors.append(f"Row {i}: email_key is not unique (duplicate key '{key}')")
                else:
                    candidate_keys.add(key)

        if errors:
            summary_msg = f"Schema contract violation: {len(errors)} error(s) detected. First errors: " + "; ".join(errors[:5])
            log.error(summary_msg)
            raise AirflowFailException(summary_msg)

        log.info("Validated %d rows successfully against schema contract.", row_count)
        return {
            "validated_path": str(raw_path),
            "row_count": row_count,
        }

    @task(task_id="transform")
    def transform(validated_meta: Dict[str, Any], dag_run=None) -> Dict[str, Any]:
        run_id = getattr(dag_run, "run_id", "default_run")
        safe_run_id = "".join(c if c.isalnum() else "_" for c in str(run_id))
        
        validated_path = Path(validated_meta["validated_path"])
        df_raw = pq.read_table(validated_path).to_pandas()
        row_count = len(df_raw)
        log.info("Transforming %d rows", row_count)

        transformed_rows = []
        unique_keys = set()

        for i, row in df_raw.iterrows():
            ts = parse_utc_timestamp(row["timestamp"])
            ts_iso = ts.isoformat()
            email_date = ts.strftime("%Y-%m-%d")

            body = clean_text(row["message_body"])
            body_md5 = hashlib.md5(body.encode("utf-8")).hexdigest()
            key_raw = f"{row['thread_id']}|{ts_iso}|{row['sender']}|{body_md5}"
            email_key = hashlib.sha256(key_raw.encode("utf-8")).hexdigest()

            if email_key in unique_keys:
                raise AirflowFailException(f"Transformation failure: duplicate email_key '{email_key}' at row {i}")
            unique_keys.add(email_key)

            email_types = ast.literal_eval(row["email_types"])
            raw_prods = ast.literal_eval(row["product_types"])
            canonical_prods = [PRODUCT_CANONICAL[p.strip().lower()] for p in raw_prods]

            transformed_rows.append({
                "email_key": email_key,
                "email_date": email_date,
                "subject": clean_text(row["subject"]),
                "sender": row["sender"],
                "receiver": row["receiver"],
                "timestamp": ts,
                "message_body": body,
                "thread_id": row["thread_id"],
                "email_types": email_types,
                "email_status": row["email_status"],
                "email_criticality": row["email_criticality"],
                "product_types": canonical_prods,
                "agent_effectivity": row["agent_effectivity"],
                "agent_efficiency": row["agent_efficiency"],
                "customer_satisfaction": float(row["customer_satisfaction"]),
            })

        arrow_schema = pa.schema([
            pa.field("email_key", pa.string(), nullable=False),
            pa.field("email_date", pa.string(), nullable=False),
            pa.field("subject", pa.string(), nullable=False),
            pa.field("sender", pa.string(), nullable=False),
            pa.field("receiver", pa.string(), nullable=False),
            pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("message_body", pa.string(), nullable=False),
            pa.field("thread_id", pa.string(), nullable=False),
            pa.field("email_types", pa.list_(pa.string()), nullable=False),
            pa.field("email_status", pa.string(), nullable=False),
            pa.field("email_criticality", pa.string(), nullable=False),
            pa.field("product_types", pa.list_(pa.string()), nullable=False),
            pa.field("agent_effectivity", pa.string(), nullable=False),
            pa.field("agent_efficiency", pa.string(), nullable=False),
            pa.field("customer_satisfaction", pa.float64(), nullable=False),
        ])

        table = pa.Table.from_pylist(transformed_rows, schema=arrow_schema)
        transformed_output_path = TEMP_DIR / f"transformed_{safe_run_id}.parquet"
        pq.write_table(table, transformed_output_path)
        log.info("Transformation complete. Stored %d rows at %s", table.num_rows, transformed_output_path)

        return {
            "transformed_path": str(transformed_output_path),
            "row_count": table.num_rows,
        }

    @task(task_id="write_parquet_to_minio")
    def write_parquet_to_minio(trans_meta: Dict[str, Any], dag_run=None) -> Dict[str, Any]:
        run_id = getattr(dag_run, "run_id", "default_run")
        safe_run_id = "".join(c if c.isalnum() else "_" for c in str(run_id))

        transformed_path = Path(trans_meta["transformed_path"])
        if not transformed_path.exists():
            raise AirflowFailException(f"Transformed parquet file missing: {transformed_path}")

        table = pq.read_table(transformed_path)
        row_count = table.num_rows
        log.info("Preparing to write %d rows to MinIO", row_count)

        minio_cfg = get_minio_s3_config()
        bucket = minio_cfg["bucket"]
        endpoint_url = minio_cfg["endpoint_url"]
        dataset_prefix = "customer_care_emails"

        # 1. Write partitioned dataset locally to staging directory
        staging_dir = TEMP_DIR / f"staging_{safe_run_id}"
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)

        pq.write_to_dataset(
            table,
            root_path=str(staging_dir),
            partition_cols=["email_date"],
            existing_data_behavior="overwrite_or_ignore",
        )

        # Collect written partition files
        parquet_files = list(staging_dir.glob("**/*.parquet"))
        partition_dirs = set(p.parent.name for p in parquet_files)
        log.info(
            "Staged %d parquet file(s) across %d partition(s) locally.",
            len(parquet_files),
            len(partition_dirs),
        )

        # 2. Connect to MinIO with boto3 S3 client
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=minio_cfg["access_key"],
            aws_secret_access_key=minio_cfg["secret_key"],
            region_name="us-east-1",
        )

        # Ensure bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket)
        except Exception:
            log.info("Bucket %s does not exist; creating bucket", bucket)
            s3_client.create_bucket(Bucket=bucket)

        # 3. Clear only designated dataset prefix for idempotency
        paginator = s3_client.get_paginator("list_objects_v2")
        prefix_key = f"{dataset_prefix}/"
        delete_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix_key):
            for obj in page.get("Contents", []):
                delete_keys.append({"Key": obj["Key"]})

        if delete_keys:
            log.info("Clearing %d existing object(s) under %s", len(delete_keys), prefix_key)
            for chunk in [delete_keys[i:i + 1000] for i in range(0, len(delete_keys), 1000)]:
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": chunk})

        # 4. Upload partitioned Parquet files
        uploaded_count = 0
        for local_file in parquet_files:
            rel_path = local_file.relative_to(staging_dir).as_posix()
            s3_key = f"{dataset_prefix}/{rel_path}"
            s3_client.upload_file(str(local_file), bucket, s3_key)
            uploaded_count += 1

        log.info(
            "Successfully published %d rows, %d partitions, %d Parquet files to s3://%s/%s",
            row_count,
            len(partition_dirs),
            uploaded_count,
            bucket,
            prefix_key,
        )

        # Clean staging and temp files
        shutil.rmtree(staging_dir, ignore_errors=True)
        try:
            transformed_path.unlink(missing_ok=True)
        except Exception:
            pass

        return {
            "status": "success",
            "rows_written": row_count,
            "partitions_written": len(partition_dirs),
            "files_written": uploaded_count,
            "minio_bucket": bucket,
            "minio_prefix": prefix_key,
        }

    raw = read_csv()
    validated = validate_contract(raw)
    trans = transform(validated)
    write_parquet_to_minio(trans)

pipeline = csv_to_parquet_minio_pipeline()
