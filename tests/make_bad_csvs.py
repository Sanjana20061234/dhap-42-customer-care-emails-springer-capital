import csv
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SAMPLE_CSV = BASE_DIR / "dags" / "extraction" / "sample.csv"
TEST_DATA_DIR = BASE_DIR / "tests" / "data"

def make_bad_csvs():
    TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    if not SAMPLE_CSV.exists():
        raise FileNotFoundError(f"Sample CSV not found at {SAMPLE_CSV}")
        
    with open(SAMPLE_CSV, "r", encoding="utf-8", newline="") as f:
        reader = list(csv.reader(f))
        
    header = reader[0]
    rows = reader[1:]
    
    print(f"Loaded sample CSV with {len(rows)} rows and {len(header)} columns.")
    
    # 1. Renamed header: subject -> subject_x
    bad_header = [c if c != "subject" else "subject_x" for c in header]
    with open(TEST_DATA_DIR / "bad_header_renamed.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(bad_header)
        writer.writerows(rows)
    print("Generated bad_header_renamed.csv")

    # 2. Missing column: remove 'thread_id'
    col_idx = header.index("thread_id")
    bad_header_missing = [c for i, c in enumerate(header) if i != col_idx]
    with open(TEST_DATA_DIR / "bad_missing_column.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(bad_header_missing)
        for r in rows:
            writer.writerow([val for i, val in enumerate(r) if i != col_idx])
    print("Generated bad_missing_column.csv")

    # 3. Non-numeric customer_satisfaction
    cs_idx = header.index("customer_satisfaction")
    rows_cs = [list(r) for r in rows]
    rows_cs[5][cs_idx] = "not_a_number"
    with open(TEST_DATA_DIR / "bad_customer_satisfaction.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_cs)
    print("Generated bad_customer_satisfaction.csv")

    # 4. Empty value in non-nullable column: sender=""
    sender_idx = header.index("sender")
    rows_empty = [list(r) for r in rows]
    rows_empty[10][sender_idx] = "   "
    with open(TEST_DATA_DIR / "bad_empty_non_nullable.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_empty)
    print("Generated bad_empty_non_nullable.csv")

    # 5. Invalid email_status
    status_idx = header.index("email_status")
    rows_status = [list(r) for r in rows]
    rows_status[15][status_idx] = "pending"
    with open(TEST_DATA_DIR / "bad_invalid_status.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_status)
    print("Generated bad_invalid_status.csv")

    # 6. Invalid timestamp
    ts_idx = header.index("timestamp")
    rows_ts = [list(r) for r in rows]
    rows_ts[20][ts_idx] = "invalid_timestamp_2023"
    with open(TEST_DATA_DIR / "bad_invalid_timestamp.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_ts)
    print("Generated bad_invalid_timestamp.csv")

    # 7. Malformed Python-style list in email_types
    types_idx = header.index("email_types")
    rows_malformed = [list(r) for r in rows]
    rows_malformed[25][types_idx] = "['inquiry'"
    with open(TEST_DATA_DIR / "bad_malformed_list.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_malformed)
    print("Generated bad_malformed_list.csv")

    # 8. Duplicate email_key case: duplicate row 0 at the end
    rows_dup = [list(r) for r in rows]
    rows_dup.append(list(rows[0]))
    with open(TEST_DATA_DIR / "bad_duplicate_key.csv", "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows_dup)
    print("Generated bad_duplicate_key.csv")

if __name__ == "__main__":
    make_bad_csvs()
