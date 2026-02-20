"""
=============================================================
  PRODUCTION MINI LAKEHOUSE — Core Ingestion Pipeline
=============================================================
  Features:
    - Deduplication on ingest (no duplicate DateTime rows)
    - Data quality checks before append
    - Snapshot management + auto-expire (keep 7 days)
    - Audit log (JSON) — every run recorded
    - Idempotent — safe to run multiple times
=============================================================

  SETUP:
    Set BASE_DIR and TRAINING_BATCH_DIR below to match your
    local directory structure before running.
=============================================================
"""

import os
import re
import json
import hashlib
import logging
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from datetime import datetime, timezone, timedelta
from pathlib import Path

# -----------------------------------------------------------------------
# MONKEY-PATCH — must happen before any other pyiceberg imports
# -----------------------------------------------------------------------
from pyiceberg.io.pyarrow import PyArrowFileIO

_orig = PyArrowFileIO.__dict__["parse_location"]

@staticmethod
def _patched_parse_location(location: str, properties=None) -> tuple[str, str, str]:
    m = re.match(r'^file:///([A-Za-z]:.*)', location)
    if m:
        return "file", "", m.group(1).replace("%20", " ").replace("\\", "/")
    m = re.match(r'^/([A-Za-z]:.*)', location)
    if m:
        return "file", "", m.group(1).replace("%20", " ").replace("\\", "/")
    m = re.match(r'^([A-Za-z]:[/\\].*)', location)
    if m:
        return "file", "", m.group(1).replace("\\", "/")
    if properties is not None:
        return _orig.__func__(location, properties)
    return _orig.__func__(location)

PyArrowFileIO.parse_location = _patched_parse_location
# -----------------------------------------------------------------------

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, TimestampType, DoubleType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError

# -----------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------
# ⚠️  CHANGE THESE TWO PATHS TO MATCH YOUR LOCAL SETUP
BASE_DIR           = r"/your/path/to/lakehouse"           # e.g. r"C:\Users\YourName\Data Lakehouse"
TRAINING_BATCH_DIR = os.path.join(BASE_DIR, "Training Batch")   # subfolder containing symbol folders

NAMESPACE          = "gold"
AUDIT_LOG_PATH     = os.path.join(BASE_DIR, "audit_log.json")
INGESTED_LOG_PATH  = os.path.join(BASE_DIR, "ingested_files.json")
SNAPSHOT_RETENTION_DAYS = 7
MIN_ROWS_THRESHOLD  = 100       # reject files with fewer rows than this
MAX_NULL_PCT        = 0.05      # reject if >5% nulls in any column

_fwd          = BASE_DIR.replace("\\", "/")
WAREHOUSE_URI = "file:///" + _fwd
SQLITE_URI    = "sqlite:///" + _fwd + "/iceberg_catalog.db"

# -----------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
    ]
)
log = logging.getLogger("lakehouse")

# -----------------------------------------------------------------------
# AUDIT LOG
# -----------------------------------------------------------------------
def load_audit_log() -> list:
    if os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH, "r") as f:
            return json.load(f)
    return []

def append_audit_entry(entry: dict):
    log_data = load_audit_log()
    log_data.append(entry)
    with open(AUDIT_LOG_PATH, "w") as f:
        json.dump(log_data, f, indent=2, default=str)

# -----------------------------------------------------------------------
# INGESTED FILES TRACKER — prevents reprocessing same file twice
# -----------------------------------------------------------------------
def load_ingested_files() -> dict:
    if os.path.exists(INGESTED_LOG_PATH):
        with open(INGESTED_LOG_PATH, "r") as f:
            return json.load(f)
    return {}

def save_ingested_files(data: dict):
    with open(INGESTED_LOG_PATH, "w") as f:
        json.dump(data, f, indent=2, default=str)

def file_checksum(path: str) -> str:
    """MD5 checksum of file — used to detect if file changed since last ingest."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

# -----------------------------------------------------------------------
# DATA QUALITY CHECKS
# -----------------------------------------------------------------------
def run_quality_checks(tbl: pa.Table, source_path: str) -> tuple[bool, list[str]]:
    issues = []

    # 1. Minimum row count
    if tbl.num_rows < MIN_ROWS_THRESHOLD:
        issues.append(f"Too few rows: {tbl.num_rows} (minimum {MIN_ROWS_THRESHOLD})")

    # 2. Required columns present
    required_cols = {"DateTime", "Bid", "Ask"}
    missing = required_cols - set(tbl.schema.names)
    if missing:
        issues.append(f"Missing columns: {missing}")

    # 3. Null percentage check
    for col_name in tbl.schema.names:
        col = tbl.column(col_name)
        null_count = col.null_count
        null_pct = null_count / tbl.num_rows if tbl.num_rows > 0 else 0
        if null_pct > MAX_NULL_PCT:
            issues.append(f"Column '{col_name}' has {null_pct:.1%} nulls (max {MAX_NULL_PCT:.0%})")

    # 4. DateTime must be parseable (basic sanity)
    if "DateTime" in tbl.schema.names:
        dt_col = tbl.column("DateTime").cast(pa.timestamp("us"))
        if dt_col.null_count == tbl.num_rows:
            issues.append("DateTime column is entirely null")

    # 5. Bid/Ask must be positive
    for price_col in ["Bid", "Ask"]:
        if price_col in tbl.schema.names:
            col = tbl.column(price_col).cast(pa.float64())
            non_null = col.drop_null()
            if len(non_null) > 0:
                min_val = pc.min(non_null).as_py()
                if min_val is not None and min_val <= 0:
                    issues.append(f"Column '{price_col}' has non-positive values (min={min_val})")

    passed = len(issues) == 0
    return passed, issues

# -----------------------------------------------------------------------
# NORMALIZE ARROW TABLE
# -----------------------------------------------------------------------
def normalize_arrow_table(tbl: pa.Table) -> pa.Table:
    new_cols = {}
    for name in tbl.schema.names:
        col = tbl.column(name)
        t   = col.type
        if pa.types.is_timestamp(t):
            col = col.cast(pa.timestamp("us"))
        elif t == pa.float32():
            col = col.cast(pa.float64())
        new_cols[name] = col
    return pa.table(new_cols)

# -----------------------------------------------------------------------
# ICEBERG SCHEMA FROM ARROW
# -----------------------------------------------------------------------
def iceberg_schema_from_arrow(tbl: pa.Table) -> Schema:
    fields = []
    for i, name in enumerate(tbl.schema.names, start=1):
        arrow_field  = tbl.schema.field(name)
        t            = arrow_field.type
        nullable     = arrow_field.nullable
        iceberg_type = TimestampType() if pa.types.is_timestamp(t) else DoubleType()
        fields.append(NestedField(i, name, iceberg_type, required=not nullable))
    return Schema(*fields)

# -----------------------------------------------------------------------
# DEDUPLICATION — remove rows whose DateTime already exists in Iceberg table
# -----------------------------------------------------------------------
def deduplicate(new_data: pa.Table, iceberg_table) -> pa.Table:
    try:
        existing = iceberg_table.scan(
            selected_fields=("DateTime",)
        ).to_arrow()

        if existing.num_rows == 0:
            return new_data

        existing_ts = pc.unique(existing.column("DateTime"))
        new_ts      = new_data.column("DateTime")

        mask = pc.invert(pc.is_in(new_ts, value_set=existing_ts))
        deduped = new_data.filter(mask)

        removed = new_data.num_rows - deduped.num_rows
        if removed > 0:
            log.info(f"    Deduplication: removed {removed:,} duplicate rows, "
                     f"keeping {deduped.num_rows:,}")
        return deduped

    except Exception as e:
        log.warning(f"    Deduplication skipped (could not scan existing data): {e}")
        return new_data

# -----------------------------------------------------------------------
# SNAPSHOT MANAGEMENT — expire snapshots older than retention window
# -----------------------------------------------------------------------
def manage_snapshots(iceberg_table, table_id: str):
    try:
        snapshots = iceberg_table.metadata.snapshots
        log.info(f"    Snapshots: {len(snapshots)} total")

        if len(snapshots) <= 1:
            log.info(f"    Skipping expiry — only {len(snapshots)} snapshot(s) exist. "
                     f"Need at least 2 before expiry is safe.")
            return

        MIN_SNAPSHOTS_TO_KEEP = 2
        sorted_snaps  = sorted(snapshots, key=lambda s: s.timestamp_ms, reverse=True)
        protected_ids = {s.snapshot_id for s in sorted_snaps[:MIN_SNAPSHOTS_TO_KEEP]}

        cutoff_ms = int(
            (datetime.now(tz=timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS))
            .timestamp() * 1000
        )
        eligible = [
            s for s in snapshots
            if s.timestamp_ms < cutoff_ms
            and s.snapshot_id not in protected_ids
        ]

        if not eligible:
            log.info(f"    No snapshots eligible for expiry — "
                     f"keeping minimum {MIN_SNAPSHOTS_TO_KEEP}, "
                     f"all within {SNAPSHOT_RETENTION_DAYS}-day window.")
            return

        iceberg_table.expire_snapshots() \
                     .expire_older_than(cutoff_ms) \
                     .commit()
        log.info(f"    Expired {len(eligible)} snapshot(s) older than "
                 f"{SNAPSHOT_RETENTION_DAYS} days. "
                 f"{len(snapshots) - len(eligible)} retained.")

    except Exception as e:
        log.warning(f"    Snapshot management failed: {e}")

# -----------------------------------------------------------------------
# ENSURE TABLE EXISTS
# -----------------------------------------------------------------------
def ensure_table(catalog, table_id: str, schema: Schema, partition_spec: PartitionSpec):
    try:
        catalog.create_table(
            identifier=table_id,
            schema=schema,
            partition_spec=partition_spec,
        )
        log.info(f"    Table '{table_id}' created.")
    except Exception:
        log.info(f"    Table '{table_id}' already exists.")

# -----------------------------------------------------------------------
# MAIN PIPELINE
# -----------------------------------------------------------------------
def run_pipeline():
    run_start  = datetime.now(tz=timezone.utc)
    run_id     = run_start.strftime("%Y%m%d_%H%M%S")
    run_summary = {
        "run_id":     run_id,
        "started_at": run_start.isoformat(),
        "tables":     {},
        "status":     "success",
    }

    log.info("=" * 60)
    log.info(f"  PIPELINE RUN START  —  run_id={run_id}")
    log.info("=" * 60)

    catalog = load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": SQLITE_URI,
            "warehouse": WAREHOUSE_URI,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        }
    )
    log.info("Catalog loaded.")

    try:
        catalog.create_namespace(NAMESPACE)
        log.info(f"Namespace '{NAMESPACE}' created.")
    except NamespaceAlreadyExistsError:
        log.info(f"Namespace '{NAMESPACE}' exists.")

    ingested_files = load_ingested_files()

    symbol_folders = [
        f for f in os.listdir(TRAINING_BATCH_DIR)
        if os.path.isdir(os.path.join(TRAINING_BATCH_DIR, f))
    ]
    log.info(f"Symbols found: {symbol_folders}")

    for symbol in symbol_folders:
        symbol_path      = os.path.join(TRAINING_BATCH_DIR, symbol)
        symbol_lower     = symbol.lower()
        table_id         = f"{NAMESPACE}.{symbol_lower}"
        table_summary    = {
            "files_processed": [],
            "files_skipped":   [],
            "rows_appended":   0,
            "rows_rejected":   0,
            "quality_issues":  [],
            "status":          "success",
        }

        log.info(f"\n--- {symbol} ---")

        parquet_files = list(Path(symbol_path).rglob("*.parquet"))
        if not parquet_files:
            log.warning(f"  No parquet files found in {symbol_path}")
            continue

        total_appended = 0

        for pfile in parquet_files:
            pfile_str = str(pfile)
            checksum  = file_checksum(pfile_str)

            if ingested_files.get(pfile_str) == checksum:
                log.info(f"  SKIP (already ingested): {pfile.name}")
                table_summary["files_skipped"].append(pfile.name)
                continue

            log.info(f"  Processing: {pfile.name}")

            raw         = ds.dataset(pfile_str, format="parquet").to_table()
            table_arrow = normalize_arrow_table(raw)

            passed, issues = run_quality_checks(table_arrow, pfile_str)
            if not passed:
                log.warning(f"  QUALITY FAIL — {pfile.name}: {issues}")
                table_summary["quality_issues"].extend(issues)
                table_summary["rows_rejected"] += table_arrow.num_rows
                table_summary["files_skipped"].append(pfile.name)
                continue

            schema = iceberg_schema_from_arrow(table_arrow)
            partition_spec = PartitionSpec(
                fields=[
                    PartitionField(
                        source_id=schema.find_field("DateTime").field_id,
                        field_id=1000,
                        name="DateTime_year",
                        transform=YearTransform(),
                    )
                ]
            )

            ensure_table(catalog, table_id, schema, partition_spec)
            iceberg_table = catalog.load_table(table_id)
            clean_data = deduplicate(table_arrow, iceberg_table)

            if clean_data.num_rows == 0:
                log.info(f"  No new rows after deduplication — skipping append.")
                table_summary["files_skipped"].append(pfile.name)
                ingested_files[pfile_str] = checksum
                continue

            iceberg_table.append(clean_data)
            total_appended += clean_data.num_rows
            log.info(f"  ✓ Appended {clean_data.num_rows:,} rows from {pfile.name}")

            ingested_files[pfile_str] = checksum
            table_summary["files_processed"].append(pfile.name)

        try:
            iceberg_table = catalog.load_table(table_id)
            manage_snapshots(iceberg_table, table_id)
        except NoSuchTableError:
            pass

        table_summary["rows_appended"] = total_appended
        run_summary["tables"][symbol_lower] = table_summary
        log.info(f"  Total appended for {symbol}: {total_appended:,} rows")

    save_ingested_files(ingested_files)

    run_summary["finished_at"]  = datetime.now(tz=timezone.utc).isoformat()
    run_summary["duration_secs"] = (
        datetime.now(tz=timezone.utc) - run_start
    ).total_seconds()
    append_audit_entry(run_summary)

    log.info("\n" + "=" * 60)
    log.info(f"  PIPELINE RUN COMPLETE  —  run_id={run_id}")
    log.info(f"  Duration: {run_summary['duration_secs']:.1f}s")
    log.info("=" * 60)

    return run_summary


if __name__ == "__main__":
    run_pipeline()