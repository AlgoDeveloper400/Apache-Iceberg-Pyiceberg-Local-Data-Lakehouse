# 🏔️ Local Apache Iceberg Mini Lakehouse

A production-grade, local Apache Iceberg data lakehouse pipeline built with **PyIceberg** and **PyArrow**. Designed for ingesting time-series financial tick data (Bid/Ask) stored as Parquet files, with a full suite of data quality, deduplication, snapshot management, and audit logging — all running on your local filesystem with a SQLite catalog.

---

## 📁 Project Structure

```
your-lakehouse/
├── lakehouse_pipeline.py      # Core ingestion pipeline
├── lakehouse_scheduler.py     # Folder watcher + daily scheduler
├── requirements.txt           # Python dependencies
│
├── Training Batch/            # Drop your symbol folders here
│   ├── EURUSD/
│   │   └── *.parquet
│   └── GBPUSD/
│       └── *.parquet
│
├── gold/                      # Iceberg table data (auto-created)
├── iceberg_catalog.db         # SQLite catalog (auto-created)
├── audit_log.json             # Pipeline run history (auto-created)
├── ingested_files.json        # File tracker (auto-created)
└── pipeline.log               # Log file (auto-created)
```

---

## ⚙️ Setup

### 1. Clone the repo

```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure your paths

Open `lakehouse_pipeline.py` and update the two path variables near the top of the file:

```python
BASE_DIR           = r"/your/path/to/lakehouse"
TRAINING_BATCH_DIR = os.path.join(BASE_DIR, "Training Batch")
```

**Examples:**
- Windows: `r"C:\Users\YourName\Documents\Data Lakehouse"`
- macOS/Linux: `"/home/yourname/data-lakehouse"`

### 4. Prepare your data

Inside `TRAINING_BATCH_DIR`, create one subfolder per trading symbol containing `.parquet` files. Each Parquet file must have at least these columns:

| Column     | Type       | Description              |
|------------|------------|--------------------------|
| `DateTime` | timestamp  | Tick timestamp (UTC)     |
| `Bid`      | float      | Bid price (must be > 0)  |
| `Ask`      | float      | Ask price (must be > 0)  |

```
Training Batch/
├── EURUSD/
│   ├── EURUSD_2023.parquet
│   └── EURUSD_2024.parquet
└── GBPUSD/
    └── GBPUSD_2024.parquet
```

---

## 🚀 Running the Pipeline

### Run once (manual trigger)

```bash
python lakehouse_scheduler.py --now
```

### Watch folder for new files (auto-trigger on drop)

```bash
python lakehouse_scheduler.py --watch
```

### Scheduled daily runs only (runs at 02:00 UTC)

```bash
python lakehouse_scheduler.py --schedule
```

### Production mode — folder watch + daily schedule (default)

```bash
python lakehouse_scheduler.py
```

---

## 🛡️ Features

### ✅ Deduplication
Rows with a `DateTime` value already present in the Iceberg table are automatically dropped before append. Safe to re-run with the same files.

### ✅ File-level idempotency
An MD5 checksum is stored per file. If a file hasn't changed since last ingest, it is skipped entirely.

### ✅ Data Quality Checks
Before any file is ingested, it must pass:
- Minimum row count (`MIN_ROWS_THRESHOLD = 100`)
- Required columns present (`DateTime`, `Bid`, `Ask`)
- Null percentage per column ≤ 5% (`MAX_NULL_PCT = 0.05`)
- `Bid` and `Ask` values must be strictly positive
- `DateTime` column must not be entirely null

Files that fail quality checks are logged and skipped.

### ✅ Snapshot Management
Iceberg snapshots older than 7 days are expired automatically. At least 2 snapshots are always retained regardless of age to prevent data loss.

### ✅ Audit Log
Every pipeline run is recorded to `audit_log.json` with:
- Run ID and timestamps
- Per-symbol rows appended / rejected
- Files processed and skipped
- Quality issues encountered

### ✅ Partitioning
Tables are partitioned by **year** on the `DateTime` column for efficient time-range queries.

---

## 🔧 Configuration Reference

All tunable parameters are at the top of `lakehouse_pipeline.py`:

| Parameter                  | Default | Description                              |
|---------------------------|---------|------------------------------------------|
| `NAMESPACE`               | `gold`  | Iceberg namespace for all tables         |
| `SNAPSHOT_RETENTION_DAYS` | `7`     | Days before snapshots are eligible for expiry |
| `MIN_ROWS_THRESHOLD`      | `100`   | Minimum rows required to pass QC         |
| `MAX_NULL_PCT`            | `0.05`  | Maximum allowed null ratio per column    |

Scheduler parameters are in `lakehouse_scheduler.py`:

| Parameter                  | Default | Description                              |
|---------------------------|---------|------------------------------------------|
| `WATCH_INTERVAL_SECS`     | `30`    | Folder polling interval in seconds       |
| `SCHEDULE_INTERVAL_HRS`   | `24`    | Hours between scheduled pipeline runs   |
| `SCHEDULE_AT_HOUR`        | `2`     | UTC hour to trigger scheduled run (0–23)|

---

## 🗂️ How It Works

```
New .parquet file dropped
        │
        ▼
  MD5 checksum check ──── Already ingested? ──► Skip
        │
        ▼
  Load & normalize Arrow table
        │
        ▼
  Data quality checks ──── Fail? ──► Log & skip
        │
        ▼
  Create Iceberg table (if not exists)
        │
        ▼
  Deduplicate against existing DateTime values
        │
        ▼
  Append new rows → Iceberg snapshot created
        │
        ▼
  Expire old snapshots (keep ≥ 2, ≤ 7 days)
        │
        ▼
  Write audit log entry
```

---

## 📦 Dependencies

| Package              | Purpose                          |
|---------------------|----------------------------------|
| `pyiceberg`         | Iceberg catalog + table ops      |
| `pyarrow`           | Arrow in-memory table processing |
| `sqlite3` (stdlib)  | Local SQLite catalog backend     |

---

## 📝 Notes

- **Windows users:** The monkey-patch at the top of `lakehouse_pipeline.py` fixes a `PyArrowFileIO` path-parsing bug for Windows drive letters (e.g. `C:\...`). This is required and intentional — do not remove it.
- **Iceberg catalog:** Uses SQLite as the catalog backend (`iceberg_catalog.db`). No Hive or AWS Glue required.
- **Thread safety:** In production mode, a `threading.Lock` prevents the watcher and scheduler from running the pipeline concurrently.

---

## Final Thoughts
This local setup allows you to experiment and play around with Apache Iceberg tables without messy catalogs, S3 buckets or massive Trino/Spark clusters. Ready for anyone willing to learn about how Iceberg works.