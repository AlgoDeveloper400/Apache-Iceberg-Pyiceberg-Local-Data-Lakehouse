"""
=============================================================
  PRODUCTION MINI LAKEHOUSE — Scheduler & Folder Watcher
=============================================================
  Just run:
    python lakehouse_scheduler.py          → watch + schedule (production)
    python lakehouse_scheduler.py --now    → run once immediately
    python lakehouse_scheduler.py --watch  → folder watch only
    python lakehouse_scheduler.py --schedule → scheduled runs only
=============================================================
"""

import argparse
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from lakehouse_pipeline import run_pipeline, TRAINING_BATCH_DIR

# -----------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------
WATCH_INTERVAL_SECS   = 30    # poll folder every 30 seconds
SCHEDULE_INTERVAL_HRS = 24    # full scheduled run every 24 hours
SCHEDULE_AT_HOUR      = 2     # at 2am UTC

log = logging.getLogger("scheduler")

# -----------------------------------------------------------------------
# FOLDER WATCHER
# -----------------------------------------------------------------------
class FolderWatcher:
    def __init__(self, watch_dir: str):
        self.watch_dir  = watch_dir
        self.seen_files = self._snapshot()
        log.info(f"[watcher] Watching: {watch_dir}")
        log.info(f"[watcher] Baseline: {len(self.seen_files)} parquet file(s) on disk")

    def _snapshot(self) -> dict:
        return {
            str(p): p.stat().st_mtime
            for p in Path(self.watch_dir).rglob("*.parquet")
        }

    def has_changes(self) -> bool:
        current = self._snapshot()
        changed = False
        for path, mtime in current.items():
            if path not in self.seen_files:
                log.info(f"[watcher] New file: {Path(path).name}")
                changed = True
            elif self.seen_files[path] != mtime:
                log.info(f"[watcher] Modified file: {Path(path).name}")
                changed = True
        self.seen_files = current
        return changed


# -----------------------------------------------------------------------
# SCHEDULER
# -----------------------------------------------------------------------
class Scheduler:
    def __init__(self, interval_hrs: int, at_hour: int):
        self.interval_hrs = interval_hrs
        self.at_hour      = at_hour
        self.last_run     = None

    def should_run(self) -> bool:
        if self.last_run is None:
            return False
        now = datetime.now(tz=timezone.utc)
        hours_since = (now - self.last_run).total_seconds() / 3600
        return hours_since >= self.interval_hrs and now.hour == self.at_hour

    def mark_ran(self):
        self.last_run = datetime.now(tz=timezone.utc)


# -----------------------------------------------------------------------
# MODES
# -----------------------------------------------------------------------
def mode_now():
    log.info("=" * 50)
    log.info("Running pipeline once...")
    log.info("=" * 50)
    summary = run_pipeline()
    total   = sum(t.get("rows_appended", 0) for t in summary.get("tables", {}).values())
    log.info(f"Done. Rows appended this run: {total:,}")


def mode_watch():
    log.info("=" * 50)
    log.info(f"Folder watch mode — polling every {WATCH_INTERVAL_SECS}s")
    log.info("Drop parquet files into Training Batch to trigger ingestion.")
    log.info("Press Ctrl+C to stop.")
    log.info("=" * 50)

    watcher = FolderWatcher(TRAINING_BATCH_DIR)
    while True:
        try:
            time.sleep(WATCH_INTERVAL_SECS)
            if watcher.has_changes():
                log.info("[watcher] Changes detected — running pipeline...")
                run_pipeline()
                log.info("[watcher] Done. Resuming watch.")
        except KeyboardInterrupt:
            log.info("Watcher stopped.")
            break
        except Exception as e:
            log.error(f"[watcher] Error: {e}")
            time.sleep(60)


def mode_schedule():
    log.info("=" * 50)
    log.info(f"Schedule mode — runs every {SCHEDULE_INTERVAL_HRS}h at {SCHEDULE_AT_HOUR:02d}:00 UTC")
    log.info("Press Ctrl+C to stop.")
    log.info("=" * 50)

    scheduler = Scheduler(SCHEDULE_INTERVAL_HRS, SCHEDULE_AT_HOUR)
    while True:
        try:
            time.sleep(60)
            if scheduler.should_run():
                log.info("[scheduler] Scheduled trigger — running pipeline...")
                run_pipeline()
                scheduler.mark_ran()
        except KeyboardInterrupt:
            log.info("Scheduler stopped.")
            break
        except Exception as e:
            log.error(f"[scheduler] Error: {e}")
            time.sleep(300)


def mode_all():
    """Production mode — folder watch + daily schedule running together."""
    log.info("=" * 50)
    log.info("Production mode: folder watch + daily schedule")
    log.info(f"  Watch interval : every {WATCH_INTERVAL_SECS}s")
    log.info(f"  Schedule       : every {SCHEDULE_INTERVAL_HRS}h at {SCHEDULE_AT_HOUR:02d}:00 UTC")
    log.info("Press Ctrl+C to stop.")
    log.info("=" * 50)

    watcher   = FolderWatcher(TRAINING_BATCH_DIR)
    scheduler = Scheduler(SCHEDULE_INTERVAL_HRS, SCHEDULE_AT_HOUR)
    lock      = threading.Lock()  # prevents two pipeline runs at the same time

    def watch_loop():
        while True:
            try:
                time.sleep(WATCH_INTERVAL_SECS)
                if watcher.has_changes():
                    with lock:
                        log.info("[watch] New files — running pipeline...")
                        run_pipeline()
                        scheduler.mark_ran()
                        log.info("[watch] Done.")
            except Exception as e:
                log.error(f"[watch] Error: {e}")
                time.sleep(60)

    def schedule_loop():
        while True:
            try:
                time.sleep(60)
                if scheduler.should_run():
                    with lock:
                        log.info("[schedule] Scheduled run — running pipeline...")
                        run_pipeline()
                        scheduler.mark_ran()
                        log.info("[schedule] Done.")
            except Exception as e:
                log.error(f"[schedule] Error: {e}")
                time.sleep(300)

    t1 = threading.Thread(target=watch_loop,    daemon=True, name="watcher")
    t2 = threading.Thread(target=schedule_loop, daemon=True, name="scheduler")
    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Production mode stopped.")


# -----------------------------------------------------------------------
# ENTRYPOINT — defaults to production mode (watch + schedule) if no flag given
# -----------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Lakehouse Scheduler — defaults to production mode (watch + schedule)",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--now",      action="store_true", help="Run pipeline once and exit")
    parser.add_argument("--watch",    action="store_true", help="Watch folder for new files only")
    parser.add_argument("--schedule", action="store_true", help="Scheduled daily runs only")
    args = parser.parse_args()

    if args.now:
        mode_now()
    elif args.watch:
        mode_watch()
    elif args.schedule:
        mode_schedule()
    else:
        mode_all()