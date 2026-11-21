"""Measure the migration and history SQL with a full 32-process day and week."""

import json
from pathlib import Path
import re
import sqlite3
import tempfile
import time

STORE = Path(__file__).resolve().parents[1] / "crates/roost-store"
SOURCE = (STORE / "src/metrics.rs").read_text()
START = 1_800_000_000
DAY = 86_400
WEEK = 7 * DAY


def sql(prefix):
    return next(query for query in re.findall(r'c"([^"]+)"', SOURCE) if query.startswith(prefix))


def timed(operation):
    start = time.perf_counter()
    result = operation()
    return result, round((time.perf_counter() - start) * 1000, 3)


def size(db):
    return db.execute("PRAGMA page_count").fetchone()[0] * db.execute("PRAGMA page_size").fetchone()[0]


def seed(db):
    for migration in sorted((STORE / "migrations").glob("*.sql")):
        db.executescript(migration.read_text())
    db.executescript("""
        INSERT INTO projects (id,path,revision,data) VALUES ('benchmark','/fixture',1,'{}');
        WITH RECURSIVE slots(value) AS (VALUES(0) UNION ALL SELECT value+1 FROM slots WHERE value<31)
        INSERT INTO process_metric_series (project_id,kind,profile_id,slot,instance_id,generation)
        SELECT 'benchmark',CASE WHEN value=31 THEN 'scheduler' ELSE 'worker' END,
          CASE WHEN value=31 THEN 'scheduler-profile' ELSE 'worker-profile' END,
          value,'instance-'||value,1 FROM slots;
    """)
    empty = size(db)
    with db:
        db.execute("""
            WITH RECURSIVE ticks(s) AS (VALUES(0) UNION ALL SELECT s+2 FROM ticks WHERE s<86398)
            INSERT INTO process_metric_raw
              (series_id,observed_at,cpu_percent,rss_bytes,uptime_seconds,peak_rss_bytes)
            SELECT series.id,?+ticks.s,7.5,26843545,ticks.s,26843545
            FROM ticks CROSS JOIN process_metric_series series
        """, (START,))
    raw_bytes = size(db) - empty
    with db:
        db.execute("""
            WITH RECURSIVE buckets(s) AS (VALUES(0) UNION ALL SELECT s+300 FROM buckets WHERE s<604500)
            INSERT INTO process_metric_rollups
              (series_id,bucket,cpu_count,cpu_mean,cpu_min,cpu_max,rss_count,rss_mean,rss_min,rss_max,peak_rss_bytes,max_uptime_seconds)
            SELECT series.id,?+buckets.s,150,7.5,0,10,150,26843545,26843545,26843545,26843545,buckets.s+300
            FROM buckets CROSS JOIN process_metric_series series
        """, (START - WEEK,))
    return raw_bytes, size(db) - empty - raw_bytes


def maintenance(db):
    with db:
        end = START + 300
        db.execute(sql("INSERT OR IGNORE INTO process_metric_rollups"), (str(end),))
        db.execute(sql("UPDATE process_metric_state SET rolled_until="), (str(end),))
        for table, watermark, before in (("raw", "raw_pruned", end), ("rollups", "rollup_pruned", START)):
            rows = db.execute(sql(f"SELECT sequence FROM process_metric_{table} WHERE"), (str(before),)).fetchall()
            for (sequence,) in rows:
                db.execute(sql(f"DELETE FROM process_metric_{table} WHERE"), (str(sequence),))
            db.execute(sql(f"UPDATE process_metric_state SET {watermark}="), (str(rows[-1][0]),))
        db.execute(sql("DELETE FROM process_metric_series WHERE"))
    return size(db)


def history(db):
    bounds = sql("SELECT CASE WHEN first IS NOT NULL")
    start, end = START + DAY - 1400, START + DAY
    first, last = json.loads(db.execute(bounds, (str(start), str(end))).fetchone()[0])
    query = sql("SELECT json_object(\n  'sequence'")
    return db.execute(query, ("benchmark", None, str(first - 1), str(last), "257")).fetchall()


def main():
    with tempfile.TemporaryDirectory(prefix="roost-metrics-storage-") as path:
        db = sqlite3.connect(Path(path) / "roost.sqlite3")
        db.execute("PRAGMA foreign_keys=ON")
        (raw_bytes, rollup_bytes), seed_ms = timed(lambda: seed(db))
        times = []
        for _ in range(10):
            records, elapsed = timed(lambda: history(db))
            assert len(records) == 257
            times.append(elapsed)
        journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0]
        (size_after, cleanup_ms) = timed(lambda: maintenance(db))
        raw_rows = db.execute("SELECT count(*) FROM process_metric_raw").fetchone()[0]
        rollup_rows = db.execute("SELECT count(*) FROM process_metric_rollups").fetchone()[0]
        assert raw_rows == 1_382_400 - 512 and rollup_rows == 64_512 + 32 - 256
        print(json.dumps({
            "sqlite": sqlite3.sqlite_version, "raw_bytes": raw_bytes,
            "week_rollup_bytes": rollup_bytes, "seed_ms": seed_ms,
            "recent_257_row_history_ms": times, "cleanup_commit_ms": cleanup_ms,
            "raw_rows_after": raw_rows, "rollup_rows_after": rollup_rows,
            "database_bytes_after_cleanup": size_after, "journal_mode": journal_mode,
        }))
        db.close()


if __name__ == "__main__":
    main()
