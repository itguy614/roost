"""Measure cumulative daemon and real PHP queue-probe costs at 32 processes."""

import argparse
import json
import os
from pathlib import Path
import re
import pwd
import resource
import runpy
import sqlite3
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = runpy.run_path(str(ROOT / "scripts/test-queue-metrics.py"))
HELPERS = FIXTURE["HELPERS"]


def database_stats(state):
    with sqlite3.connect(state / "roost.sqlite3") as db:
        raw, gap = db.execute("""
            SELECT sum(rows),max(gap) FROM (
                SELECT count(*) AS rows, observed_at - lag(observed_at) OVER (ORDER BY observed_at) AS gap
                FROM process_metric_raw GROUP BY observed_at
            )
        """).fetchone()
        has_queue = db.execute("SELECT count(*) FROM sqlite_master WHERE name='queue_metric_raw'").fetchone()[0]
        queues = db.execute("SELECT count(*) FROM queue_metric_raw").fetchone()[0] if has_queue else 0
        return raw, gap, queues


def benchmark(root, binary, vendor, seconds, interval, redis_socket, enabled, loaded):
    root.mkdir()
    state = root / "state"
    with (root / "daemon.log").open("w") as log:
        daemon = subprocess.Popen([binary.resolve(), "--state-dir", state], stdout=log, stderr=log)
        client = None
        try:
            client = FIXTURE["ready"](lambda: FIXTURE["connect"](state))
            launch, running = FIXTURE["start_supervision"](root, client, 31, loaded)
            supervised = {"id": launch["id"]}
            projects = []
            for i in range(8):
                app = root / f"queue-{i}"
                FIXTURE["app"](app, vendor, redis_socket)
                (app / "record-usage").touch()
                names = ["primary"] + [f"q-{q:02}" for q in range(15)]
                project = FIXTURE["register"](client, app, [("db", "database", names), ("redis", "redis", names)])
                projects.append((app, project))
                if enabled:
                    client.request("metrics.queue.configure", {**project, "enabled": True})
            if enabled:
                # Begin after all projects have sampled. Keep measurement scoped to
                # a steady interval and exclude start-up PHP samples by timestamp.
                def first_samples():
                    client.request("daemon.status")
                    return all((app / "usage.jsonl").exists() for app, _ in projects)

                FIXTURE["ready"](first_samples, 20)
            cpu_start, rss_peak = HELPERS["process_usage"](daemon.pid)
            size_start = size_peak = HELPERS["state_size"](state)
            elapsed_start = time.monotonic()
            wall_start = time.time()
            methods = [("daemon.status", {}), ("process.list", supervised),
                       ("metrics.process.current", supervised), ("metrics.project.summary", supervised)]
            if enabled:
                methods.append(("metrics.queue.current", projects[0][1]))
            latencies = {name: [] for name, _ in methods}
            while time.monotonic() - elapsed_start < seconds:
                for name, params in methods:
                    before = time.monotonic()
                    client.request(name, params)
                    latencies[name].append((time.monotonic() - before) * 1000)
                rss_peak = max(rss_peak, HELPERS["process_usage"](daemon.pid)[1])
                size_peak = max(size_peak, HELPERS["state_size"](state))
                time.sleep(interval)
            wall_end = time.time()
            elapsed = time.monotonic() - elapsed_start
            cpu_end, _ = HELPERS["process_usage"](daemon.pid)
            assert running()
            usages = []
            for app, _ in projects:
                log = app / "usage.jsonl"
                if log.exists():
                    usages.extend(row for row in map(json.loads, log.read_text().splitlines())
                                  if wall_start <= row["at"] <= wall_end)
            if enabled:
                assert usages and all(FIXTURE["inspect"](client, project)[("redis", "primary")]["state"] == "fresh"
                                      for _, project in projects)
            daemon_cpu = cpu_end - cpu_start
            php_cpu = sum(row["cpu_seconds"] for row in usages)
            raw, gap, queue = database_stats(state)
            print(json.dumps({"workload": "loaded" if loaded else "idle", "queue_enabled": enabled,
                "seconds": round(elapsed, 3), "daemon_cpu_seconds": round(daemon_cpu, 3),
                "php_probe_cpu_seconds": round(php_cpu, 3),
                "daemon_plus_count_php_core_percent": round(100 * (daemon_cpu + php_cpu) / elapsed, 3),
                "completed_php_probes": len(usages), "daemon_peak_rss_kib": rss_peak,
                "peak_single_php_rss_bytes": max((row["peak_rss_bytes"] for row in usages), default=0),
                "sqlite_main_and_journal_growth_bytes": size_peak - size_start,
                "process_sample_rows_since_startup": raw, "process_sample_max_gap_seconds": gap,
                "queue_samples_since_startup": queue,
                "rpc_ms": {name: HELPERS["percentiles"](times) for name, times in latencies.items()}}), flush=True)
        finally:
            try:
                if client:
                    client.close()
            finally:
                if daemon.poll() is None:
                    daemon.terminate()
                daemon.wait(timeout=40)


def seeded_storage(root):
    storage = runpy.run_path(str(ROOT / "scripts/benchmark-process-metrics-storage.py"))
    path = root / "seeded.sqlite3"
    with sqlite3.connect(path) as db:
        raw_process, rollup_process = storage["seed"](db)
        initial = storage["size"](db)
        db.executescript("""
            WITH RECURSIVE selectors(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM selectors WHERE n<255)
            INSERT INTO queue_metric_series (project_id,connection,queue)
              SELECT 'benchmark','database','queue-'||n FROM selectors;
            WITH RECURSIVE ticks(s) AS (VALUES(0) UNION ALL SELECT s+10 FROM ticks WHERE s<86390)
            INSERT INTO queue_metric_raw (series_id,observed_at,pending)
              SELECT series.id,1800000000+s,1 FROM ticks CROSS JOIN queue_metric_series series;
        """)
        raw_bytes = storage["size"](db) - initial
        db.executescript("""
            WITH RECURSIVE buckets(s) AS (VALUES(0) UNION ALL SELECT s+300 FROM buckets WHERE s<604500)
            INSERT INTO queue_metric_rollups (series_id,bucket,samples,observations,mean,minimum,maximum)
              SELECT series.id,1800000000-604800+s,30,30,1,1,1
              FROM buckets CROSS JOIN queue_metric_series series;
        """)
        rollup_bytes = storage["size"](db) - initial - raw_bytes
        source = (ROOT / "crates/roost-store/src/queue_metrics.rs").read_text()
        statements = re.findall(r'c"([^"]+)"', source)

        def cleanup():
            with db:
                db.execute("BEGIN IMMEDIATE")
                end = "1800000300"
                db.execute(next(q for q in statements if q.startswith("INSERT OR IGNORE INTO queue_metric_rollups")), (end,))
                db.execute(next(q for q in statements if q.startswith("UPDATE queue_metric_state SET rolled_until")), (end,))
                for table, watermark, floor in (("raw", "raw_pruned", end), ("rollups", "rollup_pruned", "1800000000")):
                    choose = next(q for q in statements if q.startswith(
                        f"SELECT coalesce(max(sequence),0) FROM (SELECT sequence FROM queue_metric_{table}"))
                    last = db.execute(choose, (floor,)).fetchone()[0]
                    if last:
                        delete = next(q for q in statements if q.startswith(f"DELETE FROM queue_metric_{table} WHERE sequence"))
                        db.execute(delete, (str(last), floor))
                        mark = next(q for q in statements if q.startswith(f"UPDATE queue_metric_state SET {watermark}"))
                        db.execute(mark, (str(last),))
                db.execute(next(q for q in statements if q.startswith("DELETE FROM queue_metric_series")))

        raw_history = next(q for q in statements if q.startswith("SELECT json_object(\n")
                           and "FROM queue_metric_raw r JOIN" in q)
        history_ms = {}
        for description, start in (("raw_recent", 1800085000), ("raw_full_day", 1800000000)):
            first = db.execute("SELECT min(sequence) FROM queue_metric_raw WHERE observed_at>=?", (start,)).fetchone()[0]
            args = ("benchmark", None, None, str(start), "1800086400", str(first-1), "2211840", "257")
            records, elapsed = storage["timed"](lambda: db.execute(raw_history, args).fetchall())
            assert len(records) == 257
            history_ms[description] = elapsed
        _, cleanup_ms = storage["timed"](cleanup)
        print(json.dumps({"case": "seeded_at_capacity", "queue_raw_rows": 2211840,
                          "queue_rollup_rows": 516096, "queue_raw_bytes": raw_bytes,
                          "queue_rollup_bytes": rollup_bytes,
                          "process_metric_bytes": raw_process + rollup_process,
                          "retention_commit_ms": cleanup_ms, "history_ms": history_ms,
                          "combined_db_bytes": storage["size"](db)}), flush=True)


def herd_resolution(root, vendor):
    fixture = root / "herd"
    FIXTURE["app"](fixture, vendor, root / "redis.sock")
    herd = Path.home() / "Library/Application Support/Herd/bin"
    command = [herd / "php", "-d", "display_errors=stderr", "-d", "log_errors=0", herd / "herd.phar", "which-php"]
    environment = {"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(Path.home()),
                   "TMPDIR": tempfile.gettempdir(), "USER": pwd.getpwuid(os.geteuid()).pw_name}
    initial = resource.getrusage(resource.RUSAGE_CHILDREN)
    durations = []
    for _ in range(24):
        started = time.monotonic()
        subprocess.run(command, cwd=fixture, env=environment, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        durations.append((time.monotonic() - started) * 1000)
    final = resource.getrusage(resource.RUSAGE_CHILDREN)
    print(json.dumps({"case": "herd_resolution_separate", "lookups": len(durations),
        "cpu_seconds": round(final.ru_utime + final.ru_stime - initial.ru_utime - initial.ru_stime, 3),
        "peak_child_rss_bytes": final.ru_maxrss, "lookup_ms": HELPERS["percentiles"](durations)}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--laravel13", type=Path)
    parser.add_argument("--seconds", default=40, type=int)
    parser.add_argument("--interval", default=2.0, type=float)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--storage-only", action="store_true")
    modes.add_argument("--herd-only", action="store_true")
    args = parser.parse_args()
    if args.herd_only and not args.laravel13:
        parser.error("--laravel13 is required for a Herd measurement")
    if not (args.storage_only or args.herd_only) and not all((args.baseline, args.candidate, args.laravel13)):
        parser.error("--baseline, --candidate, and --laravel13 are required for live runs")
    with tempfile.TemporaryDirectory(prefix="roost-qb-", dir="/private/tmp") as location:
        root = Path(location)
        if args.herd_only:
            herd_resolution(root, args.laravel13.resolve() / "vendor")
            return
        if args.storage_only:
            seeded_storage(root)
            return
        sock = root / "redis.sock"
        redis = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", str(sock),
            "--unixsocketperm", "700", "--save", "", "--appendonly", "no"], cwd=root, stdout=subprocess.DEVNULL)
        try:
            FIXTURE["ready"](sock.exists)
            subprocess.run(["redis-cli", "-s", str(sock), "acl", "setuser", "roost_probe", "on",
                ">fixture-password", "~queues:*", "-@all", "+llen", "+select"], check=True, stdout=subprocess.DEVNULL)
            for label, loaded in (("idle", False), ("loaded", True)):
                for name, binary, enabled in (("baseline", args.baseline, False), ("candidate", args.candidate, True)):
                    benchmark(root / f"{label}-{name}", binary, args.laravel13.resolve() / "vendor",
                              args.seconds, args.interval, sock, enabled, loaded)
            seeded_storage(root)
        finally:
            redis.terminate()
            redis.wait()


if __name__ == "__main__":
    main()
