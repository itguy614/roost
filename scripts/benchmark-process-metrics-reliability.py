"""Run a live concurrent retention benchmark or a 24-hour process metrics soak."""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import runpy
import signal
import sqlite3
import subprocess
import threading
import time

HELPERS = runpy.run_path(str(Path(__file__).with_name("benchmark-process-metrics.py")))
Client = HELPERS["Client"]
percentiles = HELPERS["percentiles"]
process_usage = HELPERS["process_usage"]
state_size = HELPERS["state_size"]
DAY = 86_400


def connect(state, daemon):
    deadline = time.monotonic() + 10
    while daemon.poll() is None and time.monotonic() < deadline:
        try:
            return Client(state)
        except OSError:
            time.sleep(.05)
    raise RuntimeError("daemon did not open an IPC connection")


def configure(client, args, php):
    groups = []
    workloads = (("workers", 16), ("mixed", 14), ("crash", 1))
    for name, count in workloads:
        app = args.run_directory / name
        app.mkdir()
        mode = "soak_crash" if name == "crash" else "loaded" if args.seed_retention else "graceful"
        (app / "artisan").write_text(mode)
        project = client.request("project.add", {"path": str(app)})["result"]["id"]
        profile = client.request("worker.profile.create", {
            "project_id": project, "name": name, "connection": "database",
            "options": {"queues": ["critical", "default"], "instances": count, "graceful_stop_seconds": 1},
        })["result"]["id"]
        groups.append(({"id": project, "php": str(php)}, profile, count))
    scheduler = client.request("scheduler.configure", {
        "id": groups[1][0]["id"], "options": {"graceful_stop_seconds": 1},
    })["result"]["id"]
    return groups, scheduler


def seed(db_path, groups, scheduler):
    now = int(time.time())
    oldest = (now - DAY) // 300 * 300 - 300
    with sqlite3.connect(db_path, timeout=2) as db:
        db.executemany(
            """INSERT INTO process_metric_series
               (project_id,kind,profile_id,slot,instance_id,generation)
               VALUES (?,'worker',?,?,lower(hex(randomblob(16))),1)""",
            [(group["id"], profile, slot) for group, profile, count in groups for slot in range(count)],
        )
        db.execute(
            """INSERT INTO process_metric_series
               (project_id,kind,profile_id,slot,instance_id,generation)
               VALUES (?,'scheduler',?,0,lower(hex(randomblob(16))),1)""",
            (groups[1][0]["id"], scheduler),
        )
        db.execute("""
            WITH RECURSIVE ticks(s) AS (VALUES(0) UNION ALL SELECT s+2 FROM ticks WHERE s<86398)
            INSERT INTO process_metric_raw
              (series_id,observed_at,cpu_percent,rss_bytes,uptime_seconds,peak_rss_bytes)
            SELECT series.id,?+ticks.s,7.5,26843545,ticks.s,26843545
            FROM ticks CROSS JOIN process_metric_series series
        """, (oldest,))
        db.execute("""
            WITH RECURSIVE buckets(s) AS (VALUES(0) UNION ALL SELECT s+300 FROM buckets WHERE s<604500)
            INSERT INTO process_metric_rollups
              (series_id,bucket,cpu_count,cpu_mean,cpu_min,cpu_max,rss_count,rss_mean,rss_min,rss_max,peak_rss_bytes,max_uptime_seconds)
            SELECT series.id,?+buckets.s,150,7.5,0,10,150,26843545,26843545,26843545,26843545,buckets.s+300
            FROM buckets CROSS JOIN process_metric_series series
        """, (oldest - 7 * DAY,))
        db.execute("UPDATE process_metric_state SET rolled_until=?", (oldest,))
    return oldest


def snapshot(db_path, start):
    with sqlite3.connect(db_path, timeout=2) as db:
        db.execute("PRAGMA query_only=ON")
        raw_pruned, rollup_pruned, rolled_until = db.execute(
            "SELECT raw_pruned,rollup_pruned,rolled_until FROM process_metric_state"
        ).fetchone()
        sampling = db.execute(
            """SELECT count(*),count(DISTINCT observed_at),min(observed_at),max(observed_at)
               FROM process_metric_raw WHERE observed_at>=?""", (start,)
        ).fetchone()
        raw = db.execute("SELECT count(*) FROM process_metric_raw").fetchone()[0]
        rollups = db.execute("SELECT count(*) FROM process_metric_rollups").fetchone()[0]
        return {"raw_pruned_through": raw_pruned, "rollup_pruned_through": rollup_pruned,
                "rolled_until": rolled_until, "raw_rows": raw, "rollup_rows": rollups,
                "live_rows": sampling[0], "live_sample_seconds": sampling[1],
                "first_live_sample": sampling[2], "last_live_sample": sampling[3]}


def sample_gap(db_path, start):
    with sqlite3.connect(db_path, timeout=2) as db:
        return db.execute("""
            SELECT max(gap) FROM (
                SELECT observed_at - lag(observed_at) OVER (ORDER BY observed_at) AS gap
                FROM (SELECT DISTINCT observed_at FROM process_metric_raw WHERE observed_at>=?)
            )
        """, (start,)).fetchone()[0]


def reader(state, project, resolution, from_time, stop, latencies, failures):
    client = None
    method = "metrics.process.history"
    try:
        client = Client(state)
        while not stop.is_set():
            now = int(time.time())
            start = from_time if from_time is not None else now - 600
            t = time.monotonic()
            client.request(method, {"project_id": project, "resolution": resolution,
                                    "from": start, "to": now + 1, "limit": 256})
            latencies.append((time.monotonic() - t) * 1000)
            stop.wait(.25 if from_time is not None else 2)
    except (OSError, RuntimeError, ValueError) as error:
        failures.append(str(error))
    finally:
        if client:
            client.close()


def exercise(args, client, daemon, state, groups, scheduler):
    db_path = state / "roost.sqlite3"
    oldest = seed(db_path, groups, scheduler) if args.seed_retention else None
    for launch, _, _ in groups:
        client.request("project.start", launch)
    schedule = groups[1][0]
    client.request("scheduler.start", schedule)
    crash_app = args.run_directory / "crash"

    def count_running():
        workers = sum(
            sum(row["state"] == "running" for row in client.request("process.list", {"id": launch["id"]}))
            for launch, _, _ in groups
        )
        schedule_process = client.request("scheduler.status", {"id": schedule["id"]}).get("process")
        return workers + int(bool(schedule_process and schedule_process["state"] == "running"))

    ready = time.monotonic() + 30
    while count_running() != 32:
        if time.monotonic() >= ready:
            raise RuntimeError("32 managed processes failed to reach running")
        time.sleep(.1)

    start = time.monotonic()
    start_wall = int(time.time())
    base_cpu, peak_rss = process_usage(daemon.pid)
    base_size = peak_size = state_size(state)
    latencies = defaultdict(list)
    failures = []
    stop = threading.Event()
    readers = []
    targets = [("raw", None)]
    if oldest is not None:
        targets = [("raw", oldest), ("raw", start_wall - 1800), ("5m", oldest - 5 * DAY), ("5m", oldest)]
    for index, (resolution, cutoff) in enumerate(targets):
        label = f"{resolution}_reader_{index}"
        thread = threading.Thread(target=reader, args=(
            state, groups[index % len(groups)][0]["id"], resolution, cutoff,
            stop, latencies[label], failures,
        ))
        thread.start()
        readers.append(thread)

    def request(method, params=None):
        t = time.monotonic()
        result = client.request(method, params)
        latencies[method].append((time.monotonic() - t) * 1000)
        return result

    crash_count = schedule_restarts = 0
    next_crash = min(60, args.seconds / 4)
    next_restart = min(120, args.seconds / 2)
    next_progress = 0
    snapshot_interval = 15 if oldest is not None else 300
    minimum_running = 32
    incomplete_observations = 0
    try:
        while (elapsed := time.monotonic() - start) < args.seconds:
            request("daemon.status")
            request("process.list", {"id": groups[0][0]["id"]})
            request("metrics.process.current", {"id": schedule["id"]})
            summary = request("metrics.project.summary", {"id": groups[2][0]["id"]})
            if elapsed >= next_crash:
                (crash_app / "exit-once").touch(exist_ok=False)
                crash_count += 1
                next_crash += 1800
            if elapsed >= next_restart:
                request("scheduler.restart", schedule)
                schedule_restarts += 1
                next_restart += 3600
            if elapsed >= next_progress:
                next_progress = elapsed + snapshot_interval
                cpu, rss = process_usage(daemon.pid)
                active = count_running()
                peak_rss = max(peak_rss, rss)
                peak_size = max(peak_size, state_size(state))
                minimum_running = min(minimum_running, active)
                incomplete_observations += int(active != 32)
                print(json.dumps({"kind": "progress", "seconds": round(elapsed, 1), "running": active,
                                  "daemon_cpu_seconds": round(cpu - base_cpu, 2), "rss_kib": rss,
                                  "state_bytes": state_size(state), "crashes": summary["lifecycle"]["crashes"],
                                  **snapshot(db_path, start_wall)}), flush=True)
            if failures:
                raise RuntimeError(f"concurrent reader failed: {failures}")
            time.sleep(.25 if oldest is not None else 2)
    finally:
        stop.set()
        for thread in readers:
            thread.join(timeout=6)
    active = count_running()
    crash_summary = request("metrics.project.summary", {"id": groups[2][0]["id"]})
    schedule_summary = request("metrics.project.summary", {"id": schedule["id"]})
    cpu, rss = process_usage(daemon.pid)
    result = snapshot(db_path, start_wall)
    print(json.dumps({
        "kind": "result", "seconds": round(time.monotonic() - start, 2), "running": active,
        "minimum_running_at_checkpoint": minimum_running,
        "checkpoints_below_32": incomplete_observations, "induced_crashes": crash_count,
        "observed_crashes": crash_summary["lifecycle"]["crashes"],
        "worker_restarts": crash_summary["lifecycle"]["restarts"],
        "scheduler_restart_requests": schedule_restarts, "scheduler_restarts": schedule_summary["lifecycle"]["restarts"],
        "daemon_cpu_seconds": round(cpu - base_cpu, 2), "peak_rss_kib_at_checkpoint": max(peak_rss, rss),
        "peak_db_and_journal_growth_bytes_at_checkpoint": max(peak_size, state_size(state)) - base_size,
        "longest_persisted_sample_gap_seconds": sample_gap(db_path, start_wall),
        "rpc_latency_ms": {name: percentiles(values) for name, values in latencies.items() if values},
        "rpc_requests": {name: len(values) for name, values in latencies.items()},
        **result,
    }), flush=True)
    if (failures or active != 32 or crash_summary["lifecycle"]["crashes"] != crash_count
            or crash_summary["lifecycle"]["restarts"] != crash_count
            or schedule_summary["lifecycle"]["restarts"] != schedule_restarts
            or (oldest is not None and (result["raw_pruned_through"] == 0 or result["rollup_pruned_through"] == 0))):
        raise RuntimeError(f"reliability check failed: {failures}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--run-directory", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=DAY)
    parser.add_argument("--seed-retention", action="store_true")
    args = parser.parse_args()
    if args.seconds < 24:
        parser.error("--seconds must be at least 24")
    args.run_directory = args.run_directory.resolve()
    args.run_directory.mkdir(exist_ok=True)
    print(json.dumps({"kind": "started", "harness_pid": os.getpid(), "target_seconds": args.seconds}), flush=True)
    state = args.run_directory / "state"
    at = args.run_directory / "php"
    source = Path(__file__).resolve().parents[1] / "crates/roost-daemon/tests/fixtures/queue_worker.rs"
    subprocess.run(["rustc", "--edition=2024", source, "-o", at], check=True)
    with (args.run_directory / "daemon.log").open("x") as log:
        daemon = subprocess.Popen([args.candidate.resolve(), "--state-dir", state],
                                  stdout=log, stderr=log, stdin=subprocess.DEVNULL)
        client = None
        try:
            client = connect(state, daemon)
            groups, scheduler = configure(client, args, at)
            exercise(args, client, daemon, state, groups, scheduler)
        finally:
            if client:
                client.close()
            if daemon.poll() is None:
                daemon.terminate()
            daemon.wait(timeout=70)


if __name__ == "__main__":
    def stop_run(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop_run)
    main()
