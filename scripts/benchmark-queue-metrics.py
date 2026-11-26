"""Measure cumulative daemon and real PHP queue-probe costs at 32 processes."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import pwd
import resource
import runpy
import sqlite3
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = runpy.run_path(str(ROOT / "scripts/test-queue-metrics.py"))
HELPERS = FIXTURE["HELPERS"]


ACCOUNTING = r"""
#include <inttypes.h>
#include <libproc.h>
#include <mach/mach_time.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/resource.h>
int main(int argc, char **argv) {
    mach_timebase_info_data_t scale;
    if (argc < 2 || mach_timebase_info(&scale) != 0 || !scale.denom)
        return 1;
    for (int i = 1; i < argc; i++) {
        struct rusage_info_v3 usage = {0};
        if (proc_pid_rusage(atoi(argv[i]), RUSAGE_INFO_V3, (rusage_info_t *)&usage)) {
            perror("proc_pid_rusage");
            return 1;
        }
        printf("%" PRIu64 " %" PRIu64 "\n",
            (uint64_t)(((__uint128_t)usage.ri_user_time + usage.ri_system_time) * scale.numer / scale.denom),
            (uint64_t)(((__uint128_t)usage.ri_child_user_time + usage.ri_child_system_time) * scale.numer / scale.denom));
    }
    return 0;
}
"""


def cpu_tree(accounting, pids):
    output = subprocess.check_output([accounting, *map(str, pids)], text=True)
    return [tuple(int(value) / 1_000_000_000 for value in line.split())
            for line in output.splitlines()]


def cpu_seconds(accounting, pid):
    return cpu_tree(accounting, [pid])[0]


def guardian_pids(daemon_pid):
    output = subprocess.check_output(["pgrep", "-P", str(daemon_pid), "-f", "__worker-guardian"], text=True)
    return sorted(map(int, output.split()))


def cpu_snapshot(accounting, daemon_pid, redis_pid, guardians):
    daemon, redis, *live_guardians = cpu_tree(accounting, [daemon_pid, redis_pid, *guardians])
    return (*daemon, redis[0], sum(own for own, _ in live_guardians))


def calibrate(accounting):
    child_before = sum(resource.getrusage(resource.RUSAGE_CHILDREN)[:2])
    own_before, kernel_before = cpu_seconds(accounting, os.getpid())
    subprocess.run([sys.executable, "-c", "sum(i*i for i in range(8000000))"], check=True)
    child_after = sum(resource.getrusage(resource.RUSAGE_CHILDREN)[:2])
    own_after, kernel_after = cpu_seconds(accounting, os.getpid())
    elapsed = child_after - child_before
    reported = kernel_after - kernel_before
    assert own_after >= own_before and abs(elapsed - reported) < .015, (elapsed, reported)
    print(json.dumps({"case": "darwin_child_cpu_calibration", "getrusage_seconds": elapsed,
                      "proc_pid_rusage_seconds": reported}), flush=True)


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


def benchmark(root, binary, accounting, vendor, seconds, interval, redis_socket, redis_pid, variant, loaded,
              enabled_projects=None, soak=False):
    enabled_projects = (0 if variant == "baseline" else 8) if enabled_projects is None else enabled_projects
    enabled = enabled_projects > 0
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
                if i < enabled_projects:
                    client.request("metrics.queue.configure", {**project, "enabled": True})
            if enabled:
                # Begin after all projects have sampled. Keep measurement scoped to
                # a steady interval and exclude start-up PHP samples by timestamp.
                def first_samples():
                    client.request("daemon.status")
                    return all((app / "usage.jsonl").exists() for app, _ in projects[:enabled_projects])

                FIXTURE["ready"](first_samples, 20)
            _, rss_peak = HELPERS["process_usage"](daemon.pid)
            guardians = guardian_pids(daemon.pid)
            assert len(guardians) == 32, guardians
            cpu_start, child_start, redis_start, guardian_start = cpu_snapshot(accounting, daemon.pid, redis_pid, guardians)
            size_start = size_peak = HELPERS["state_size"](state)
            elapsed_start = time.monotonic()
            wall_start = time.time()
            methods = [("daemon.status", {}), ("process.list", supervised),
                       ("metrics.process.current", supervised), ("metrics.project.summary", supervised),
                       ("metrics.process.history", {"project_id": supervised["id"],
                         "from": int(wall_start) - 600, "limit": 256})]
            if enabled or variant == "scale":
                methods.append(("metrics.queue.current", projects[0][1]))
            latencies = {name: [] for name, _ in methods}
            with contextlib.ExitStack() as cleanup:
                faults = cleanup.enter_context(queue_faults(root, daemon.pid, redis_socket, projects[0])) if soak else None
                next_checkpoint = 0
                while (running_for := time.monotonic() - elapsed_start) < seconds:
                    if faults:
                        faults.tick(client, running_for)
                    for name, params in methods:
                        before = time.monotonic()
                        client.request(name, params)
                        latencies[name].append((time.monotonic() - before) * 1000)
                    rss_peak = max(rss_peak, HELPERS["process_usage"](daemon.pid)[1])
                    size_peak = max(size_peak, HELPERS["state_size"](state))
                    if faults and running_for >= next_checkpoint:
                        own, children, backend, custody = cpu_snapshot(accounting, daemon.pid, redis_pid, guardians)
                        print(json.dumps({"case": "soak_checkpoint", "seconds": round(running_for, 2),
                            "daemon_cpu_seconds": round(own-cpu_start, 3), "child_cpu_seconds": round(children-child_start, 3),
                            "redis_cpu_seconds": round(backend-redis_start, 3),
                            "guardian_cpu_seconds": round(custody-guardian_start, 3),
                            "worst_rpc_p95_ms_so_far": max(HELPERS["percentiles"](times)["p95"] for times in latencies.values()),
                            "peak_daemon_rss_kib": rss_peak, "peak_db_and_journal_bytes": size_peak,
                            **faults.result()}), flush=True)
                        next_checkpoint += 300
                    time.sleep(interval)
                fault_result = faults.result() if faults else {}
                if faults:
                    expected = len(range(45, seconds - 50, 300))
                    assert expected > 0 and faults.failures == faults.recoveries == expected, fault_result
                    assert any(offset > 120 for offset in faults.herd_lookups.values()), fault_result
            wall_end = time.time()
            elapsed = time.monotonic() - elapsed_start
            cpu_end, child_end, redis_end, guardian_end = cpu_snapshot(accounting, daemon.pid, redis_pid, guardians)
            assert guardians == guardian_pids(daemon.pid)
            assert running()
            usages = []
            for app, _ in projects:
                log = app / "usage.jsonl"
                if log.exists():
                    usages.extend(row for row in map(json.loads, log.read_text().splitlines())
                                  if wall_start <= row["at"] <= wall_end)
            if enabled:
                assert usages and all(FIXTURE["inspect"](client, project)[("redis", "primary")]["state"] == "fresh"
                                      for _, project in projects[:enabled_projects])
            daemon_cpu = cpu_end - cpu_start
            children_cpu = child_end - child_start
            guardian_cpu = guardian_end - guardian_start
            redis_cpu = redis_end - redis_start
            # The measurement window has no guardian replacements. All
            # activity charged to reaped children comes from the probes and
            # their descendants, including Herd resolution subprocesses.
            if enabled:
                assert children_cpu > 0, "Darwin did not report reaped probe CPU"
            php_cpu = sum(row["cpu_seconds"] for row in usages)
            raw, gap, queue = database_stats(state)
            print(json.dumps({"case": "soak_complete" if soak else "live",
                "workload": "loaded" if loaded else "idle", "variant": variant, "queue_enabled": enabled,
                "enabled_projects": enabled_projects,
                "seconds": round(elapsed, 3), "daemon_cpu_seconds": round(daemon_cpu, 3),
                "php_count_probe_cpu_seconds_at_shutdown": round(php_cpu, 3),
                "reaped_children_cpu_seconds_including_herd": round(children_cpu, 3),
                "daemon_plus_all_reaped_children_core_percent": round(100 * (daemon_cpu + children_cpu) / elapsed, 3),
                "guardian_cpu_seconds": round(guardian_cpu, 3), "redis_cpu_seconds": round(redis_cpu, 3),
                "cpu_including_guardians_and_redis_core_percent": round(100 * (daemon_cpu + children_cpu + guardian_cpu + redis_cpu) / elapsed, 3),
                "completed_php_probes": len(usages), "daemon_peak_rss_kib": rss_peak,
                "peak_single_php_rss_bytes": max((row["peak_rss_bytes"] for row in usages), default=0),
                "sqlite_main_and_journal_growth_bytes": size_peak - size_start,
                "peak_db_and_journal_bytes": size_peak,
                "process_sample_rows_since_startup": raw, "process_sample_max_gap_seconds": gap,
                "queue_samples_since_startup": queue,
                **fault_result,
                "rpc_ms": {name: HELPERS["percentiles"](times) for name, times in latencies.items()}}), flush=True)
        finally:
            try:
                if client:
                    client.close()
            finally:
                if daemon.poll() is None:
                    daemon.terminate()
                daemon.wait(timeout=40)


@contextlib.contextmanager
def queue_faults(root, daemon_pid, healthy, project):
    fixture, params = project
    silent_socket = root / "unresponsive.sock"
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(str(silent_socket))
        listener.listen(128)
        faults = Faults(fixture, params, healthy, silent_socket)
        stop = threading.Event()

        def observe_herd():
            while not stop.is_set():
                result = subprocess.run(["pgrep", "-P", str(daemon_pid), "-f", "herd.phar"],
                                        capture_output=True, text=True)
                if result.returncode in (0, 1):
                    for pid in result.stdout.split():
                        faults.herd_lookups.setdefault(pid, time.monotonic() - faults.started)
                stop.wait(.15)

        monitor = threading.Thread(target=observe_herd)
        monitor.start()
        try:
            yield faults
        finally:
            stop.set()
            monitor.join(timeout=10)
            (fixture / "redis-socket").write_text(str(healthy))


class Faults:
    def __init__(self, fixture, params, healthy, silent):
        self.fixture, self.params = fixture, params
        self.healthy, self.silent = healthy, silent
        self.started = time.monotonic()
        self.herd_lookups = {}
        self.next_fault = 45
        self.restore_at = None
        self.pending = None
        self.failures = 0
        self.recoveries = 0

    def tick(self, client, now):
        if self.restore_at is None and now >= self.next_fault:
            assert self.pending is None
            self.restore_at = self.next_fault + 25
            self.next_fault += 300
            self.pending = "failure"
            (self.fixture / "redis-socket").write_text(str(self.silent))
            print(json.dumps({"case": "soak_fault", "seconds": round(now, 3), "kind": "injected"}), flush=True)
        if self.restore_at is not None and now >= self.restore_at:
            assert self.pending == "recovery", "fault window missed its timeout"
            self.restore_at = None
            (self.fixture / "redis-socket").write_text(str(self.healthy))
        if self.pending:
            reading = FIXTURE["inspect"](client, self.params)[("redis", "primary")]
            if self.pending == "failure" and reading.get("reason") == "timeout":
                assert reading["state"] in ("stale", "unknown")
                self.failures += 1
                self.pending = "recovery"
                print(json.dumps({"case": "soak_fault", "seconds": round(now, 3), "kind": "timeout"}), flush=True)
            elif self.pending == "recovery" and self.restore_at is None and reading["state"] == "fresh":
                self.recoveries += 1
                self.pending = None
                print(json.dumps({"case": "soak_fault", "seconds": round(now, 3), "kind": "recovered"}), flush=True)

    def result(self):
        return {"timeouts_observed": self.failures, "recoveries_observed": self.recoveries,
                "herd_resolution_processes_observed": len(self.herd_lookups),
                "herd_resolutions_after_120s": sum(offset > 120 for offset in self.herd_lookups.copy().values())}


def seeded_storage(root, cadence=10, flapping=False):
    storage = runpy.run_path(str(ROOT / "scripts/benchmark-process-metrics-storage.py"))
    path = root / f"seeded-{cadence}-{flapping}.sqlite3"
    with sqlite3.connect(path) as db:
        raw_process, rollup_process = storage["seed"](db)
        initial = storage["size"](db)
        db.executescript("""
            WITH RECURSIVE selectors(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM selectors WHERE n<255)
            INSERT INTO queue_metric_series (project_id,connection,queue)
              SELECT 'benchmark','database','queue-'||n FROM selectors;
            INSERT INTO queue_probe_projects VALUES ('benchmark','fixture');
            INSERT INTO queue_metric_observed
              SELECT project_id,connection,queue,queue,NULL FROM queue_metric_series;
        """)
        with db:
            # Thirty- and sixty-second candidates retain the poll tick on
            # every outage and recovery, in addition to periodic samples.
            db.execute("""
                WITH RECURSIVE ticks(s) AS (VALUES(0) UNION ALL SELECT s+10 FROM ticks WHERE s<86390)
                INSERT INTO queue_metric_raw (series_id,observed_at,pending,reason,flags)
                SELECT series.id,1800000000+s,
                    CASE WHEN (?2 AND (s/10)%2=1) OR (NOT ?2 AND series.id<=16
                        AND s IN (9010,9020,18020,45050)) THEN NULL ELSE (s/10+series.id)%9 END,
                    CASE WHEN (?2 AND (s/10)%2=1) OR (NOT ?2 AND series.id<=16
                        AND s IN (9010,9020,18020,45050)) THEN 'timeout' ELSE NULL END,
                    (CASE WHEN s%?1=0 THEN 1 ELSE 0 END) |
                    (CASE WHEN s=0 THEN 2 WHEN ?2 OR (series.id<=16
                       AND s IN (9010,9030,18020,18030,45050,45060)) THEN 12 ELSE 0 END)
                FROM ticks CROSS JOIN queue_metric_series series
                WHERE ?2 OR s%?1=0 OR (series.id <= 16 AND s IN (9010,9030,18020,18030,45050,45060))
            """, (cadence, flapping))
        raw_rows = db.execute("SELECT count(*) FROM queue_metric_raw").fetchone()[0]
        if flapping:
            transition_rows = db.execute("SELECT count(*) FROM queue_metric_raw WHERE flags & 12 != 0").fetchone()[0]
            assert transition_rows == 256 * 8639
        else:
            transition_rows = db.execute("""SELECT count(*) FROM queue_metric_raw WHERE flags & 12 != 0
            """).fetchone()[0]
            assert transition_rows == 96
        raw_bytes = storage["size"](db) - initial
        db.executescript(f"""
            WITH RECURSIVE buckets(s) AS (VALUES(0) UNION ALL SELECT s+300 FROM buckets WHERE s<604500)
            INSERT INTO queue_metric_rollups (series_id,bucket,samples,observations,mean,minimum,maximum,
               periodic_observations,transition_observations,legacy_observations)
              SELECT series.id,1800000000-604800+s,{5 if flapping else 300 // cadence},
                {30 if flapping else 300 // cadence},1,1,1,{300 // cadence},{30 if flapping else 0},0
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
                db.execute(next(q for q in statements if q.startswith("DELETE FROM queue_metric_observed")))

        raw_history = next(q for q in statements if q.startswith("SELECT json_object(\n")
                           and "FROM queue_metric_raw r JOIN" in q)
        history_ms = {}
        for description, start in (("raw_recent", 1800085000), ("raw_full_day", 1800000000)):
            first = db.execute("SELECT min(sequence) FROM queue_metric_raw WHERE observed_at>=?", (start,)).fetchone()[0]
            args = ("benchmark", None, None, str(start), "1800086400", str(first-1), str(raw_rows), "257")
            records, elapsed = storage["timed"](lambda: db.execute(raw_history, args).fetchall())
            assert len(records) == 257
            history_ms[description] = elapsed
        journal_peak = 0
        stop = threading.Event()

        def sample_journal():
            nonlocal journal_peak
            journal = path.with_name(path.name + "-journal")
            while not stop.is_set():
                try:
                    journal_peak = max(journal_peak, journal.stat().st_size)
                except FileNotFoundError:
                    pass
                stop.wait(0.0005)

        observer = threading.Thread(target=sample_journal)
        observer.start()
        try:
            _, cleanup_ms = storage["timed"](cleanup)
        finally:
            stop.set()
            observer.join()
        print(json.dumps({"case": "seeded_at_capacity", "persistence_seconds": cadence, "flapping_all_selectors": flapping,
                          "queue_raw_rows": raw_rows, "expected_periodic_samples_per_bucket": 300 // cadence,
                          "availability_transition_rows": transition_rows,
                          "queue_rollup_rows": 516096, "queue_raw_bytes": raw_bytes,
                          "queue_rollup_bytes": rollup_bytes,
                          "process_metric_bytes": raw_process + rollup_process,
                          "retention_commit_ms": cleanup_ms, "history_ms": history_ms,
                          "combined_db_bytes": storage["size"](db), "rollback_journal_sampled_peak_bytes": journal_peak,
                          "combined_db_and_peak_journal_bytes": storage["size"](db) + journal_peak}), flush=True)


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
    parser.add_argument("--seconds", default=150, type=int)
    parser.add_argument("--uncached", type=Path, help="also measure the prior queue build for 60 seconds")
    parser.add_argument("--candidate-only", action="store_true", help="repeat only the cached candidate")
    parser.add_argument("--workload", choices=["idle", "loaded", "both"], default="both")
    parser.add_argument("--interval", default=2.0, type=float)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--storage-only", action="store_true")
    modes.add_argument("--herd-only", action="store_true")
    modes.add_argument("--scale", action="store_true")
    modes.add_argument("--soak", action="store_true")
    args = parser.parse_args()
    if args.herd_only and not args.laravel13:
        parser.error("--laravel13 is required for a Herd measurement")
    if args.soak and args.seconds < 3600:
        parser.error("--soak requires at least 3600 seconds")
    if not (args.storage_only or args.herd_only) and not all((args.baseline or args.candidate_only or args.scale or args.soak, args.candidate, args.laravel13)):
        parser.error("live runs require --candidate, --laravel13, and either --baseline or --candidate-only")
    with tempfile.TemporaryDirectory(prefix="roost-qb-", dir="/private/tmp") as location:
        root = Path(location)
        if args.herd_only:
            herd_resolution(root, args.laravel13.resolve() / "vendor")
            return
        if args.storage_only:
            for cadence in (10, 30, 60):
                seeded_storage(root, cadence)
            seeded_storage(root, 30, flapping=True)
            return
        source = root / "accounting.c"
        source.write_text(ACCOUNTING)
        accounting = root / "accounting"
        subprocess.run(["cc", "-O2", source, "-o", accounting], check=True)
        calibrate(accounting)
        sock = root / "redis.sock"
        redis = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", str(sock),
            "--unixsocketperm", "700", "--save", "", "--appendonly", "no"], cwd=root, stdout=subprocess.DEVNULL)
        try:
            FIXTURE["ready"](sock.exists)
            subprocess.run(["redis-cli", "-s", str(sock), "acl", "setuser", "roost_probe", "on",
                ">fixture-password", "~queues:*", "-@all", "+llen", "+select"], check=True, stdout=subprocess.DEVNULL)
            if args.soak:
                benchmark(root / "soak", args.candidate, accounting, args.laravel13.resolve() / "vendor",
                          args.seconds, args.interval, sock, redis.pid, "soak", False, 8, True)
                return
            if args.scale:
                for enabled_projects in (0, 1, 2, 4, 8):
                    benchmark(root / f"scale-{enabled_projects}", args.candidate, accounting,
                              args.laravel13.resolve() / "vendor", args.seconds, args.interval,
                              sock, redis.pid, "scale", args.workload == "loaded", enabled_projects)
                return
            for label, loaded in (("idle", False), ("loaded", True)):
                if args.workload not in ("both", label):
                    continue
                variants = [] if args.candidate_only else [("baseline", args.baseline, args.seconds)]
                variants.append(("cached", args.candidate, args.seconds))
                if args.uncached and not loaded:
                    variants.append(("uncached", args.uncached, 60))
                for variant, binary, seconds in variants:
                    benchmark(root / f"{label}-{variant}", binary, accounting, args.laravel13.resolve() / "vendor",
                              seconds, args.interval, sock, redis.pid, variant, loaded)
            seeded_storage(root)
        finally:
            redis.terminate()
            redis.wait()


if __name__ == "__main__":
    main()
