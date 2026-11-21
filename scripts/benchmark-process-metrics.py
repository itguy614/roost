"""Compare baseline and candidate Roost daemons with disposable macOS fixtures."""

import argparse
import json
import math
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import time
import uuid


class Client:
    def __init__(self, state):
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(5)
        self.socket.connect(str(state / "roost.sock"))
        self.stream = self.socket.makefile("rwb")
        self.capabilities = self.request("daemon.hello", {"protocol_version": 1})["capabilities"]

    def request(self, method, params=None):
        request = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
        self.stream.write(json.dumps(request).encode() + b"\n")
        self.stream.flush()
        reply = json.loads(self.stream.readline())
        if "error" in reply:
            raise RuntimeError(f"{method}: {reply['error']}")
        return reply["result"]

    def close(self):
        self.stream.close()
        self.socket.close()


def percentiles(values):
    sorted_values = sorted(values)
    return {
        f"p{p}": round(sorted_values[math.ceil(len(values) * p / 100) - 1], 3)
        for p in (50, 95, 99)
    }


def process_usage(pid):
    text = subprocess.check_output(["ps", "-p", str(pid), "-o", "time=,rss="], text=True)
    cpu, rss = text.split()
    seconds = 0
    for part in cpu.split(":"):
        seconds = seconds * 60 + float(part)
    return seconds, int(rss)


def state_size(state):
    return sum(path.stat().st_size for path in state.glob("roost.sqlite3*"))


def run(daemon_path, duration, loaded, common_reads, root, php):
    root.mkdir()
    app = root / "app"
    app.mkdir()
    (app / "artisan").write_text("loaded" if loaded else "graceful")
    state = root / "state"
    log = (root / "daemon.log").open("w")
    daemon = subprocess.Popen([daemon_path, "--state-dir", state], stdout=log, stderr=log)
    client = None
    try:
        ready = time.monotonic() + 15
        while client is None:
            if daemon.poll() is not None or time.monotonic() >= ready:
                raise RuntimeError((root / "daemon.log").read_text())
            try:
                client = Client(state)
            except OSError:
                time.sleep(.05)
        idle_rss = process_usage(daemon.pid)[1]
        project = client.request("project.add", {"path": str(app)})["result"]["id"]
        p = {"id": project}
        for name, instances in (("first", 16), ("second", 15)):
            client.request("worker.profile.create", {
                "project_id": project, "name": name, "connection": "database",
                "options": {"queues": ["critical", "default"], "instances": instances, "graceful_stop_seconds": 1},
            })
        client.request("scheduler.configure", {**p, "options": {"graceful_stop_seconds": 1}})
        launch = {**p, "php": str(php)}
        client.request("project.start", launch)
        client.request("scheduler.start", launch)
        ready = time.monotonic() + 20
        while time.monotonic() < ready:
            workers = client.request("process.list", p)
            scheduler = client.request("scheduler.status", p).get("process")
            if len(workers) == 31 and all(w["state"] == "running" for w in workers) and scheduler and scheduler["state"] == "running":
                break
            time.sleep(.05)
        else:
            raise RuntimeError("31 workers and one scheduler did not reach running")
        peak_rss = idle_rss
        initial_bytes = peak_bytes = state_size(state)
        counted_workers = len(workers) + 1
        metrics = "metrics.process.current" in client.capabilities
        from_time = int(time.time())
        methods = [("daemon.status", {}), ("process.list", p)]
        if metrics and not common_reads:
            methods += [
                ("metrics.process.current", p),
                ("metrics.project.summary", p),
                ("metrics.process.history", {"project_id": project, "from": from_time, "limit": 256}),
            ]
        latency = {name: [] for name, _ in methods}
        start = time.monotonic()
        restart_ms = None
        cpu_before, _ = process_usage(daemon.pid)
        while (elapsed := time.monotonic() - start) < duration:
            for method, params in methods:
                t = time.monotonic()
                client.request(method, params)
                latency[method].append((time.monotonic() - t) * 1000)
            if elapsed >= duration / 2 and restart_ms is None:
                t = time.monotonic()
                client.request("scheduler.restart", launch)
                restart_ms = round((time.monotonic() - t) * 1000, 3)
            peak_rss = max(peak_rss, process_usage(daemon.pid)[1])
            peak_bytes = max(peak_bytes, state_size(state))
            time.sleep(.2)
        cpu_after, _ = process_usage(daemon.pid)
        rows = None
        if metrics:
            with sqlite3.connect(state / "roost.sqlite3") as db:
                rows = db.execute("SELECT count(*) FROM process_metric_raw WHERE observed_at>=?", (from_time,)).fetchone()[0]
        return {
            "active_at_start": counted_workers, "mode": "loaded" if loaded else "idle",
            "elapsed_seconds": round(time.monotonic() - start, 2),
            "daemon_cpu_seconds": round(cpu_after - cpu_before, 2),
            "idle_rss_kib": idle_rss, "peak_rss_kib": peak_rss,
            "database_and_journal_peak_growth_bytes": peak_bytes - initial_bytes,
            "sample_rows": rows, "scheduler_restart_ms": restart_ms,
            "rpc_latency_ms": {name: percentiles(values) for name, values in latency.items()},
        }
    finally:
        if client:
            client.close()
        if daemon.poll() is None:
            daemon.terminate()
        daemon.wait(timeout=70)
        log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--loaded", action="store_true")
    parser.add_argument("--common-reads", action="store_true")
    args = parser.parse_args()
    if args.seconds < 4:
        parser.error("--seconds must be at least 4")
    with tempfile.TemporaryDirectory(prefix="roost-mb-", dir="/private/tmp") as path:
        root = Path(path)
        php = root / "php"
        source = Path(__file__).resolve().parents[1] / "crates/roost-daemon/tests/fixtures/queue_worker.rs"
        subprocess.run(["rustc", "--edition=2024", source, "-o", php], check=True)
        for name, daemon in (("baseline", args.baseline), ("candidate", args.candidate)):
            if daemon:
                result = run(daemon.resolve(), args.seconds, args.loaded, args.common_reads, root / name, php)
                print(json.dumps({"variant": name, **result}), flush=True)


if __name__ == "__main__":
    main()
