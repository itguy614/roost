"""Run end-to-end pending counts on disposable Laravel and Redis fixtures."""

import argparse
import json
from pathlib import Path
import re
import runpy
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
HELPERS = runpy.run_path(str(ROOT / "scripts/benchmark-process-metrics.py"))
Client = HELPERS["Client"]


def ready(factory, seconds=12):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        result = factory()
        if result:
            return result
        time.sleep(.1)
    raise AssertionError("fixture condition did not settle")


def connect(state):
    try:
        return Client(state)
    except OSError:
        return None


def app(root, vendor, redis_socket):
    root.mkdir()
    (root / "vendor").symlink_to(vendor, target_is_directory=True)
    (root / "bootstrap/cache").mkdir(parents=True)
    (root / "config").mkdir()
    (root / "artisan").touch()
    (root / "redis-socket").write_text(str(redis_socket))
    shutil.copyfile(ROOT / "crates/roost-daemon/tests/fixtures/queue-metrics/app.php", root / "bootstrap/app.php")
    now = int(time.time())
    with sqlite3.connect(root / "jobs.sqlite3") as db:
        db.execute("CREATE TABLE jobs (id INTEGER, queue TEXT, reserved_at INTEGER, available_at INTEGER)")
        db.executemany("INSERT INTO jobs VALUES (?,?,?,?)", [
            (1, "primary", None, now - 100), (2, "primary", None, now + 600),
            (3, "primary", now - 100, now - 100), (4, "other", None, now - 100),
        ])


def register(client, root, groups):
    project = client.request("project.add", {"path": str(root)})["result"]["id"]
    for name, connection, queues in groups:
        client.request("worker.profile.create", {"project_id": project, "name": name,
                       "connection": connection, "options": {"queues": queues}})
    return {"id": project}


def start_supervision(root, client, workers=1, loaded=False):
    fixture = root / "supervised"
    fixture.mkdir()
    (fixture / "artisan").write_text("loaded" if loaded else "graceful")
    php = root / "fixture-php"
    subprocess.run(["rustc", "--edition=2024", ROOT / "crates/roost-daemon/tests/fixtures/queue_worker.rs",
                    "-o", php], check=True)
    params = {"id": client.request("project.add", {"path": str(fixture)})["result"]["id"]}
    for index in range(0, workers, 16):
        client.request("worker.profile.create", {"project_id": params["id"], "name": str(index),
                       "connection": "database", "options": {"instances": min(16, workers-index),
                        "queues": ["critical", "default"], "graceful_stop_seconds": 1}})
    client.request("scheduler.configure", {**params, "options": {"graceful_stop_seconds": 1}})
    launch = {**params, "php": str(php)}
    client.request("project.start", launch)
    client.request("scheduler.start", launch)

    def running():
        slots = client.request("process.list", params)
        scheduler = client.request("scheduler.status", params).get("process")
        return len(slots) == workers and all(s["state"] == "running" for s in slots) and scheduler and scheduler["state"] == "running"

    ready(running)
    return launch, running


def inspect(client, params):
    readings = client.request("metrics.queue.current", params)
    return {(q["connection"], q["requested_queue"]): q["pending"] for q in readings["queues"]}


def db_rows(root):
    with sqlite3.connect(root / "jobs.sqlite3") as db:
        return db.execute("SELECT * FROM jobs ORDER BY id").fetchall()


def matrix(args, root, client, redis, redis_socket, launch, running):
    tracked = [
        (("database", None), "fresh", 1), (("database", "empty"), "fresh", 0),
        (("database", "other"), "fresh", 1), (("redis", None), "fresh", 1),
        (("redis", "primary"), "fresh", 1), (("redis", "empty"), "fresh", 0),
        (("redis", "other"), "fresh", 1), (("sync", "other"), "unsupported", None),
        (("missing", "other"), "unknown", None), (("custom", None), "unsupported", None),
    ]
    seeded = {
        "queues:primary": ("lrange", "0", "-1"),
        "queues:other": ("lrange", "0", "-1"),
        "queues:primary:delayed": ("zrange", "0", "-1"),
        "queues:primary:reserved": ("zrange", "0", "-1"),
    }

    def queue_contents():
        return {key: redis(op, key, *rest) for key, (op, *rest) in seeded.items()}

    before_redis = queue_contents()
    projects = []
    for version, supplied in (("12.50.0", args.laravel12), ("13.43.0", args.laravel13)):
        fixture = root / version
        app(fixture, supplied.resolve() / "vendor", redis_socket)
        (fixture / "custom-connector").touch()
        groups = [("database-default", "database", []), ("database-other", "database", ["other", "empty"]),
                  ("redis-default", "redis", []), ("redis-other", "redis", ["primary", "other", "empty"]),
                  ("sync", "sync", ["other"]), ("missing", "missing", ["other"]), ("custom", "custom", [])]
        params = register(client, fixture, groups)
        assert client.request("metrics.queue.current", params) == {
            "project_id": params["id"], "enabled": False, "queues": [], "polling": {"state": "disabled"},
        }
        if not projects:
            revision = client.request("project.get", params)["revision"]
            client.request("watch.enable", {**params, "expected_revision": revision,
                "rules": {"include": ["queue-default.php"], "exclude": []}})
            ready(lambda: client.request("watch.status", params)["state"] == "watching")
        projects.append((version, fixture, params, db_rows(fixture)))
        client.request("metrics.queue.configure", {**params, "enabled": True})

    for version, fixture, params, before in projects:
        try:
            result = ready(lambda: result_when_ready(client, params), 18)
        except AssertionError as error:
            raise AssertionError((version, client.request("metrics.queue.current", params))) from error
        for key, state, value in tracked:
            assert (result[key]["state"], result[key]["value"]) == (state, value), (version, key, result)
        assert db_rows(fixture) == before
        sql = (fixture / "sql.log").read_text().splitlines()
        assert sql and len(sql) % 3 == 0 and all("select count(*)" in line.lower() and "reserved_at" in line and "available_at" in line for line in sql), sql
        request = {"project_id": params["id"], "from": int(time.time()) - 120, "limit": 1}
        first = client.request("metrics.queue.history", request)
        second = client.request("metrics.queue.history", {**request, "cursor": first["next_cursor"]})
        assert first["has_more"] and first["items"][0]["sequence"] != second["items"][0]["sequence"]
        print(json.dumps({"version": version, "sqlite_queries": len(sql), "sqlite_contents_unchanged": True,
                          "history_cursor_progressed": True, "redis_counts": "ready=1, other=1, empty=0"}), flush=True)
    assert queue_contents() == before_redis

    # Point only one fixture's Redis client at an actual backend socket that
    # accepts connections but never responds to the client's authentication.
    silent_path = root / "silent.sock"
    with socket.socket(socket.AF_UNIX) as silent:
        silent.bind(str(silent_path))
        silent.listen()
        _, hung, params, _ = projects[0]
        (hung / "record-admissions").touch()
        (hung / "redis-socket").write_text(str(silent_path))
        start = time.monotonic()
        checks = []
        while time.monotonic() - start < 17:
            then = time.monotonic()
            client.request("daemon.status")
            assert running()
            checks.append((time.monotonic() - then) * 1000)
            result = inspect(client, params)
            if result[("redis", None)].get("reason") == "timeout":
                assert result[("redis", None)]["state"] == "stale"
                assert result[("redis", None)]["last_known"]["value"] == 1
                assert inspect(client, projects[1][2])[("redis", None)]["state"] == "fresh"
                admitted = float((hung / "admissions.log").read_text().splitlines()[-1])
                print(json.dumps({"timeout_isolated": True, "status_and_supervision_max_ms": max(checks),
                                  "php_entry_to_timeout_visible_ms": (time.time() - admitted) * 1000,
                                  "time_to_timeout_ms_including_next_round": (time.monotonic()-start)*1000}), flush=True)
                break
            time.sleep(.15)
        else:
            raise AssertionError("hung Laravel Redis connection did not time out")
        default_history = {"project_id": params["id"], "connection": "redis", "queue": "primary",
                           "from": int(time.time()) - 120}
        latest = client.request("metrics.queue.history", default_history)["items"][-1]
        assert latest["pending"]["reason"] == "timeout" and latest["pending"]["value"] is None
        (hung / "redis-socket").write_text(str(redis_socket))
        ready(lambda: result_when_ready(client, params), 16)
        assert client.request("metrics.queue.history", default_history)["items"][-1]["pending"]["value"] == 1
        contention(root, client, hung, params, projects[1][1], launch, running)
        client.request("metrics.queue.configure", {**params, "enabled": False})
        assert not client.request("metrics.queue.current", params)["enabled"]
        client.request("metrics.queue.configure", {**projects[1][2], "enabled": False})
        with sqlite3.connect(root / "state/roost.sqlite3") as db:
            assert db.execute("SELECT count(*) FROM queue_metric_raw").fetchone()[0] > 0
        if args.saturation:
            saturation(args, root, client, silent_path, running)


def contention(root, client, fixture, params, other_fixture, launch, running):
    # Block telemetry COMMIT with a genuine rollback-journal reader. Reads and
    # watcher invalidation still run, and opt-out must report failure honestly.
    state = root / "state"
    writer_result = []
    started_mutation = threading.Event()
    optout_key = str(uuid.uuid4())
    admission_log = fixture / "admissions.log"
    before = admission_log.read_text().splitlines()
    log = root / "daemon.log"
    warning = "queue metric batch could not commit"
    warning_count = log.read_text().count(warning)
    for app_dir in (fixture, other_fixture):
        (app_dir / "hold-probe").touch()

    def probe_entered():
        client.request("daemon.status")
        return len(admission_log.read_text().splitlines()) > len(before)

    ready(probe_entered, 14)
    watch_start = time.monotonic()

    def scheduler_mutation():
        wire = Client(state)
        try:
            started_mutation.set()
            start = time.monotonic()
            ack = wire.request("scheduler.restart", launch)
            writer_result.append((time.monotonic() - start, ack))
        finally:
            wire.close()

    with sqlite3.connect(state / "roost.sqlite3") as blocker:
        blocker.execute("BEGIN")
        blocker.execute("SELECT count(*) FROM queue_metric_raw").fetchone()
        (fixture / "queue-default.php").write_text("<?php return 'reconfigured';\n")

        def cleared():
            pending = inspect(client, params)[("redis", None)]
            return pending.get("reason") == "awaiting_probe" and "last_known" not in pending

        ready(cleared, 4)
        invalidation_ms = (time.monotonic() - watch_start) * 1000
        # The other project still has a valid publication ticket and must hit
        # the actual no-wait SQLite COMMIT path while this reader is held.
        def write_blocked():
            client.request("daemon.status")
            return log.read_text().count(warning) > warning_count

        ready(write_blocked, 4)
        start = time.monotonic()
        assert cleared()
        current_ms = (time.monotonic() - start) * 1000
        history = {"project_id": params["id"], "connection": "redis", "queue": "reconfigured",
                   "from": int(time.time()) - 120}
        assert client.request("metrics.queue.history", history)["items"] == []
        start = time.monotonic()
        try:
            client.request("metrics.queue.configure", {**params, "enabled": False,
                           "idempotency_key": optout_key})
        except RuntimeError as error:
            assert "database" in str(error) or "-32603" in str(error), error
        else:
            raise AssertionError("opt-out cannot report a commit while SQLite blocks it")
        optout_busy_ms = (time.monotonic() - start) * 1000
        assert client.request("metrics.queue.current", params)["enabled"]
        start = time.monotonic()
        assert running()
        lifecycle_read_ms = (time.monotonic() - start) * 1000

        worker = threading.Thread(target=scheduler_mutation)
        worker.start()
        try:
            assert started_mutation.wait(2)
            time.sleep(.06)
            assert not writer_result, "lifecycle write committed before the reader released"
        finally:
            blocker.rollback()
            worker.join(timeout=5)
        assert writer_result and writer_result[0][1]["result"]["desired_state"] == "running"

    for app_dir in (fixture, other_fixture):
        (app_dir / "hold-probe").unlink()
    ready(lambda: result_when_ready(client, params), 16)
    default = next(row for row in client.request("metrics.queue.current", params)["queues"]
                   if row["connection"] == "redis" and row["requested_queue"] is None)
    assert default["queue"] == "reconfigured" and default["pending"]["value"] == 0
    assert client.request("metrics.queue.history", history)["items"][-1]["pending"]["value"] == 0
    ack = client.request("metrics.queue.configure", {**params, "enabled": False,
                         "idempotency_key": optout_key})
    assert not ack["result"]["enabled"] and not client.request("metrics.queue.current", params)["enabled"]
    print(json.dumps({"watch_invalidation_ms": invalidation_ms, "queue_current_under_lock_ms": current_ms,
        "optout_busy_ms": optout_busy_ms, "supervision_reads_under_lock_ms": lifecycle_read_ms,
        "scheduler_restart_across_reader_release_ms": writer_result[0][0]*1000,
        "old_batch_discarded": True, "optout_retry_committed": True, "new_default_attributed": True}), flush=True)


def saturation(args, root, client, silent_path, running):
    fixtures = {}
    for i in range(9):
        fixture = root / f"sat-{i}"
        app(fixture, args.laravel13.resolve() / "vendor", silent_path)
        (fixture / "record-admissions").touch()
        selectors = [f"q-{j:02}" for j in range(65)] if i == 0 else ["other"]
        params = register(client, fixture, [("saturation", "redis", selectors)])
        fixtures[params["id"]] = fixture
        client.request("metrics.queue.configure", {**params, "enabled": True})

    def admissions():
        found = {}
        for project, fixture in fixtures.items():
            path = fixture / "admissions.log"
            if path.exists():
                found[project] = [float(stamp) for stamp in path.read_text().splitlines()]
        return found

    target = next(iter(fixtures))
    started = time.monotonic()
    maximum = 0
    while time.monotonic() - started < 52:
        tick = time.monotonic()
        assert running()
        maximum = max(maximum, (time.monotonic() - tick) * 1000)
        visits = admissions()
        if all(visits.get(project) for project in fixtures) and len(visits[target]) >= 3:
            history = client.request("metrics.queue.history", {"project_id": target,
                "from": int(time.time()) - 120, "limit": 256})
            seen = {row["queue"] for row in history["items"] if row["pending"]["reason"] == "timeout"}
            if len(seen) == 65:
                break
        time.sleep(.15)
    else:
        raise AssertionError("saturated selector rotation did not finish")
    first = sorted((visits[project][0], project) for project in fixtures)
    spread = first[-1][0] - first[0][0]
    assert spread < 20
    for entries in visits.values():
        assert all(b - a > 3 for a, b in zip(entries, entries[1:]))
    begin = first[0][0]
    print(json.dumps({"saturated_projects": len(first), "first_admission_spread_s": spread,
        "first_admission_offsets_s": [round(stamp-begin, 3) for stamp, _ in first],
        "selectors_rotated": len(seen), "max_supervision_status_ms": maximum}), flush=True)


def result_when_ready(client, params):
    result = inspect(client, params)
    return result if result.get(("redis", None), {}).get("state") == "fresh" else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--laravel12", required=True, type=Path)
    parser.add_argument("--laravel13", required=True, type=Path)
    parser.add_argument("--daemon", default=ROOT / "target/debug/roost-daemon", type=Path)
    parser.add_argument("--saturation", action="store_true")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="roost-q-", dir="/private/tmp") as path:
        root = Path(path)
        sock = root / "redis.sock"

        def redis(*command):
            return subprocess.check_output(["redis-cli", "-s", str(sock), "--raw", *command], text=True).strip()

        with (root / "redis.log").open("w+") as log, (root / "daemon.log").open("w+") as daemon_log:
            server = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", str(sock),
                "--unixsocketperm", "700", "--save", "", "--appendonly", "no"], cwd=root, stdout=log, stderr=log)
            monitor = daemon = client = None
            try:
                ready(lambda: sock.exists())
                redis("rpush", "queues:primary", "ready")
                redis("rpush", "queues:other", "other")
                redis("zadd", "queues:primary:delayed", "1", "due", "9999999999", "future")
                redis("zadd", "queues:primary:reserved", "1", "reserved")
                assert redis("acl", "setuser", "roost_probe", "on", ">fixture-password", "~queues:*",
                             "-@all", "+llen", "+select") == "OK"
                with (root / "commands.log").open("w+") as observed:
                    monitor = subprocess.Popen(["redis-cli", "-s", str(sock), "--raw", "MONITOR"], stdout=observed)
                    state = root / "state"
                    daemon = subprocess.Popen([str(args.daemon.resolve()), "--state-dir", str(state)], stdout=daemon_log, stderr=daemon_log)
                    client = ready(lambda: connect(state))
                    launch, running = start_supervision(root, client)
                    matrix(args, root, client, redis, sock, launch, running)
                    observed.seek(0)
                    commands = observed.read()
                    assert commands.lower().count('"llen"') >= 6, commands
                    observed_commands = {command.lower() for command in re.findall(r'\] "([A-Za-z]+)"', commands)}
                    # LRANGE/ZRANGE are fixture snapshots. AUTH and SELECT
                    # establish the read-only Redis session.
                    assert observed_commands <= {"auth", "select", "llen", "lrange", "zrange"}, observed_commands
                    print(json.dumps({"redis_llens": commands.lower().count('"llen"'),
                                      "redis_queue_contents_unchanged": True, "observed_commands": sorted(observed_commands)}), flush=True)
            except BaseException:
                daemon_log.seek(0)
                print(daemon_log.read(), end="")
                raise
            finally:
                if client:
                    client.close()
                for child in (daemon, monitor, server):
                    if child and child.poll() is None:
                        child.terminate()
                        child.wait(timeout=30)


if __name__ == "__main__":
    main()
