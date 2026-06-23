#!/usr/bin/env python3
import argparse
import gc
import json
import mmap
import multiprocessing as mp
import os
import statistics
import sys
import time
from pathlib import Path


EEXIST = -17
O_DIRECT = getattr(os, "O_DIRECT", 0)


def percentile(values, q):
    if not values:
        return 0.0
    values = sorted(values)
    idx = int(len(values) * q)
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def latency_stats(values):
    return {
        "avg_sec": statistics.fmean(values) if values else 0.0,
        "p50_sec": percentile(values, 0.50),
        "p95_sec": percentile(values, 0.95),
        "p99_sec": percentile(values, 0.99),
        "max_sec": max(values) if values else 0.0,
    }


def make_client(args, role, client_id):
    sys.path.insert(0, args.python_interface)
    import pyfalconfs

    workspace = Path(args.workspace) / f"{role}_{client_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    return pyfalconfs.Client(str(workspace), args.config)


def mkdir(client, path):
    ret = client.Mkdir(path)
    if ret not in (0, EEXIST):
        raise RuntimeError(f"Mkdir {path} failed: {ret}")


def describe_path_state(client, path):
    try:
        stat_result = client.Stat(path)
        return f"stat_ret={stat_result.ret}, stat={stat_result.stat_dict}"
    except Exception as exc:
        return f"stat_error={exc}"


def create_write_close(client, path, payload, oflags=None, size=None, offset=0):
    flags = os.O_CREAT | os.O_RDWR if oflags is None else oflags
    write_size = len(payload) if size is None else size
    ret, fd = client.Create(path, flags)
    if ret != 0:
        raise RuntimeError(f"Create {path} failed: {ret}, fd={fd}, {describe_path_state(client, path)}")
    try:
        ret = client.Write(path, fd, payload, write_size, offset)
        if ret != 0:
            raise RuntimeError(f"Write {path} failed: {ret}, fd={fd}, {describe_path_state(client, path)}")
        ret = client.Flush(path, fd)
        if ret != 0:
            raise RuntimeError(f"Flush {path} failed: {ret}, fd={fd}, {describe_path_state(client, path)}")
    finally:
        close_ret = client.Close(path, fd)
        if close_ret != 0:
            raise RuntimeError(f"Close {path} failed: {close_ret}, fd={fd}, {describe_path_state(client, path)}")


def open_read_close(client, path, size):
    ret, fd = client.Open(path, os.O_RDONLY)
    if ret != 0:
        raise RuntimeError(f"Open {path} failed: {ret}, fd={fd}, {describe_path_state(client, path)}")
    primary_error = None
    try:
        buf = bytearray(size)
        ret = client.Read(path, fd, buf, size, 0)
        if ret != size:
            raise RuntimeError(f"Read {path} failed: ret={ret}, expected={size}, fd={fd}, {describe_path_state(client, path)}")
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        close_ret = client.Close(path, fd)
        if close_ret != 0:
            close_error = f"Close {path} failed: {close_ret}, fd={fd}, {describe_path_state(client, path)}"
            if primary_error is not None:
                raise RuntimeError(f"{primary_error}; additionally {close_error}") from primary_error
            raise RuntimeError(close_error)


def worker_write(args, role, client_id, base_dir, files, start_event, ready_q, result_q):
    try:
        client = make_client(args, role, client_id)
        mkdir(client, base_dir)
        payload = bytearray(b"a" * args.file_size)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        latencies = []
        for i in range(files):
            path = f"{base_dir}/f_{i:08d}"
            op_begin = time.monotonic()
            create_write_close(client, path, payload)
            latencies.append(time.monotonic() - op_begin)
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "error": "",
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "error": str(exc),
        })


def worker_read(args, role, client_id, base_dir, files, start_event, ready_q, result_q):
    try:
        client = make_client(args, role, client_id)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        latencies = []
        for i in range(files):
            path = f"{base_dir}/f_{i:08d}"
            op_begin = time.monotonic()
            open_read_close(client, path, args.file_size)
            latencies.append(time.monotonic() - op_begin)
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "error": "",
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "error": str(exc),
        })


def worker_write_timed(args, role, client_id, base_dir, max_files, start_event, stop_event, ready_q, result_q):
    begin = None
    latencies = []
    i = 0
    path = ""
    try:
        client = make_client(args, role, client_id)
        mkdir(client, base_dir)
        payload = bytearray(b"a" * args.file_size)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        deadline = begin + args.duration_sec
        while not stop_event.is_set() and time.monotonic() < deadline and (max_files <= 0 or i < max_files):
            path = f"{base_dir}/f_{i:08d}"
            op_begin = time.monotonic()
            create_write_close(client, path, payload)
            latencies.append(time.monotonic() - op_begin)
            i += 1
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "stop_reason": "max_files" if max_files > 0 and i >= max_files else "duration",
            "error": "",
            "last_path": path,
            "next_index": i,
        })
    except Exception as exc:
        elapsed = time.monotonic() - begin if begin is not None else 0.0
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "stop_reason": "error",
            "error": str(exc),
            "failed_path": path,
            "next_index": i,
            "completed_files_before_error": len(latencies),
        })


def worker_read_timed(args, role, client_id, base_dir, files, start_event, stop_event, ready_q, result_q):
    try:
        if files <= 0:
            raise RuntimeError("timed read requires at least one prepared file per reader")
        client = make_client(args, role, client_id)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        deadline = begin + args.duration_sec
        latencies = []
        i = 0
        while not stop_event.is_set() and time.monotonic() < deadline:
            path = f"{base_dir}/f_{i % files:08d}"
            op_begin = time.monotonic()
            open_read_close(client, path, args.file_size)
            latencies.append(time.monotonic() - op_begin)
            i += 1
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "stop_reason": "duration",
            "error": "",
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "stop_reason": "error",
            "error": str(exc),
        })




def worker_read_pinned_timed(args, role, client_id, base_dir, files, start_index, counters, start_event, stop_event, ready_q, result_q):
    client = None
    opened = []
    try:
        if files <= 0:
            raise RuntimeError("pinned timed read requires at least one prepared file per reader")
        client = make_client(args, role, client_id)
        for i in range(files):
            path = f"{base_dir}/f_{start_index + i:08d}"
            ret, fd = client.Open(path, os.O_RDONLY)
            if ret != 0:
                raise RuntimeError(f"Open pinned read file {path} failed: {ret}, fd={fd}, {describe_path_state(client, path)}")
            buf = bytearray(args.file_size)
            ret = client.Read(path, fd, buf, args.file_size, 0)
            if ret != args.file_size:
                raise RuntimeError(f"Warmup read pinned file {path} failed: ret={ret}, expected={args.file_size}, fd={fd}, {describe_path_state(client, path)}")
            opened.append((path, fd))
        ready_q.put({"role": role, "client_id": client_id, "ready": True, "pinned_files": len(opened)})
        start_event.wait()
        begin = time.monotonic()
        deadline = begin + args.duration_sec
        latencies = []
        operation_errors = []
        i = 0
        while not stop_event.is_set() and time.monotonic() < deadline:
            path, fd = opened[i % len(opened)]
            buf = bytearray(args.file_size)
            op_begin = time.monotonic()
            ret = client.Read(path, fd, buf, args.file_size, 0)
            if ret == args.file_size:
                latencies.append(time.monotonic() - op_begin)
            else:
                if len(operation_errors) < 20:
                    operation_errors.append(f"Read pinned {path} failed: ret={ret}, expected={args.file_size}, fd={fd}, {describe_path_state(client, path)}")
                time.sleep(args.hot_read_retry_sleep_sec)
            i += 1
        elapsed = time.monotonic() - begin
        close_errors = []
        for path, fd in opened:
            close_ret = client.Close(path, fd)
            if close_ret != 0 and len(close_errors) < 20:
                close_errors.append(f"Close pinned {path} failed: {close_ret}, fd={fd}, {describe_path_state(client, path)}")
        if close_errors:
            operation_errors.extend(close_errors[:max(0, 20 - len(operation_errors))])
        fatal_error = ""
        if not latencies and operation_errors:
            fatal_error = f"pinned reader completed 0 reads; first_error={operation_errors[0]}"
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "operation_error_count": max(0, i - len(latencies)) + len(close_errors),
            "operation_errors": operation_errors,
            "pinned_files": len(opened),
            "stop_reason": "duration" if not fatal_error else "error",
            "error": fatal_error,
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "operation_error_count": 0,
            "operation_errors": [],
            "pinned_files": len(opened),
            "stop_reason": "error",
            "error": str(exc),
        })


def worker_write_timed_publish(args, role, client_id, base_dir, max_files, counters, start_event, stop_event, ready_q, result_q):
    begin = None
    latencies = []
    i = 0
    path = ""
    try:
        client = make_client(args, role, client_id)
        mkdir(client, base_dir)
        payload = bytearray(b"a" * args.file_size)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        deadline = begin + args.duration_sec
        while not stop_event.is_set() and time.monotonic() < deadline and (max_files <= 0 or i < max_files):
            path = f"{base_dir}/f_{i:08d}"
            op_begin = time.monotonic()
            create_write_close(client, path, payload)
            latencies.append(time.monotonic() - op_begin)
            i += 1
            counters[client_id] = i
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "operation_error_count": 0,
            "operation_errors": [],
            "stop_reason": "max_files" if max_files > 0 and i >= max_files else "duration",
            "error": "",
            "last_path": path,
            "next_index": i,
        })
    except Exception as exc:
        elapsed = time.monotonic() - begin if begin is not None else 0.0
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "operation_error_count": 0,
            "operation_errors": [],
            "stop_reason": "error",
            "error": str(exc),
            "failed_path": path,
            "next_index": i,
            "completed_files_before_error": len(latencies),
        })


def worker_read_hot_timed(args, role, client_id, writer_prefix, writer_clients, counters, start_event, stop_event, ready_q, result_q):
    try:
        client = make_client(args, role, client_id)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        deadline = begin + args.duration_sec
        latencies = []
        operation_errors = []
        i = 0
        while not stop_event.is_set() and time.monotonic() < deadline:
            writer_id = (client_id + i) % writer_clients
            count = counters[writer_id]
            readable_count = count - max(args.hot_read_lag, 0)
            if readable_count < max(args.hot_read_min_files, 1):
                time.sleep(args.hot_read_retry_sleep_sec)
                continue
            window = min(readable_count, max(args.hot_read_window, 1))
            index = readable_count - 1 - (i % window)
            path = f"{args.dir}_{writer_prefix}_{writer_id}/f_{index:08d}"
            op_begin = time.monotonic()
            try:
                open_read_close(client, path, args.file_size)
                latencies.append(time.monotonic() - op_begin)
                i += 1
            except Exception as exc:
                if len(operation_errors) < 20:
                    operation_errors.append(str(exc))
                i += 1
                time.sleep(args.hot_read_retry_sleep_sec)
        elapsed = time.monotonic() - begin
        fatal_error = ""
        if not latencies and operation_errors:
            fatal_error = f"hot reader completed 0 reads; first_error={operation_errors[0]}"
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "operation_error_count": max(0, i - len(latencies)),
            "operation_errors": operation_errors,
            "hot_read_lag": args.hot_read_lag,
            "hot_read_min_files": args.hot_read_min_files,
            "stop_reason": "duration" if not fatal_error else "error",
            "error": fatal_error,
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "operation_error_count": 0,
            "operation_errors": [],
            "stop_reason": "error",
            "error": str(exc),
        })

def worker_unlink_timed(args, role, client_id, base_dir, files, start_event, stop_event, ready_q, result_q):
    try:
        client = make_client(args, role, client_id)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        deadline = begin + args.duration_sec
        latencies = []
        i = 0
        while not stop_event.is_set() and time.monotonic() < deadline and i < files:
            path = f"{base_dir}/f_{i:08d}"
            op_begin = time.monotonic()
            ret = client.Unlink(path)
            if ret != 0:
                raise RuntimeError(f"Unlink {path} failed: {ret}")
            latencies.append(time.monotonic() - op_begin)
            i += 1
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "stop_reason": "max_files" if i >= files else "duration",
            "error": "",
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "stop_reason": "error",
            "error": str(exc),
        })

def worker_unlink(args, role, client_id, base_dir, files, start_event, ready_q, result_q):
    try:
        client = make_client(args, role, client_id)
        ready_q.put({"role": role, "client_id": client_id, "ready": True})
        start_event.wait()
        begin = time.monotonic()
        latencies = []
        for i in range(files):
            path = f"{base_dir}/f_{i:08d}"
            op_begin = time.monotonic()
            ret = client.Unlink(path)
            if ret != 0:
                raise RuntimeError(f"Unlink {path} failed: {ret}")
            latencies.append(time.monotonic() - op_begin)
        elapsed = time.monotonic() - begin
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": len(latencies),
            "elapsed_sec": elapsed,
            "latencies": latencies,
            "error": "",
        })
    except Exception as exc:
        ready_q.put({"role": role, "client_id": client_id, "ready": False, "error": str(exc)})
        result_q.put({
            "role": role,
            "client_id": client_id,
            "files": 0,
            "elapsed_sec": 0.0,
            "latencies": [],
            "error": str(exc),
        })


def run_processes(jobs):
    start_event = mp.Event()
    ready_q = mp.Queue()
    result_q = mp.Queue()
    procs = []
    for target, params in jobs:
        proc = mp.Process(target=target, args=(*params, start_event, ready_q, result_q))
        proc.start()
        procs.append(proc)

    for _ in procs:
        ready_q.get()

    begin = time.monotonic()
    start_event.set()
    results = [result_q.get() for _ in procs]
    elapsed = time.monotonic() - begin

    for proc in procs:
        proc.join()
    return elapsed, results


def run_processes_timed(jobs, duration_sec):
    start_event = mp.Event()
    stop_event = mp.Event()
    ready_q = mp.Queue()
    result_q = mp.Queue()
    procs = []
    for target, params in jobs:
        proc = mp.Process(target=target, args=(*params, start_event, stop_event, ready_q, result_q))
        proc.start()
        procs.append(proc)

    for _ in procs:
        ready_q.get()

    begin = time.monotonic()
    start_event.set()
    time.sleep(duration_sec)
    stop_event.set()
    results = [result_q.get() for _ in procs]
    elapsed = time.monotonic() - begin

    for proc in procs:
        proc.join()
    return elapsed, results




def run_processes_timed_hot(read_jobs, write_jobs, duration_sec):
    start_event = mp.Event()
    stop_event = mp.Event()
    ready_q = mp.Queue()
    result_q = mp.Queue()
    counters = mp.Array('q', len(write_jobs))
    procs = []
    for target, params in write_jobs:
        proc = mp.Process(target=target, args=(*params, counters, start_event, stop_event, ready_q, result_q))
        proc.start()
        procs.append(proc)
    for target, params in read_jobs:
        proc = mp.Process(target=target, args=(*params, counters, start_event, stop_event, ready_q, result_q))
        proc.start()
        procs.append(proc)

    for _ in procs:
        ready_q.get()

    begin = time.monotonic()
    start_event.set()
    time.sleep(duration_sec)
    stop_event.set()
    results = [result_q.get() for _ in procs]
    elapsed = time.monotonic() - begin

    for proc in procs:
        proc.join()
    return elapsed, results

def summarize_group(results, elapsed, file_size):
    latencies = []
    files = 0
    errors = []
    operation_error_count = 0
    operation_errors = []
    max_worker_elapsed = 0.0
    for result in results:
        latencies.extend(result["latencies"])
        files += result["files"]
        max_worker_elapsed = max(max_worker_elapsed, result["elapsed_sec"])
        operation_error_count += result.get("operation_error_count", 0)
        operation_errors.extend(result.get("operation_errors", [])[:10])
        if result["error"]:
            errors.append(result)
    stats = latency_stats(latencies)
    return {
        "files": files,
        "elapsed_sec": elapsed,
        "max_worker_elapsed_sec": max_worker_elapsed,
        "files_per_sec": files / elapsed if elapsed > 0 else 0.0,
        "mib_per_sec": files * file_size / elapsed / 1048576.0 if elapsed > 0 else 0.0,
        "latency_avg_sec": stats["avg_sec"],
        "latency_p50_sec": stats["p50_sec"],
        "latency_p95_sec": stats["p95_sec"],
        "latency_p99_sec": stats["p99_sec"],
        "latency_max_sec": stats["max_sec"],
        "error_count": len(errors),
        "errors": errors,
        "operation_error_count": operation_error_count,
        "operation_errors": operation_errors[:20],
        "per_client": [
            {
                "role": r["role"],
                "client_id": r["client_id"],
                "files": r["files"],
                "elapsed_sec": r["elapsed_sec"],
                "stop_reason": r.get("stop_reason", ""),
                "operation_error_count": r.get("operation_error_count", 0),
                "error": r["error"],
            }
            for r in sorted(results, key=lambda x: (x["role"], x["client_id"]))
        ],
    }


def build_jobs(args, target, role, base_prefix, total_files, clients):
    per_client = (total_files + clients - 1) // clients
    jobs = []
    remaining = total_files
    for client_id in range(clients):
        files = min(per_client, remaining)
        remaining -= files
        base_dir = f"{args.dir}_{base_prefix}_{client_id}"
        jobs.append((target, (args, role, client_id, base_dir, files)))
    return jobs


def build_write_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_write, role, base_prefix, total_files, clients)


def build_read_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_read, role, base_prefix, total_files, clients)


def build_unlink_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_unlink, role, base_prefix, total_files, clients)


def build_timed_write_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_write_timed, role, base_prefix, total_files, clients)


def build_timed_read_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_read_timed, role, base_prefix, total_files, clients)


def build_timed_unlink_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_unlink_timed, role, base_prefix, total_files, clients)


def build_hot_write_jobs(args, role, base_prefix, total_files, clients):
    return build_jobs(args, worker_write_timed_publish, role, base_prefix, total_files, clients)


def build_hot_read_jobs(args, role, writer_prefix, clients):
    return [(worker_read_hot_timed, (args, role, client_id, writer_prefix, clients)) for client_id in range(clients)]


def build_shared_pinned_prepare_jobs(args, role, base_prefix, total_files):
    shared_dir = f"{args.dir}_{base_prefix}_shared"
    return [(worker_write, (args, role, 0, shared_dir, total_files))]


def build_pinned_read_jobs(args, role, base_prefix, files_per_client, clients):
    if args.shared_pinned_read_dir:
        shared_dir = f"{args.dir}_{base_prefix}_shared"
        return [
            (worker_read_pinned_timed, (args, role, client_id, shared_dir, files_per_client, client_id * files_per_client))
            for client_id in range(clients)
        ]
    return [
        (worker_read_pinned_timed, (args, role, client_id, f"{args.dir}_{base_prefix}_{client_id}", files_per_client, 0))
        for client_id in range(clients)
    ]


def run_write_only(args):
    elapsed, results = run_processes(build_write_jobs(args, "writer", "write", args.files, args.clients))
    summary = summarize_group(results, elapsed, args.file_size)
    return {"mode": args.mode, "writer_clients": args.clients, "written_files": summary.pop("files"), **summary}


def run_read_only(args):
    prepare_elapsed, prepare_results = run_processes(
        build_write_jobs(args, "prepare_read", "read", args.read_files, args.clients)
    )
    read_elapsed, read_results = run_processes(
        build_read_jobs(args, "reader", "read", args.read_files, args.clients)
    )
    prepare = summarize_group(prepare_results, prepare_elapsed, args.file_size)
    read = summarize_group(read_results, read_elapsed, args.file_size)
    return {
        "mode": args.mode,
        "clients": args.clients,
        "prepared_read_files": prepare.pop("files"),
        "read_files": read.pop("files"),
        "prepare": prepare,
        "read": read,
        "error_count": prepare["error_count"] + read["error_count"],
    }


def run_unlink_only(args):
    prepare_elapsed, prepare_results = run_processes(
        build_write_jobs(args, "prepare", "delete", args.unlink_files, args.clients)
    )
    unlink_elapsed, unlink_results = run_processes(
        build_unlink_jobs(args, "deleter", "delete", args.unlink_files, args.clients)
    )
    prepare = summarize_group(prepare_results, prepare_elapsed, args.file_size)
    unlink = summarize_group(unlink_results, unlink_elapsed, args.file_size)
    return {
        "mode": args.mode,
        "clients": args.clients,
        "created_files": prepare.pop("files"),
        "unlinked_files": unlink.pop("files"),
        "create": prepare,
        "unlink": unlink,
        "error_count": prepare["error_count"] + unlink["error_count"],
    }


def run_create_evict(args):
    result = run_write_only(args)
    if args.wait_sec > 0:
        time.sleep(args.wait_sec)
    result["wait_sec"] = args.wait_sec
    return result


def run_concurrent_unlink(args):
    prepare_elapsed, prepare_results = run_processes(
        build_write_jobs(args, "prepare", "delete", args.unlink_files, args.clients)
    )
    write_jobs = build_timed_write_jobs(args, "writer", "write", args.files, args.clients)
    delete_jobs = build_timed_unlink_jobs(args, "deleter", "delete", args.unlink_files, args.clients)
    mixed_elapsed, mixed_results = run_processes_timed(write_jobs + delete_jobs, args.duration_sec)
    writer_results = [r for r in mixed_results if r["role"] == "writer"]
    deleter_results = [r for r in mixed_results if r["role"] == "deleter"]
    writer = summarize_group(writer_results, mixed_elapsed, args.file_size)
    deleter = summarize_group(deleter_results, mixed_elapsed, args.file_size)
    prepare = summarize_group(prepare_results, prepare_elapsed, args.file_size)
    delete_dataset_exhausted = any((r.get("stop_reason") == "max_files") for r in deleter_results)
    return {
        "mode": args.mode,
        "timed": True,
        "duration_sec": args.duration_sec,
        "writer_clients": args.clients,
        "deleter_clients": args.clients,
        "total_processes": args.clients * 2,
        "requested_write_files": args.files,
        "requested_delete_files": args.unlink_files,
        "prepared_delete_files": prepare.pop("files"),
        "written_files": writer.pop("files"),
        "unlinked_files": deleter.pop("files"),
        "delete_dataset_exhausted": delete_dataset_exhausted,
        "prepare": prepare,
        "mixed_elapsed_sec": mixed_elapsed,
        "writer": writer,
        "deleter": deleter,
        "error_count": prepare["error_count"] + writer["error_count"] + deleter["error_count"],
    }


def run_read_write(args):
    timed = args.duration_sec > 0
    if timed:
        pinned_total_files = args.pinned_read_files * args.clients
        prepare_clients = 1 if args.shared_pinned_read_dir else args.clients
        prepare_jobs = (
            build_shared_pinned_prepare_jobs(args, "prepare_read", "pinned_read", pinned_total_files)
            if args.shared_pinned_read_dir
            else build_write_jobs(args, "prepare_read", "pinned_read", pinned_total_files, prepare_clients)
        )
        prepare_elapsed, prepare_results = run_processes(prepare_jobs)
        prepare = summarize_group(prepare_results, prepare_elapsed, args.file_size)
        read_jobs = build_pinned_read_jobs(args, "reader", "pinned_read", args.pinned_read_files, args.clients)
        write_jobs = build_hot_write_jobs(args, "writer", "write", args.files, args.clients)
        mixed_elapsed, mixed_results = run_processes_timed_hot(read_jobs, write_jobs, args.duration_sec)
        read_pattern = "shared_pinned_read_set" if args.shared_pinned_read_dir else "pinned_read_set"
    else:
        prepare_elapsed, prepare_results = run_processes(
            build_write_jobs(args, "prepare_read", "read", args.read_files, args.clients)
        )
        read_jobs = build_read_jobs(args, "reader", "read", args.read_files, args.clients)
        write_jobs = build_write_jobs(args, "writer", "write", args.files, args.clients)
        mixed_elapsed, mixed_results = run_processes(read_jobs + write_jobs)
        prepare = summarize_group(prepare_results, prepare_elapsed, args.file_size)
        read_pattern = "prepared_read_dataset"
    reader_results = [r for r in mixed_results if r["role"] == "reader"]
    writer_results = [r for r in mixed_results if r["role"] == "writer"]
    reader = summarize_group(reader_results, mixed_elapsed, args.file_size)
    writer = summarize_group(writer_results, mixed_elapsed, args.file_size)
    result = {
        "mode": args.mode,
        "timed": timed,
        "duration_sec": args.duration_sec,
        "read_pattern": read_pattern,
        "hot_read_window": args.hot_read_window if timed else 0,
        "hot_read_lag": args.hot_read_lag if timed else 0,
        "hot_read_min_files": args.hot_read_min_files if timed else 0,
        "pinned_read_files_per_client": args.pinned_read_files if timed else 0,
        "shared_pinned_read_dir": bool(args.shared_pinned_read_dir) if timed else False,
        "reader_clients": args.clients,
        "writer_clients": args.clients,
        "total_processes": args.clients * 2,
        "prepared_read_files": prepare.pop("files"),
        "read_files": reader.pop("files"),
        "written_files": writer.pop("files"),
        "prepare": prepare,
        "mixed_elapsed_sec": mixed_elapsed,
        "reader": reader,
        "writer": writer,
        "error_count": prepare["error_count"] + reader["error_count"] + writer["error_count"],
    }
    if args.mode == "read_write_evict" and args.wait_sec > 0:
        time.sleep(args.wait_sec)
        result["wait_sec"] = args.wait_sec
    return result


def direct_buffer(size, offset):
    mm = mmap.mmap(-1, size + max(offset, 0) + 4096)
    mm[:] = b"d" * len(mm)
    if offset == 0:
        return mm, mm
    view = memoryview(mm)[offset:offset + size]
    return mm, view


def direct_probe_one(client, base_dir, name, size, write_offset, buffer_offset):
    path = f"{base_dir}/{name}"
    mm, buf = direct_buffer(max(size, 1), buffer_offset)
    flags = os.O_CREAT | os.O_RDWR | O_DIRECT
    item = {
        "name": name,
        "path": path,
        "size": size,
        "offset": write_offset,
        "buffer_offset": buffer_offset,
        "oflags": flags,
        "o_direct_available": bool(O_DIRECT),
        "create_ret": None,
        "fd": None,
        "write_ret": None,
        "flush_ret": None,
        "close_ret": None,
        "stat": None,
        "error": "",
        "cleanup_warning": "",
    }
    try:
        ret, fd = client.Create(path, flags)
        item["create_ret"] = ret
        item["fd"] = fd
        if ret == 0:
            item["write_ret"] = client.Write(path, fd, buf, size, write_offset)
            item["flush_ret"] = client.Flush(path, fd)
            item["close_ret"] = client.Close(path, fd)
            item["stat"] = describe_path_state(client, path)
    except Exception as exc:
        item["error"] = str(exc)
        try:
            if item["fd"] is not None:
                item["close_ret"] = client.Close(path, item["fd"])
        except Exception as close_exc:
            item["error"] += f"; close_error={close_exc}"
    finally:
        if isinstance(buf, memoryview):
            try:
                buf.release()
            except BufferError as exc:
                item["cleanup_warning"] = (item.get("cleanup_warning") or "") + f"; buffer_release_error={exc}"
        buf = None
        gc.collect()
        try:
            mm.close()
        except BufferError as exc:
            item["cleanup_warning"] = (item.get("cleanup_warning") or "") + f"; mmap_close_error={exc}"
    return item


def run_direct_unaligned(args):
    client = make_client(args, "direct", 0)
    base_dir = f"{args.dir}_direct"
    mkdir(client, base_dir)
    cases = [
        ("D-1-aligned", args.file_size, 0, 0),
        ("D-2-length-unaligned", max(args.file_size - 1, 1), 0, 0),
        ("D-3-offset-unaligned", args.file_size, 1, 0),
        ("D-4-buffer-unaligned", args.file_size, 0, 1),
    ]
    probes = [direct_probe_one(client, base_dir, *case) for case in cases]
    return {
        "mode": args.mode,
        "clients": 1,
        "probes": probes,
        "error_count": 0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=[
        "write_only",
        "read_only",
        "unlink_only",
        "create_evict",
        "concurrent_unlink",
        "read_write",
        "read_write_evict",
        "direct_unaligned",
    ], required=True)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--files", type=int, default=6000)
    parser.add_argument("--read-files", type=int, default=None)
    parser.add_argument("--unlink-files", type=int, default=6000)
    parser.add_argument("--file-size", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--wait-sec", type=int, default=45)
    parser.add_argument("--duration-sec", type=float, default=0.0,
                        help="Run read_write/read_write_evict for a fixed duration; 0 keeps fixed-file mode.")
    parser.add_argument("--hot-read-window", type=int, default=1024,
                        help="For timed read/write cases, readers loop over a completed writer-file window.")
    parser.add_argument("--hot-read-lag", type=int, default=128,
                        help="For timed read/write cases, readers skip this many newest completed files per writer.")
    parser.add_argument("--hot-read-min-files", type=int, default=0,
                        help="Readers wait until each writer has at least this many readable files after lag; 0 uses hot-read-window.")
    parser.add_argument("--pinned-read-files", type=int, default=256,
                        help="For timed read/write cases, each reader opens this many prepared files before writers start.")
    parser.add_argument("--shared-pinned-read-dir", action="store_true",
                        help="For timed read/write cases, all readers open the same prepared directory.")
    parser.add_argument("--hot-read-retry-sleep-sec", type=float, default=0.001,
                        help="Sleep between hot-read retries while waiting for enough writer files or after a read error.")
    parser.add_argument("--dir", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--config", default="/usr/local/falconfs/falcon_client/config/config.json")
    parser.add_argument("--python-interface", default="/home/hx/code/falconfs/python_interface")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.read_files is None:
        args.read_files = args.files
    if args.hot_read_min_files <= 0:
        args.hot_read_min_files = max(args.hot_read_window, 1)

    Path(args.workspace).mkdir(parents=True, exist_ok=True)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    if args.mode == "write_only":
        result = run_write_only(args)
    elif args.mode == "read_only":
        result = run_read_only(args)
    elif args.mode == "unlink_only":
        result = run_unlink_only(args)
    elif args.mode == "create_evict":
        result = run_create_evict(args)
    elif args.mode == "concurrent_unlink":
        result = run_concurrent_unlink(args)
    elif args.mode in ("read_write", "read_write_evict"):
        result = run_read_write(args)
    elif args.mode == "direct_unaligned":
        result = run_direct_unaligned(args)
    else:
        raise RuntimeError(f"unknown mode: {args.mode}")

    result.update({
        "clients": args.clients,
        "files": args.files,
        "read_files": args.read_files,
        "unlink_files": args.unlink_files,
        "file_size_bytes": args.file_size,
        "duration_sec": args.duration_sec,
        "dir": args.dir,
        "workspace": args.workspace,
    })
    with open(args.output, "w", encoding="utf-8") as out:
        json.dump(result, out, indent=2, sort_keys=True)
        out.write("\n")

    if result.get("error_count", 0) != 0:
        raise SystemExit(1)


if __name__ == "__main__":
    mp.set_start_method("spawn")
    main()
