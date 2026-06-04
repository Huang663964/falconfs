#!/usr/bin/env python3
import argparse
import json
import multiprocessing as mp
import os
import statistics
import sys
import time
from pathlib import Path


EEXIST = -17


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


def create_write_close(client, path, payload):
    ret, fd = client.Create(path, os.O_CREAT | os.O_RDWR)
    if ret != 0:
        raise RuntimeError(f"Create {path} failed: {ret}")
    ret = client.Write(path, fd, payload, len(payload), 0)
    if ret != 0:
        raise RuntimeError(f"Write {path} failed: {ret}")
    ret = client.Flush(path, fd)
    if ret != 0:
        raise RuntimeError(f"Flush {path} failed: {ret}")
    ret = client.Close(path, fd)
    if ret != 0:
        raise RuntimeError(f"Close {path} failed: {ret}")


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


def summarize_group(results, elapsed, file_size):
    latencies = []
    files = 0
    errors = []
    max_worker_elapsed = 0.0
    for result in results:
        latencies.extend(result["latencies"])
        files += result["files"]
        max_worker_elapsed = max(max_worker_elapsed, result["elapsed_sec"])
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
        "per_client": [
            {
                "role": r["role"],
                "client_id": r["client_id"],
                "files": r["files"],
                "elapsed_sec": r["elapsed_sec"],
                "error": r["error"],
            }
            for r in sorted(results, key=lambda x: (x["role"], x["client_id"]))
        ],
    }


def build_write_jobs(args, role, base_prefix, total_files, clients):
    per_client = (total_files + clients - 1) // clients
    jobs = []
    remaining = total_files
    for client_id in range(clients):
        files = min(per_client, remaining)
        remaining -= files
        base_dir = f"{args.dir}_{base_prefix}_{client_id}"
        jobs.append((worker_write, (args, role, client_id, base_dir, files)))
    return jobs


def build_unlink_jobs(args, role, base_prefix, total_files, clients):
    per_client = (total_files + clients - 1) // clients
    jobs = []
    remaining = total_files
    for client_id in range(clients):
        files = min(per_client, remaining)
        remaining -= files
        base_dir = f"{args.dir}_{base_prefix}_{client_id}"
        jobs.append((worker_unlink, (args, role, client_id, base_dir, files)))
    return jobs


def run_write_only(args):
    elapsed, results = run_processes(build_write_jobs(args, "writer", "write", args.files, args.clients))
    summary = summarize_group(results, elapsed, args.file_size)
    return {"mode": args.mode, "writer_clients": args.clients, "written_files": summary.pop("files"), **summary}


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
    write_jobs = build_write_jobs(args, "writer", "write", args.files, args.clients)
    delete_jobs = build_unlink_jobs(args, "deleter", "delete", args.unlink_files, args.clients)
    mixed_elapsed, mixed_results = run_processes(write_jobs + delete_jobs)
    writer_results = [r for r in mixed_results if r["role"] == "writer"]
    deleter_results = [r for r in mixed_results if r["role"] == "deleter"]
    writer = summarize_group(writer_results, mixed_elapsed, args.file_size)
    deleter = summarize_group(deleter_results, mixed_elapsed, args.file_size)
    prepare = summarize_group(prepare_results, prepare_elapsed, args.file_size)
    return {
        "mode": args.mode,
        "writer_clients": args.clients,
        "deleter_clients": args.clients,
        "total_processes": args.clients * 2,
        "prepared_delete_files": prepare.pop("files"),
        "written_files": writer.pop("files"),
        "unlinked_files": deleter.pop("files"),
        "prepare": prepare,
        "mixed_elapsed_sec": mixed_elapsed,
        "writer": writer,
        "deleter": deleter,
        "error_count": prepare["error_count"] + writer["error_count"] + deleter["error_count"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["write_only", "unlink_only", "create_evict", "concurrent_unlink"], required=True)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--files", type=int, default=6000)
    parser.add_argument("--unlink-files", type=int, default=6000)
    parser.add_argument("--file-size", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--wait-sec", type=int, default=45)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--config", default="/usr/local/falconfs/falcon_client/config/config.json")
    parser.add_argument("--python-interface", default="/home/hx/code/falconfs/python_interface")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    Path(args.workspace).mkdir(parents=True, exist_ok=True)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    if args.mode == "write_only":
        result = run_write_only(args)
    elif args.mode == "unlink_only":
        result = run_unlink_only(args)
    elif args.mode == "create_evict":
        result = run_create_evict(args)
    elif args.mode == "concurrent_unlink":
        result = run_concurrent_unlink(args)
    else:
        raise RuntimeError(f"unknown mode: {args.mode}")

    result.update({
        "clients": args.clients,
        "files": args.files,
        "unlink_files": args.unlink_files,
        "file_size_bytes": args.file_size,
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
