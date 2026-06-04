#!/usr/bin/env python3
import argparse
import json
import os
import statistics
import threading
import time


def percentile(values, q):
    if not values:
        return 0.0
    data = sorted(values)
    idx = int(len(data) * q)
    if idx >= len(data):
        idx = len(data) - 1
    return data[idx]


def latency_stats(prefix, values):
    return {
        f"{prefix}_avg_sec": statistics.fmean(values) if values else 0.0,
        f"{prefix}_p50_sec": percentile(values, 0.50),
        f"{prefix}_p95_sec": percentile(values, 0.95),
        f"{prefix}_p99_sec": percentile(values, 0.99),
        f"{prefix}_max_sec": max(values) if values else 0.0,
    }


def path_join(root, index):
    return os.path.join(root, f"f_{index:08d}")


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def create_write_close(path, payload):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
    try:
        written = 0
        while written < len(payload):
            written += os.write(fd, payload[written:])
        os.fsync(fd)
    finally:
        os.close(fd)


def count_files(path):
    total = 0
    for _, _, files in os.walk(path):
        total += len(files)
    return total


def worker_count(args, total):
    if total <= 0:
        return 1
    return max(1, min(args.workers, total))


def run_parallel(total, workers, operation):
    workers = max(1, min(workers, max(1, total)))
    per_worker = [[] for _ in range(workers)]
    next_index = 0
    errors = 0
    index_lock = threading.Lock()
    error_lock = threading.Lock()
    start_event = threading.Event()

    def has_error():
        with error_lock:
            return errors > 0

    def add_error():
        nonlocal errors
        with error_lock:
            errors += 1

    def take_index():
        nonlocal next_index
        with index_lock:
            if next_index >= total:
                return None
            value = next_index
            next_index += 1
            return value

    def worker(worker_id):
        start_event.wait()
        while not has_error():
            i = take_index()
            if i is None:
                break
            op_begin = time.monotonic()
            try:
                operation(i)
            except OSError:
                add_error()
                break
            per_worker[worker_id].append(time.monotonic() - op_begin)

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(workers)]
    for thread in threads:
        thread.start()
    begin = time.monotonic()
    start_event.set()
    for thread in threads:
        thread.join()
    end = time.monotonic()
    latencies = [lat for values in per_worker for lat in values]
    return latencies, errors, end - begin


def run_write_only(args, payload):
    workers = worker_count(args, args.files)
    latencies, errors, elapsed = run_parallel(
        args.files,
        workers,
        lambda i: create_write_close(path_join(args.dir, i), payload),
    )
    if args.wait_sec > 0:
        time.sleep(args.wait_sec)
    return {
        "mode": args.mode,
        "dir": args.dir,
        "workers": workers,
        "files": args.files,
        "file_size_bytes": args.file_size,
        "written_files": len(latencies),
        "elapsed_sec": elapsed,
        "throughput_files_per_sec": len(latencies) / elapsed if elapsed > 0 else 0.0,
        "throughput_mib_per_sec": len(latencies) * args.file_size / elapsed / 1048576.0 if elapsed > 0 else 0.0,
        **latency_stats("write_latency", latencies),
        "remaining_files": count_files(args.dir),
        "error_count": errors,
    }


def run_unlink_only(args, payload):
    workers = worker_count(args, args.files)
    create_latencies, create_errors, create_elapsed = run_parallel(
        args.files,
        workers,
        lambda i: create_write_close(path_join(args.dir, i), payload),
    )
    unlink_latencies, unlink_errors, unlink_elapsed = run_parallel(
        len(create_latencies),
        workers,
        lambda i: os.unlink(path_join(args.dir, i)),
    )
    return {
        "mode": args.mode,
        "dir": args.dir,
        "workers": workers,
        "files": args.files,
        "file_size_bytes": args.file_size,
        "created_files": len(create_latencies),
        "unlinked_files": len(unlink_latencies),
        "create_elapsed_sec": create_elapsed,
        "unlink_elapsed_sec": unlink_elapsed,
        "create_files_per_sec": len(create_latencies) / create_elapsed if create_elapsed > 0 else 0.0,
        "unlink_files_per_sec": len(unlink_latencies) / unlink_elapsed if unlink_elapsed > 0 else 0.0,
        **latency_stats("create_latency", create_latencies),
        **latency_stats("unlink_latency", unlink_latencies),
        "remaining_files": count_files(args.dir),
        "error_count": create_errors + unlink_errors,
    }


def run_create_unlink(args, payload):
    create_latencies = []
    unlink_latencies = []
    pending = []
    errors = 0
    begin = time.monotonic()
    for i in range(args.files):
        path = path_join(args.dir, i)
        op_begin = time.monotonic()
        try:
            create_write_close(path, payload)
        except OSError:
            errors += 1
            break
        create_latencies.append(time.monotonic() - op_begin)
        pending.append(path)
        if len(pending) > args.window:
            old = pending.pop(0)
            op_begin = time.monotonic()
            try:
                os.unlink(old)
            except OSError:
                errors += 1
                break
            unlink_latencies.append(time.monotonic() - op_begin)

    tail_begin = time.monotonic()
    while pending and errors == 0:
        old = pending.pop(0)
        op_begin = time.monotonic()
        try:
            os.unlink(old)
        except OSError:
            errors += 1
            break
        unlink_latencies.append(time.monotonic() - op_begin)
    end = time.monotonic()

    elapsed = end - begin
    return {
        "mode": args.mode,
        "dir": args.dir,
        "files": args.files,
        "file_size_bytes": args.file_size,
        "window": args.window,
        "created_files": len(create_latencies),
        "unlinked_files": len(unlink_latencies),
        "mixed_elapsed_sec": elapsed,
        "tail_cleanup_elapsed_sec": end - tail_begin,
        "create_files_per_sec": len(create_latencies) / elapsed if elapsed > 0 else 0.0,
        "unlink_files_per_sec": len(unlink_latencies) / elapsed if elapsed > 0 else 0.0,
        "total_ops_per_sec": (len(create_latencies) + len(unlink_latencies)) / elapsed if elapsed > 0 else 0.0,
        **latency_stats("create_latency", create_latencies),
        **latency_stats("unlink_latency", unlink_latencies),
        "remaining_files": count_files(args.dir),
        "error_count": errors,
    }



def run_concurrent_unlink(args, payload):
    write_dir = os.path.join(args.dir, "write")
    delete_dir = os.path.join(args.dir, "delete")
    ensure_dir(write_dir)
    ensure_dir(delete_dir)

    workers = worker_count(args, max(args.files, args.unlink_files))
    prepare_latencies, prepare_errors, prepare_elapsed = run_parallel(
        args.unlink_files,
        workers,
        lambda i: create_write_close(path_join(delete_dir, i), payload),
    )

    write_per_worker = [[] for _ in range(workers)]
    unlink_per_worker = [[] for _ in range(workers)]
    writer_begins = [0.0] * workers
    writer_ends = [0.0] * workers
    deleter_begins = [0.0] * workers
    deleter_ends = [0.0] * workers
    next_write = 0
    next_delete = 0
    error_count = prepare_errors
    write_lock = threading.Lock()
    delete_lock = threading.Lock()
    error_lock = threading.Lock()
    start_event = threading.Event()
    delete_total = len(prepare_latencies)

    def has_error():
        with error_lock:
            return error_count > 0

    def add_error():
        nonlocal error_count
        with error_lock:
            error_count += 1

    def take_write():
        nonlocal next_write
        with write_lock:
            if next_write >= args.files:
                return None
            value = next_write
            next_write += 1
            return value

    def take_delete():
        nonlocal next_delete
        with delete_lock:
            if next_delete >= delete_total:
                return None
            value = next_delete
            next_delete += 1
            return value

    def writer(worker_id):
        start_event.wait()
        writer_begins[worker_id] = time.monotonic()
        while not has_error():
            i = take_write()
            if i is None:
                break
            op_begin = time.monotonic()
            try:
                create_write_close(path_join(write_dir, i), payload)
            except OSError:
                add_error()
                break
            write_per_worker[worker_id].append(time.monotonic() - op_begin)
        writer_ends[worker_id] = time.monotonic()

    def deleter(worker_id):
        start_event.wait()
        deleter_begins[worker_id] = time.monotonic()
        while not has_error():
            i = take_delete()
            if i is None:
                break
            op_begin = time.monotonic()
            try:
                os.unlink(path_join(delete_dir, i))
            except OSError:
                add_error()
                break
            unlink_per_worker[worker_id].append(time.monotonic() - op_begin)
        deleter_ends[worker_id] = time.monotonic()

    threads = []
    for worker_id in range(workers):
        threads.append(threading.Thread(target=writer, args=(worker_id,)))
        threads.append(threading.Thread(target=deleter, args=(worker_id,)))
    for thread in threads:
        thread.start()
    mixed_begin = time.monotonic()
    start_event.set()
    for thread in threads:
        thread.join()
    mixed_end = time.monotonic()

    write_latencies = [lat for values in write_per_worker for lat in values]
    unlink_latencies = [lat for values in unlink_per_worker for lat in values]
    writer_elapsed = max(writer_ends) - min(writer_begins) if writer_begins and min(writer_begins) > 0 else 0.0
    deleter_elapsed = max(deleter_ends) - min(deleter_begins) if deleter_begins and min(deleter_begins) > 0 else 0.0
    mixed_elapsed = mixed_end - mixed_begin
    return {
        "mode": args.mode,
        "dir": args.dir,
        "write_dir": write_dir,
        "delete_dir": delete_dir,
        "workers": workers,
        "files": args.files,
        "unlink_files": args.unlink_files,
        "file_size_bytes": args.file_size,
        "prepared_delete_files": len(prepare_latencies),
        "prepare_elapsed_sec": prepare_elapsed,
        "written_files": len(write_latencies),
        "unlinked_files": len(unlink_latencies),
        "mixed_elapsed_sec": mixed_elapsed,
        "writer_elapsed_sec": writer_elapsed,
        "deleter_elapsed_sec": deleter_elapsed,
        "write_files_per_sec_by_writer": len(write_latencies) / writer_elapsed if writer_elapsed > 0 else 0.0,
        "write_mib_per_sec_by_writer": len(write_latencies) * args.file_size / writer_elapsed / 1048576.0 if writer_elapsed > 0 else 0.0,
        "unlink_files_per_sec_by_deleter": len(unlink_latencies) / deleter_elapsed if deleter_elapsed > 0 else 0.0,
        "unlink_mib_per_sec_by_deleter": len(unlink_latencies) * args.file_size / deleter_elapsed / 1048576.0 if deleter_elapsed > 0 else 0.0,
        "write_files_per_sec_by_mixed_window": len(write_latencies) / mixed_elapsed if mixed_elapsed > 0 else 0.0,
        "unlink_files_per_sec_by_mixed_window": len(unlink_latencies) / mixed_elapsed if mixed_elapsed > 0 else 0.0,
        **latency_stats("prepare_latency", prepare_latencies),
        **latency_stats("write_latency", write_latencies),
        **latency_stats("unlink_latency", unlink_latencies),
        "remaining_files": count_files(args.dir),
        "error_count": error_count,
    }

def main():
    parser = argparse.ArgumentParser(description="Run FalconFS FUSE performance scenarios.")
    parser.add_argument("--mode", required=True, choices=["write_only", "unlink_only", "create_evict", "create_unlink", "concurrent_unlink"])
    parser.add_argument("--dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--files", required=True, type=int)
    parser.add_argument("--file-size", required=True, type=int)
    parser.add_argument("--window", type=int, default=500)
    parser.add_argument("--unlink-files", type=int, default=None)
    parser.add_argument("--wait-sec", type=int, default=0)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()

    if args.unlink_files is None:
        args.unlink_files = args.files
    if args.workers <= 0:
        args.workers = 1
    ensure_dir(args.dir)
    payload = b"a" * args.file_size
    if args.mode in ("write_only", "create_evict"):
        result = run_write_only(args, payload)
    elif args.mode == "unlink_only":
        result = run_unlink_only(args, payload)
    elif args.mode == "create_unlink":
        result = run_create_unlink(args, payload)
    else:
        result = run_concurrent_unlink(args, payload)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as out:
        json.dump(result, out, indent=2, sort_keys=True)
        out.write("\n")


if __name__ == "__main__":
    main()
