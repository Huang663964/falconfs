#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


PYTHON_CASES = {
    "P-1": "Python internal 纯写",
    "P-2": "Python internal 纯删除",
    "P-3": "Python internal 写触发 evict",
    "P-5": "Python internal 边写边删",
    "P-R1": "Python internal 纯读",
    "P-RW": "Python internal 读 + 写",
    "P-RWE": "Python internal 读 + 写 + evict",
    "P-DIO": "Python internal O_DIRECT 对齐探测",
}

FIO_CASES = {
    "B-1": "fio 单文件 direct 顺序写",
    "B-2": "fio 单文件 direct 顺序读",
    "B-3": "fio 多文件 direct 顺序写",
    "B-4": "fio 多文件 direct 顺序读",
    "B-5": "本地多文件删除基准",
    "B-6": "fio 大文件 2MiB psync 连续写",
    "B-DIO": "fio direct I/O 未对齐写探测",
}



CLEANUP_RE = re.compile(
    r"DiskCache::(?P<kind>CleanupForEvict|Cleanup)\(\): Evicted (?P<files>\d+) files, all size is (?P<size>\d+)"
    r", failed files = (?P<failed>\d+), scanned files = (?P<scanned>\d+)"
    r"(?:, target files = \d+, eviction_ratio = [^,]+)?, cleanup elapsed us = (?P<cleanup_us>\d+)"
    r", remove elapsed us = (?P<remove_us>\d+)"
)
NOTIFY_RE = re.compile(r"DiskCache::(?P<kind>Evict|Cleanup)\(\): NotifyEvicted (?P<items>\d+) items, elapsed us = (?P<elapsed_us>\d+)")
UNLINK_STATS_RE = re.compile(
    r"FalconEvictUnlinkListener stopped, enqueued = (?P<enqueued>\d+), "
    r"processed = (?P<processed>\d+), succeeded = (?P<succeeded>\d+), failed = (?P<failed>\d+), "
    r"batch calls = (?P<batch_calls>\d+), max batch size = (?P<max_batch_size>\d+), "
    r"latency samples = (?P<latency_samples>\d+), avg unlink latency us = (?P<avg_us>\d+), "
    r"p95 unlink latency us = (?P<p95_us>\d+), p99 unlink latency us = (?P<p99_us>\d+), max unlink latency us = (?P<max_us>\d+)"
)


def percentile(values, q):
    if not values:
        return None
    values = sorted(values)
    idx = int(len(values) * q)
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def fmt_mib(bytes_value):
    if bytes_value is None:
        return "N/A"
    return f"{bytes_value / 1048576.0:.2f} MiB"


def fmt_duration_us(value):
    if value is None:
        return "N/A"
    value = float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000.0:.3f}s"
    return f"{value / 1000.0:.2f}ms"


def rate_mib_per_sec(bytes_value, elapsed_us):
    if not bytes_value or not elapsed_us:
        return None
    return bytes_value / 1048576.0 / (elapsed_us / 1_000_000.0)


def rate_files_per_sec(files, elapsed_us):
    if not files or not elapsed_us:
        return None
    return files / (elapsed_us / 1_000_000.0)


def empty_cleanup_bucket():
    return {
        "rounds": 0,
        "files": 0,
        "bytes": 0,
        "failed": 0,
        "scanned": 0,
        "cleanup_us": 0,
        "remove_us": 0,
        "cleanup_samples_us": [],
        "remove_samples_us": [],
    }


def add_cleanup(bucket, files, size, failed, scanned, cleanup_us, remove_us):
    bucket["rounds"] += 1
    bucket["files"] += files
    bucket["bytes"] += size
    bucket["failed"] += failed
    bucket["scanned"] += scanned
    bucket["cleanup_us"] += cleanup_us
    bucket["remove_us"] += remove_us
    bucket["cleanup_samples_us"].append(cleanup_us)
    bucket["remove_samples_us"].append(remove_us)


def iter_text_files(root):
    if not root.exists():
        return
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        name = path.name
        if name.endswith((".json", ".err", ".txt", ".log")) or "falcon." in name:
            yield path


def parse_evict_stats(out_dir):
    stats = {
        "cleanup_for_evict": empty_cleanup_bucket(),
        "cleanup": empty_cleanup_bucket(),
        "combined": empty_cleanup_bucket(),
        "notify_items": 0,
        "notify_us": 0,
        "notify_samples_us": [],
        "unlink_stats": [],
        "matched_files": set(),
    }
    seen_cleanup_lines = set()
    seen_notify_lines = set()
    seen_unlink_lines = set()
    for path in iter_text_files(out_dir):
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    cleanup_match = CLEANUP_RE.search(line)
                    if cleanup_match:
                        line_key = line.strip()
                        if line_key in seen_cleanup_lines:
                            continue
                        seen_cleanup_lines.add(line_key)
                        values = {k: int(v) if k != "kind" else v for k, v in cleanup_match.groupdict().items()}
                        key = "cleanup_for_evict" if values["kind"] == "CleanupForEvict" else "cleanup"
                        add_cleanup(
                            stats[key],
                            values["files"],
                            values["size"],
                            values["failed"],
                            values["scanned"],
                            values["cleanup_us"],
                            values["remove_us"],
                        )
                        add_cleanup(
                            stats["combined"],
                            values["files"],
                            values["size"],
                            values["failed"],
                            values["scanned"],
                            values["cleanup_us"],
                            values["remove_us"],
                        )
                        stats["matched_files"].add(str(path))
                        continue

                    notify_match = NOTIFY_RE.search(line)
                    if notify_match:
                        line_key = line.strip()
                        if line_key in seen_notify_lines:
                            continue
                        seen_notify_lines.add(line_key)
                        items = int(notify_match.group("items"))
                        elapsed_us = int(notify_match.group("elapsed_us"))
                        stats["notify_items"] += items
                        stats["notify_us"] += elapsed_us
                        stats["notify_samples_us"].append(elapsed_us)
                        stats["matched_files"].add(str(path))
                        continue

                    unlink_match = UNLINK_STATS_RE.search(line)
                    if unlink_match:
                        line_key = line.strip()
                        if line_key in seen_unlink_lines:
                            continue
                        seen_unlink_lines.add(line_key)
                        item = {k: int(v) for k, v in unlink_match.groupdict().items()}
                        if item["enqueued"] or item["processed"] or item["failed"]:
                            stats["unlink_stats"].append(item)
                            stats["matched_files"].add(str(path))
        except OSError:
            continue
    return stats


def cleanup_summary_row(name, bucket):
    return [
        name,
        bucket["rounds"],
        bucket["files"],
        fmt_mib(bucket["bytes"]),
        fmt_duration_us(bucket["cleanup_us"]),
        fmt_duration_us(bucket["remove_us"]),
        fmt_rate(rate_files_per_sec(bucket["files"], bucket["cleanup_us"]), rate_mib_per_sec(bucket["bytes"], bucket["cleanup_us"])),
        fmt_rate(rate_files_per_sec(bucket["files"], bucket["remove_us"]), rate_mib_per_sec(bucket["bytes"], bucket["remove_us"])),
        f"{bucket['failed']}/{bucket['scanned']}",
    ]


def latency_rows_for_bucket(name, bucket):
    return [
        [name, "cleanup round p50/p95/p99/max", ", ".join(fmt_duration_us(v) for v in [
            percentile(bucket["cleanup_samples_us"], 0.50),
            percentile(bucket["cleanup_samples_us"], 0.95),
            percentile(bucket["cleanup_samples_us"], 0.99),
            max(bucket["cleanup_samples_us"]) if bucket["cleanup_samples_us"] else None,
        ])],
        [name, "remove round p50/p95/p99/max", ", ".join(fmt_duration_us(v) for v in [
            percentile(bucket["remove_samples_us"], 0.50),
            percentile(bucket["remove_samples_us"], 0.95),
            percentile(bucket["remove_samples_us"], 0.99),
            max(bucket["remove_samples_us"]) if bucket["remove_samples_us"] else None,
        ])],
    ]


def evict_overview_value(evict_stats):
    bucket = evict_stats["combined"]
    if bucket["files"] == 0:
        return "未采集到 evict 日志"
    cleanup_rate = fmt_rate(
        rate_files_per_sec(bucket["files"], bucket["cleanup_us"]),
        rate_mib_per_sec(bucket["bytes"], bucket["cleanup_us"]),
    )
    remove_rate = fmt_rate(
        rate_files_per_sec(bucket["files"], bucket["remove_us"]),
        rate_mib_per_sec(bucket["bytes"], bucket["remove_us"]),
    )
    return f"cleanup {cleanup_rate}; remove {remove_rate}"


def append_evict_section(lines, evict_stats):
    lines.append("")
    lines.append("## DiskCache evict 删除性能与时延")
    lines.append("")
    if evict_stats["combined"]["files"] == 0 and not evict_stats["unlink_stats"]:
        lines.append("未在输出目录中采集到 `DiskCache::CleanupForEvict()` / `DiskCache::Cleanup()` / `FalconEvictUnlinkListener stopped` 日志。")
        lines.append("")
        lines.append("说明：Python internal API 会把 client 侧 Falcon 日志写到 `$OUT_DIR/work_*` 下；summary 必须在清理 `work_*` 之前生成，才能统计 evict 删除吞吐。")
        return

    lines.append(table(
        ["范围", "轮次", "删除文件", "删除大小", "cleanup总耗时", "remove总耗时", "cleanup吞吐", "remove吞吐", "失败/扫描"],
        [
            cleanup_summary_row("前台 CleanupForEvict", evict_stats["cleanup_for_evict"]),
            cleanup_summary_row("后台 Cleanup", evict_stats["cleanup"]),
            cleanup_summary_row("合计", evict_stats["combined"]),
        ],
    ))
    lines.append("")

    latency_rows = []
    if evict_stats["cleanup_for_evict"]["rounds"]:
        latency_rows.extend(latency_rows_for_bucket("前台 CleanupForEvict", evict_stats["cleanup_for_evict"]))
    if evict_stats["cleanup"]["rounds"]:
        latency_rows.extend(latency_rows_for_bucket("后台 Cleanup", evict_stats["cleanup"]))
    if evict_stats["notify_samples_us"]:
        latency_rows.append(["NotifyEvicted", "p50/p95/p99/max", ", ".join(fmt_duration_us(v) for v in [
            percentile(evict_stats["notify_samples_us"], 0.50),
            percentile(evict_stats["notify_samples_us"], 0.95),
            percentile(evict_stats["notify_samples_us"], 0.99),
            max(evict_stats["notify_samples_us"]),
        ])])
    if latency_rows:
        lines.append(table(["范围", "指标", "值"], latency_rows))
        lines.append("")

    if evict_stats["unlink_stats"]:
        rows = []
        for item in evict_stats["unlink_stats"]:
            rows.append([
                item["enqueued"],
                item["processed"],
                item["succeeded"],
                item["failed"],
                item["batch_calls"],
                item["max_batch_size"],
                item["latency_samples"],
                fmt_duration_us(item["avg_us"]),
                fmt_duration_us(item["p95_us"]),
                fmt_duration_us(item["p99_us"]),
                fmt_duration_us(item["max_us"]),
            ])
        lines.append("### Evict 前置 metadata 条件 unlink")
        lines.append("")
        lines.append(table(
            ["enqueued", "processed", "succeeded", "failed", "batch calls", "max batch", "samples", "avg", "p95", "p99", "max"],
            rows,
        ))
        lines.append("")

    matched = sorted(evict_stats["matched_files"])
    if matched:
        lines.append("### Evict 日志来源")
        lines.append("")
        lines.append(table(["文件"], [[path] for path in matched[:20]]))
        if len(matched) > 20:
            lines.append("")
            lines.append(f"仅展示前 20 个日志文件，实际匹配 {len(matched)} 个文件。")

def load_json(path):
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        return {"_load_error": f"invalid json: {exc}"}


def fmt_num(value, digits=2):
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "N/A"


def fmt_sec(value):
    return f"{float(value):.3f}s" if value is not None else "N/A"


def fmt_ms(value):
    return f"{float(value) * 1000.0:.2f}ms" if value is not None else "N/A"


def fmt_rate(files_per_sec, mib_per_sec):
    if files_per_sec is None and mib_per_sec is None:
        return "N/A"
    if files_per_sec is None:
        return f"{fmt_num(mib_per_sec)} MiB/s"
    if mib_per_sec is None:
        return f"{fmt_num(files_per_sec)} files/s"
    return f"{fmt_num(files_per_sec)} files/s, {fmt_num(mib_per_sec)} MiB/s"


def python_row(case_id, data, evict_stats=None):
    name = PYTHON_CASES[case_id]
    if data is None:
        return [case_id, name, "N/A", "N/A", "N/A", "N/A", "N/A", "缺失"]
    if data.get("_load_error"):
        return [case_id, name, "N/A", "N/A", "N/A", "N/A", "N/A", data["_load_error"]]
    error = data.get("error_count", 0)
    if case_id == "P-2":
        create = data.get("create", {})
        unlink = data.get("unlink", {})
        return [
            case_id,
            name,
            str(data.get("clients", "N/A")),
            "N/A",
            fmt_rate(create.get("files_per_sec"), create.get("mib_per_sec")),
            fmt_rate(unlink.get("files_per_sec"), unlink.get("mib_per_sec")),
            fmt_sec(unlink.get("elapsed_sec")),
            str(error),
        ]
    if case_id == "P-5":
        writer = data.get("writer", {})
        deleter = data.get("deleter", {})
        return [
            case_id,
            name,
            str(data.get("total_processes", data.get("clients", "N/A"))),
            "N/A",
            fmt_rate(writer.get("files_per_sec"), writer.get("mib_per_sec")),
            fmt_rate(deleter.get("files_per_sec"), deleter.get("mib_per_sec")),
            fmt_sec(data.get("mixed_elapsed_sec")),
            str(error),
        ]
    if case_id == "P-R1":
        read = data.get("read", {})
        return [
            case_id,
            name,
            str(data.get("clients", "N/A")),
            fmt_rate(read.get("files_per_sec"), read.get("mib_per_sec")),
            "N/A",
            "N/A",
            fmt_sec(read.get("elapsed_sec")),
            str(error),
        ]
    if case_id in ("P-RW", "P-RWE"):
        reader = data.get("reader", {})
        writer = data.get("writer", {})
        evict = evict_overview_value(evict_stats) if case_id == "P-RWE" and evict_stats is not None else "N/A"
        return [
            case_id,
            name,
            str(data.get("total_processes", data.get("clients", "N/A"))),
            fmt_rate(reader.get("files_per_sec"), reader.get("mib_per_sec")),
            fmt_rate(writer.get("files_per_sec"), writer.get("mib_per_sec")),
            evict,
            fmt_sec(data.get("mixed_elapsed_sec")),
            str(error),
        ]
    if case_id == "P-DIO":
        probes = data.get("probes") or []
        failed = sum(1 for item in probes if item.get("write_ret") not in (0, None) or item.get("error"))
        return [case_id, name, "1", "N/A", "见 direct I/O 探测", "N/A", f"probes={len(probes)}, failed_or_rejected={failed}", str(error)]
    return [
        case_id,
        name,
        str(data.get("clients", data.get("writer_clients", "N/A"))),
        "N/A",
        fmt_rate(data.get("files_per_sec"), data.get("mib_per_sec")),
        evict_overview_value(evict_stats) if case_id == "P-3" and evict_stats is not None else ("DiskCache evict" if case_id == "P-3" else "N/A"),
        fmt_sec(data.get("elapsed_sec")),
        str(error),
    ]


def stop_reasons(group):
    counts = {}
    for item in group.get("per_client") or []:
        reason = item.get("stop_reason") or "N/A"
        counts[reason] = counts.get(reason, 0) + 1
    if not counts:
        return "N/A"
    return ", ".join(f"{key}:{counts[key]}" for key in sorted(counts))

def python_detail_rows(case_id, data):
    if data is None:
        return []
    rows = []
    if case_id in ("P-1", "P-3"):
        rows.extend([
            [case_id, "elapsed_sec", fmt_num(data.get("elapsed_sec"), 6)],
            [case_id, "files_per_sec", fmt_num(data.get("files_per_sec"), 6)],
            [case_id, "mib_per_sec", fmt_num(data.get("mib_per_sec"), 6)],
            [case_id, "latency_p50", fmt_ms(data.get("latency_p50_sec"))],
            [case_id, "latency_p95", fmt_ms(data.get("latency_p95_sec"))],
            [case_id, "latency_p99", fmt_ms(data.get("latency_p99_sec"))],
        ])
        if case_id == "P-3":
            rows.append([case_id, "wait_sec", fmt_num(data.get("wait_sec"), 0)])
    elif case_id == "P-2":
        unlink = data.get("unlink", {})
        rows.extend([
            [case_id, "create.elapsed_sec", fmt_num(data.get("create", {}).get("elapsed_sec"), 6)],
            [case_id, "create.files_per_sec", fmt_num(data.get("create", {}).get("files_per_sec"), 6)],
            [case_id, "create.mib_per_sec", fmt_num(data.get("create", {}).get("mib_per_sec"), 6)],
            [case_id, "unlink.elapsed_sec", fmt_num(unlink.get("elapsed_sec"), 6)],
            [case_id, "unlink.files_per_sec", fmt_num(unlink.get("files_per_sec"), 6)],
            [case_id, "unlink.mib_per_sec", fmt_num(unlink.get("mib_per_sec"), 6)],
            [case_id, "unlink.latency_p99", fmt_ms(unlink.get("latency_p99_sec"))],
        ])
    elif case_id == "P-5":
        writer = data.get("writer", {})
        deleter = data.get("deleter", {})
        rows.extend([
            [case_id, "timed", str(data.get("timed", False))],
            [case_id, "duration_sec", fmt_num(data.get("duration_sec"), 6)],
            [case_id, "mixed_elapsed_sec", fmt_num(data.get("mixed_elapsed_sec"), 6)],
            [case_id, "requested_write_files", fmt_num(data.get("requested_write_files"), 0)],
            [case_id, "requested_delete_files", fmt_num(data.get("requested_delete_files"), 0)],
            [case_id, "delete_dataset_exhausted", str(data.get("delete_dataset_exhausted", "N/A"))],
            [case_id, "writer.files_per_sec", fmt_num(writer.get("files_per_sec"), 6)],
            [case_id, "writer.mib_per_sec", fmt_num(writer.get("mib_per_sec"), 6)],
            [case_id, "deleter.files_per_sec", fmt_num(deleter.get("files_per_sec"), 6)],
            [case_id, "deleter.mib_per_sec", fmt_num(deleter.get("mib_per_sec"), 6)],
            [case_id, "writer.latency_p99", fmt_ms(writer.get("latency_p99_sec"))],
            [case_id, "deleter.latency_p99", fmt_ms(deleter.get("latency_p99_sec"))],
            [case_id, "writer.stop_reasons", stop_reasons(writer)],
            [case_id, "deleter.stop_reasons", stop_reasons(deleter)],
        ])
    elif case_id == "P-R1":
        read = data.get("read", {})
        rows.extend([
            [case_id, "read.elapsed_sec", fmt_num(read.get("elapsed_sec"), 6)],
            [case_id, "read.files_per_sec", fmt_num(read.get("files_per_sec"), 6)],
            [case_id, "read.mib_per_sec", fmt_num(read.get("mib_per_sec"), 6)],
            [case_id, "read.latency_p99", fmt_ms(read.get("latency_p99_sec"))],
        ])
    elif case_id in ("P-RW", "P-RWE"):
        reader = data.get("reader", {})
        writer = data.get("writer", {})
        rows.extend([
            [case_id, "timed", str(data.get("timed", False))],
            [case_id, "duration_sec", fmt_num(data.get("duration_sec"), 6)],
            [case_id, "read_pattern", str(data.get("read_pattern", "N/A"))],
            [case_id, "hot_read_window", fmt_num(data.get("hot_read_window"), 0)],
            [case_id, "mixed_elapsed_sec", fmt_num(data.get("mixed_elapsed_sec"), 6)],
            [case_id, "reader.files_per_sec", fmt_num(reader.get("files_per_sec"), 6)],
            [case_id, "reader.mib_per_sec", fmt_num(reader.get("mib_per_sec"), 6)],
            [case_id, "writer.files_per_sec", fmt_num(writer.get("files_per_sec"), 6)],
            [case_id, "writer.mib_per_sec", fmt_num(writer.get("mib_per_sec"), 6)],
            [case_id, "reader.latency_p99", fmt_ms(reader.get("latency_p99_sec"))],
            [case_id, "writer.latency_p99", fmt_ms(writer.get("latency_p99_sec"))],
            [case_id, "reader.operation_error_count", fmt_num(reader.get("operation_error_count"), 0)],
            [case_id, "writer.operation_error_count", fmt_num(writer.get("operation_error_count"), 0)],
            [case_id, "reader.max_worker_elapsed_sec", fmt_num(reader.get("max_worker_elapsed_sec"), 6)],
            [case_id, "writer.max_worker_elapsed_sec", fmt_num(writer.get("max_worker_elapsed_sec"), 6)],
            [case_id, "reader.stop_reasons", stop_reasons(reader)],
            [case_id, "writer.stop_reasons", stop_reasons(writer)],
        ])
        if case_id == "P-RWE":
            rows.append([case_id, "wait_sec", fmt_num(data.get("wait_sec"), 0)])
    elif case_id == "P-DIO":
        for item in data.get("probes") or []:
            parts = [
                f"create={item.get('create_ret')}",
                f"write={item.get('write_ret')}",
                f"flush={item.get('flush_ret')}",
                f"close={item.get('close_ret')}",
                f"error={item.get('error', '')}",
            ]
            if item.get("cleanup_warning"):
                parts.append(f"cleanup_warning={item.get('cleanup_warning')}")
            rows.append([case_id, item.get("name", "probe"), " ".join(parts)])
    return rows


def fio_metric(data, rw):
    if data is None:
        return None
    jobs = data.get("jobs") or []
    if not jobs:
        return None
    job = jobs[0]
    metric = job.get(rw, {})
    lat = metric.get("clat_ns") or metric.get("lat_ns", {})
    pct = lat.get("percentile", {})
    return {
        "runtime_sec": (metric.get("runtime") or 0) / 1000.0,
        "mib_per_sec": (metric.get("bw_bytes") or 0) / 1048576.0,
        "iops": metric.get("iops"),
        "p50_ms": (pct.get("50.000000") or 0) / 1000000.0,
        "p95_ms": (pct.get("95.000000") or 0) / 1000000.0,
        "p99_ms": (pct.get("99.000000") or 0) / 1000000.0,
        "max_ms": (lat.get("max") or 0) / 1000000.0,
        "error": job.get("error", 0),
    }


def fio_row(case_id, data):
    if data is None:
        return [case_id, FIO_CASES[case_id], "N/A", "N/A", "N/A", "缺失"]
    if case_id == "B-DIO":
        if data.get("_load_error"):
            return [case_id, FIO_CASES[case_id], "N/A", "见 B-DIO-status.json", "N/A", data["_load_error"]]
        if data.get("mode") == "fio_direct_unaligned":
            return [case_id, FIO_CASES[case_id], "N/A", "见 B-DIO-status.json", "N/A", f"exit_status={data.get('exit_status')}"]
        job_error = "N/A"
        jobs = data.get("jobs") or []
        if jobs:
            job_error = str(jobs[0].get("error", 0))
        return [case_id, FIO_CASES[case_id], "N/A", "见 B-DIO-status.json", "N/A", job_error]
    if data.get("_load_error"):
        return [case_id, FIO_CASES[case_id], "N/A", "N/A", "N/A", data["_load_error"]]
    if case_id == "B-5":
        return [
            case_id,
            FIO_CASES[case_id],
            fmt_sec(data.get("delete_elapsed_sec")),
            fmt_rate(data.get("delete_files_per_sec"), data.get("delete_mib_per_sec")),
            "N/A",
            "0" if data.get("remaining_files") == 0 else "remaining_files=" + str(data.get("remaining_files")),
        ]
    rw = "read" if case_id in ("B-2", "B-4") else "write"
    metric = fio_metric(data, rw)
    if metric is None:
        return [case_id, FIO_CASES[case_id], "N/A", "N/A", "N/A", "缺少指标"]
    return [
        case_id,
        FIO_CASES[case_id],
        fmt_sec(metric.get("runtime_sec")),
        fmt_rate(None, metric.get("mib_per_sec")),
        fmt_num(metric.get("iops")),
        str(metric.get("error", 0)),
    ]


def fio_matrix_rows(fio_dir):
    rows = []
    for path in sorted(fio_dir.glob("B-*-N*.json")):
        stem = path.stem
        if stem.endswith("-prepare"):
            continue
        data = load_json(path)
        if not data or data.get("_load_error"):
            rows.append([stem, "N/A", "N/A", "N/A", data.get("_load_error", "缺失") if data else "缺失"])
            continue
        if stem.startswith("B-RW-"):
            read = fio_metric(data, "read") or {}
            write = fio_metric(data, "write") or {}
            rows.append([stem, fmt_rate(None, read.get("mib_per_sec")), fmt_rate(None, write.get("mib_per_sec")), fmt_num(read.get("p99_ms")), str(read.get("error", 0) or write.get("error", 0))])
        elif stem.startswith("B-R-"):
            read = fio_metric(data, "read") or {}
            rows.append([stem, fmt_rate(None, read.get("mib_per_sec")), "N/A", fmt_num(read.get("p99_ms")), str(read.get("error", 0))])
        else:
            write = fio_metric(data, "write") or {}
            rows.append([stem, "N/A", fmt_rate(None, write.get("mib_per_sec")), fmt_num(write.get("p99_ms")), str(write.get("error", 0))])
    return rows


def table(headers, rows):
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        lines.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Summarize pyfalcon benchmark results.")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--output", default=None)
    parser.add_argument("--log-output", default=None)
    parser.add_argument("--clients", default="")
    parser.add_argument("--files", default="")
    parser.add_argument("--read-files", default="")
    parser.add_argument("--unlink-files", default="")
    parser.add_argument("--file-size", default="")
    parser.add_argument("--wait-sec", default="")
    parser.add_argument("--mixed-duration-sec", default="")
    parser.add_argument("--max-local-disk-size", default="")
    parser.add_argument("--fio-size", default="")
    parser.add_argument("--fio-large-size", default="")
    parser.add_argument("--fio-large-runtime", default="")
    parser.add_argument("--scenario", default="")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    python_dir = out_dir / "python"
    fio_dir = out_dir / "fio"
    output = Path(args.output) if args.output else out_dir / "benchmark_summary.md"
    log_output = Path(args.log_output) if args.log_output else out_dir / "benchmark_summary.log"

    python_data = {case: load_json(python_dir / f"{case}.json") for case in PYTHON_CASES}
    fio_data = {case: load_json(fio_dir / f"{case}.json") for case in FIO_CASES}
    evict_stats = parse_evict_stats(out_dir)
    evict_stats_by_case = {
        "P-3": parse_evict_stats(out_dir / "work_P-3"),
        "P-RWE": parse_evict_stats(out_dir / "work_P-RWE"),
    }

    lines = []
    lines.append("# PyFalcon internal client benchmark 测试结果")
    lines.append("")
    lines.append("## 测试条件")
    lines.append("")
    lines.append(table(["项目", "值"], [
        ["输出目录", str(out_dir)],
        ["执行场景", args.scenario or "N/A"],
        ["CLIENTS", args.clients or "N/A"],
        ["FILES", args.files or "N/A"],
        ["READ_FILES", args.read_files or "N/A"],
        ["UNLINK_FILES", args.unlink_files or "N/A"],
        ["FILE_SIZE bytes", args.file_size or "N/A"],
        ["WAIT_SEC", args.wait_sec or "N/A"],
        ["MIXED_DURATION_SEC", args.mixed_duration_sec or "N/A"],
        ["MAX_LOCAL_DISK_SIZE GiB", args.max_local_disk_size or "N/A"],
        ["FIO_SIZE", args.fio_size or "N/A"],
        ["FIO_LARGE_SIZE", args.fio_large_size or "N/A"],
        ["FIO_LARGE_RUNTIME", args.fio_large_runtime or "N/A"],
    ]))
    lines.append("")
    lines.append("## Python internal API 结果总览")
    lines.append("")
    lines.append(table(
        ["编号", "场景", "并发", "读吞吐", "写吞吐", "删除/evict 吞吐", "主要耗时", "错误"],
        [python_row(case, python_data[case], evict_stats_by_case.get(case, evict_stats) if case in ("P-3", "P-RWE") else None) for case in PYTHON_CASES],
    ))
    detail_rows = []
    for case in PYTHON_CASES:
        detail_rows.extend(python_detail_rows(case, python_data[case]))
    if detail_rows:
        lines.append("")
        lines.append("## Python JSON 明细")
        lines.append("")
        lines.append(table(["编号", "指标", "值"], detail_rows))

    lines.append("")
    append_evict_section(lines, evict_stats)

    lines.append("")
    lines.append("## fio 本地基准")
    lines.append("")
    lines.append(table(
        ["编号", "场景", "耗时", "吞吐", "IOPS", "错误"],
        [fio_row(case, fio_data[case]) for case in FIO_CASES],
    ))

    matrix_rows = fio_matrix_rows(fio_dir)
    if matrix_rows:
        lines.append("")
        lines.append("## fio 多并发矩阵")
        lines.append("")
        lines.append(table(["编号", "读吞吐", "写吞吐", "p99(ms)", "错误"], matrix_rows))

    dio_status = load_json(fio_dir / "B-DIO-status.json")
    if dio_status:
        lines.append("")
        lines.append("## direct I/O 未对齐探测")
        lines.append("")
        lines.append(table(["项目", "值"], [["fio B-DIO exit_status", dio_status.get("exit_status")], ["expected_may_fail", dio_status.get("expected_may_fail")]]))

    lines.append("")
    lines.append("## 原始结果文件")
    lines.append("")
    raw_rows = []
    for case in PYTHON_CASES:
        path = python_dir / f"{case}.json"
        raw_rows.append([case, str(path), "是" if path.exists() else "否"])
    for case in FIO_CASES:
        path = fio_dir / f"{case}.json"
        raw_rows.append([case, str(path), "是" if path.exists() else "否"])
    for path in sorted(fio_dir.glob("B-*-N*.json")):
        raw_rows.append([path.stem, str(path), "是"])
    status_path = fio_dir / "B-DIO-status.json"
    raw_rows.append(["B-DIO-status", str(status_path), "是" if status_path.exists() else "否"])
    storage_info = out_dir / "storage_info.txt"
    raw_rows.append(["storage", str(storage_info), "是" if storage_info.exists() else "否"])
    evict_config = out_dir / "evict_config.txt"
    raw_rows.append(["evict_config", str(evict_config), "是" if evict_config.exists() else "否"])
    lines.append(table(["编号", "路径", "是否存在"], raw_rows))
    lines.append("")

    content = "\n".join(lines)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    log_output.write_text(content, encoding="utf-8")
    print(f"wrote {output}")
    print(f"wrote {log_output}")


if __name__ == "__main__":
    main()
