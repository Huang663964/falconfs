#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


PYTHON_CASES = {
    "P-1": "Python internal 纯写",
    "P-2": "Python internal 纯删除",
    "P-3": "Python internal 写触发 evict",
    "P-5": "Python internal 边写边删",
}

FIO_CASES = {
    "B-1": "fio 单文件 direct 顺序写",
    "B-2": "fio 单文件 direct 顺序读",
    "B-3": "fio 多文件 direct 顺序写",
    "B-4": "fio 多文件 direct 顺序读",
    "B-5": "本地多文件删除基准",
}


def load_json(path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


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


def python_row(case_id, data):
    if data is None:
        return [case_id, PYTHON_CASES[case_id], "N/A", "N/A", "N/A", "N/A", "缺失"]
    error = data.get("error_count", 0)
    if case_id == "P-2":
        create = data.get("create", {})
        unlink = data.get("unlink", {})
        return [
            case_id,
            PYTHON_CASES[case_id],
            str(data.get("clients", "N/A")),
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
            PYTHON_CASES[case_id],
            str(data.get("total_processes", data.get("clients", "N/A"))),
            fmt_rate(writer.get("files_per_sec"), writer.get("mib_per_sec")),
            fmt_rate(deleter.get("files_per_sec"), deleter.get("mib_per_sec")),
            fmt_sec(data.get("mixed_elapsed_sec")),
            str(error),
        ]
    return [
        case_id,
        PYTHON_CASES[case_id],
        str(data.get("clients", data.get("writer_clients", "N/A"))),
        fmt_rate(data.get("files_per_sec"), data.get("mib_per_sec")),
        "DiskCache evict" if case_id == "P-3" else "N/A",
        fmt_sec(data.get("elapsed_sec")),
        str(error),
    ]


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
            [case_id, "unlink.elapsed_sec", fmt_num(unlink.get("elapsed_sec"), 6)],
            [case_id, "unlink.files_per_sec", fmt_num(unlink.get("files_per_sec"), 6)],
            [case_id, "unlink.mib_per_sec", fmt_num(unlink.get("mib_per_sec"), 6)],
            [case_id, "unlink.latency_p99", fmt_ms(unlink.get("latency_p99_sec"))],
        ])
    elif case_id == "P-5":
        writer = data.get("writer", {})
        deleter = data.get("deleter", {})
        rows.extend([
            [case_id, "mixed_elapsed_sec", fmt_num(data.get("mixed_elapsed_sec"), 6)],
            [case_id, "writer.files_per_sec", fmt_num(writer.get("files_per_sec"), 6)],
            [case_id, "writer.mib_per_sec", fmt_num(writer.get("mib_per_sec"), 6)],
            [case_id, "deleter.files_per_sec", fmt_num(deleter.get("files_per_sec"), 6)],
            [case_id, "deleter.mib_per_sec", fmt_num(deleter.get("mib_per_sec"), 6)],
            [case_id, "writer.latency_p99", fmt_ms(writer.get("latency_p99_sec"))],
            [case_id, "deleter.latency_p99", fmt_ms(deleter.get("latency_p99_sec"))],
        ])
    return rows


def fio_metric(data, rw):
    if data is None:
        return None
    jobs = data.get("jobs") or []
    if not jobs:
        return None
    job = jobs[0]
    metric = job.get(rw, {})
    lat = metric.get("lat_ns", {})
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
    parser.add_argument("--unlink-files", default="")
    parser.add_argument("--file-size", default="")
    parser.add_argument("--wait-sec", default="")
    parser.add_argument("--fio-size", default="")
    parser.add_argument("--scenario", default="")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    python_dir = out_dir / "python"
    fio_dir = out_dir / "fio"
    output = Path(args.output) if args.output else out_dir / "benchmark_summary.md"
    log_output = Path(args.log_output) if args.log_output else out_dir / "benchmark_summary.log"

    python_data = {case: load_json(python_dir / f"{case}.json") for case in PYTHON_CASES}
    fio_data = {case: load_json(fio_dir / f"{case}.json") for case in FIO_CASES}

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
        ["UNLINK_FILES", args.unlink_files or "N/A"],
        ["FILE_SIZE bytes", args.file_size or "N/A"],
        ["WAIT_SEC", args.wait_sec or "N/A"],
        ["FIO_SIZE", args.fio_size or "N/A"],
    ]))
    lines.append("")
    lines.append("## Python internal API 结果总览")
    lines.append("")
    lines.append(table(
        ["编号", "场景", "并发", "写吞吐", "删除/evict 吞吐", "主要耗时", "错误"],
        [python_row(case, python_data[case]) for case in PYTHON_CASES],
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
    lines.append("## fio 本地基准")
    lines.append("")
    lines.append(table(
        ["编号", "场景", "耗时", "吞吐", "IOPS", "错误"],
        [fio_row(case, fio_data[case]) for case in FIO_CASES],
    ))

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
    storage_info = out_dir / "storage_info.txt"
    raw_rows.append(["storage", str(storage_info), "是" if storage_info.exists() else "否"])
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
