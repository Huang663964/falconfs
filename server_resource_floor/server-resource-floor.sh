#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="${PID_FILE:-${BASE_DIR}/server-resource-floor.pid}"
LOG_FILE="${LOG_FILE:-${BASE_DIR}/server-resource-floor.log}"

target=25
swing=5
step_sec=20
mem_chunk_mib=64
node=1
python_bin="${PYTHON_BIN:-python3}"

node_cpus=()
node_cpu_count=0
node_cpu_rr=0
mem_cpu_rr=0
taskset_enabled=0

usage() {
    cat <<'USAGE'
Usage:
  bash server-resource-floor.sh start [--target PCT] [--swing PCT] [--step-sec N] [--node NODE]
  bash server-resource-floor.sh stop
  bash server-resource-floor.sh status
  bash server-resource-floor.sh once

Default target is 25%, with light random fluctuation around it.
USAGE
}

resolve_node_cpus() {
    local target_node="$1"
    local core node_id
    node_cpus=()

    if command -v lscpu >/dev/null 2>&1; then
        while IFS=, read -r core node_id; do
            [[ -z "${core}" || "${core}" == \#* ]] && continue
            [[ "${core}" == "-1" || "${node_id}" == "-1" ]] && continue
            [[ "${node_id}" == "${target_node}" ]] && node_cpus+=("${core}")
        done < <(lscpu -p=CPU,NODE --all 2>/dev/null)
    else
        echo "warning: lscpu not found, fallback to all online CPUs for binding" >&2
    fi

    if [[ "${#node_cpus[@]}" -eq 0 ]]; then
        local total_cores i
        total_cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 0)"
        for ((i = 0; i < total_cores; i++)); do
            node_cpus+=("${i}")
        done
    fi

    node_cpu_count="${#node_cpus[@]}"
    if [[ "${node_cpu_count}" -eq 0 ]]; then
        echo "error: failed to detect any CPU for node ${target_node}" >&2
        return 1
    fi

    if ! command -v taskset >/dev/null 2>&1; then
        taskset_enabled=0
    else
        taskset_enabled=1
    fi
}

parse_options() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --target)
                target="$2"
                shift 2
                ;;
            --swing)
                swing="$2"
                shift 2
                ;;
            --step-sec)
                step_sec="$2"
                shift 2
                ;;
            --node)
                node="$2"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                echo "unknown argument: $1" >&2
                usage >&2
                exit 1
                ;;
        esac
    done

    awk -v v="${target}" 'BEGIN { exit !(v ~ /^[0-9]+$/ && v > 0 && v < 90) }' || {
        echo "error: --target must be an integer between 1 and 89" >&2
        exit 1
    }
    awk -v v="${swing}" 'BEGIN { exit !(v ~ /^[0-9]+$/ && v >= 0 && v <= 30) }' || {
        echo "error: --swing must be an integer between 0 and 30" >&2
        exit 1
    }
    awk -v v="${step_sec}" 'BEGIN { exit !(v ~ /^[0-9]+$/ && v > 0) }' || {
        echo "error: --step-sec must be a positive integer" >&2
        exit 1
    }
    awk -v v="${node}" 'BEGIN { exit !(v ~ /^[0-9]+$/) }' || {
        echo "error: --node must be a non-negative integer" >&2
        exit 1
    }
}

is_running() {
    [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null
}

cpu_usage() {
    local a b
    a="$(awk '/^cpu / { print $5 + $6, $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 }' /proc/stat)"
    sleep 1
    b="$(awk '/^cpu / { print $5 + $6, $2 + $3 + $4 + $5 + $6 + $7 + $8 + $9 }' /proc/stat)"
    awk -v a="${a}" -v b="${b}" '
        BEGIN {
            split(a, x, " ")
            split(b, y, " ")
            idle = y[1] - x[1]
            total = y[2] - x[2]
            if (total <= 0) {
                printf "0"
            } else {
                printf "%.0f", 100 * (total - idle) / total
            }
        }'
}

mem_usage() {
    awk '
        /^MemTotal:/ { total = $2 }
        /^MemAvailable:/ { available = $2 }
        END { printf "%.0f", 100 * (total - available) / total }' /proc/meminfo
}

mem_need_mib() {
    local current_target="$1"
    awk -v target="${current_target}" '
        /^MemTotal:/ { total = $2 }
        /^MemAvailable:/ { available = $2 }
        END {
            used = total - available
            need = int(total * target / 100 - used)
            if (need <= 0) {
                print 0
            } else {
                print int(need / 1024) + 1
            }
        }' /proc/meminfo
}

cleanup() {
    local pid
    for pid in "${cpu_pids[@]:-}" "${mem_pids[@]:-}"; do
        [[ -n "${pid}" ]] && kill "${pid}" 2>/dev/null || true
    done
    for pid in "${cpu_pids[@]:-}" "${mem_pids[@]:-}"; do
        [[ -n "${pid}" ]] && wait "${pid}" 2>/dev/null || true
    done
    rm -f "${PID_FILE}"
}

cpu_worker() {
    while :; do
        :
    done
}

mem_worker() {
    local mib="$1"
    exec "${python_bin}" - server-resource-floor-mem "${mib}" <<'PY'
import signal
import sys
import time

def stop(signum, frame):
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

mib = int(sys.argv[2])
buf = bytearray(mib * 1024 * 1024)
for offset in range(0, len(buf), 4096):
    buf[offset] = 1

while True:
    time.sleep(3600)
PY
}

add_cpu_worker() {
    local cpu pid idx
    idx=$((node_cpu_rr % node_cpu_count))
    cpu="${node_cpus[${idx}]}"
    node_cpu_rr=$((node_cpu_rr + 1))

    cpu_worker &
    pid="$!"
    cpu_pids+=("${pid}")
    if [[ "${taskset_enabled}" -eq 1 && -n "${cpu}" ]]; then
        taskset -cp "${cpu}" "${pid}" >/dev/null 2>&1 || true
    fi
}

remove_cpu_worker() {
    local idx pid
    idx=$(( ${#cpu_pids[@]} - 1 ))
    [[ "${idx}" -lt 0 ]] && return
    pid="${cpu_pids[${idx}]}"
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    unset 'cpu_pids[idx]'
    cpu_pids=("${cpu_pids[@]}")
}

add_mem_worker() {
    local cpu pid idx
    idx=$((mem_cpu_rr % node_cpu_count))
    cpu="${node_cpus[${idx}]}"
    mem_cpu_rr=$((mem_cpu_rr + 1))

    mem_worker "${mem_chunk_mib}" &
    pid="$!"
    mem_pids+=("${pid}")
    if [[ "${taskset_enabled}" -eq 1 && -n "${cpu}" ]]; then
        taskset -cp "${cpu}" "${pid}" >/dev/null 2>&1 || true
    fi
}

remove_mem_worker() {
    local idx pid
    idx=$(( ${#mem_pids[@]} - 1 ))
    [[ "${idx}" -lt 0 ]] && return
    pid="${mem_pids[${idx}]}"
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    unset 'mem_pids[idx]'
    mem_pids=("${mem_pids[@]}")
}

random_target() {
    local delta value min_target
    delta=$((RANDOM % (swing * 2 + 1) - swing))
    value=$((target + delta))
    min_target=$((target - swing))
    [[ "${min_target}" -lt 20 ]] && min_target=20
    [[ "${value}" -lt "${min_target}" ]] && value="${min_target}"
    [[ "${value}" -gt 89 ]] && value=89
    echo "${value}"
}

worker_count_for_target() {
    local cores="$1"
    local active_target="$2"
    local baseline_cpu="$3"
    local extra_target workers min_workers
    extra_target=$((active_target - baseline_cpu - 5))
    if [[ "${extra_target}" -le 0 ]]; then
        echo 0
        return
    fi
    workers=$(( (cores * extra_target + 50) / 100 ))
    min_workers=$(( (cores * 20 + 99) / 100 ))
    [[ "${workers}" -lt "${min_workers}" ]] && workers="${min_workers}"
    echo "${workers}"
}

run_forever() {
    parse_options "$@"

    if ! command -v "${python_bin}" >/dev/null 2>&1; then
        echo "error: ${python_bin} is required" >&2
        exit 1
    fi
    if ! resolve_node_cpus "${node}"; then
        exit 1
    fi

    echo "$$" > "${PID_FILE}"
    trap cleanup EXIT
    trap 'exit 0' INT TERM

    cpu_pids=()
    mem_pids=()
    node_cpu_rr=0
    mem_cpu_rr=0

    local cores baseline_cpu current_cpu current_mem active_target mem_active_target desired_cpu desired_mem_mib desired_mem_workers
    cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
    baseline_cpu="$(cpu_usage)"

    while :; do
        active_target="$(random_target)"
        desired_cpu="$(worker_count_for_target "${cores}" "${active_target}" "${baseline_cpu}")"

        while [[ "${#cpu_pids[@]}" -lt "${desired_cpu}" ]]; do
            add_cpu_worker
        done
        while [[ "${#cpu_pids[@]}" -gt "${desired_cpu}" ]]; do
            remove_cpu_worker
        done

        mem_active_target="${active_target}"
        [[ "${mem_active_target}" -gt 29 ]] && mem_active_target=29
        desired_mem_mib="$(mem_need_mib "${mem_active_target}")"
        desired_mem_workers=$(( (desired_mem_mib + mem_chunk_mib - 1) / mem_chunk_mib ))
        while [[ "${#mem_pids[@]}" -lt "${desired_mem_workers}" ]]; do
            add_mem_worker
        done
        while [[ "${#mem_pids[@]}" -gt "${desired_mem_workers}" ]]; do
            remove_mem_worker
        done

        current_cpu="$(cpu_usage)"
        current_mem="$(mem_usage)"
        echo "target=${active_target}% baseline_cpu=${baseline_cpu}% cpu=${current_cpu}% cpu_workers=${#cpu_pids[@]} memory=${current_mem}% memory_workers=${#mem_pids[@]} node=node${node} node_cpu_count=${node_cpu_count}"
        sleep "${step_sec}"
    done
}

start() {
    parse_options "$@"
    if is_running; then
        echo "already running: pid $(cat "${PID_FILE}")"
        exit 0
    fi

    setsid bash "$0" __run --target "${target}" --swing "${swing}" --step-sec "${step_sec}" --node "${node}" </dev/null >> "${LOG_FILE}" 2>&1 &
    echo "$!" > "${PID_FILE}"
    sleep 1

    if is_running; then
        echo "started: pid $(cat "${PID_FILE}")"
        echo "log: ${LOG_FILE}"
        echo "node: ${node}"
    else
        echo "failed to start, check log: ${LOG_FILE}" >&2
        exit 1
    fi
}

stop() {
    local orphan_pids pid

    if is_running; then
        pid="$(cat "${PID_FILE}")"
        kill -- "-${pid}" 2>/dev/null || true
        kill "${pid}" 2>/dev/null || true
    fi

    sleep 1
    orphan_pids="$(ps -eo pid=,args= | awk -v self="$$" '$1 != self && /server-resource-floor[.]sh __run|server-resource-floor-mem/ { print $1 }')"
    if [[ -n "${orphan_pids}" ]]; then
        kill ${orphan_pids} 2>/dev/null || true
    fi

    sleep 1
    if [[ -n "${pid:-}" ]] && kill -0 "${pid}" 2>/dev/null; then
        kill -9 "${pid}" 2>/dev/null || true
    fi
    orphan_pids="$(ps -eo pid=,args= | awk -v self="$$" '$1 != self && /server-resource-floor[.]sh __run|server-resource-floor-mem/ { print $1 }')"
    if [[ -n "${orphan_pids}" ]]; then
        kill -9 ${orphan_pids} 2>/dev/null || true
    fi

    rm -f "${PID_FILE}"
    if [[ -n "${orphan_pids}" ]] || [[ -n "${pid:-}" ]]; then
        echo "stopped"
    else
        echo "not running"
    fi
}

status() {
    if is_running; then
        echo "running: pid $(cat "${PID_FILE}")"
    else
        echo "not running"
    fi
    echo "cpu=$(cpu_usage)% memory=$(mem_usage)%"
}

once() {
    echo "cpu=$(cpu_usage)% memory=$(mem_usage)%"
}

cmd="${1:-}"
if [[ -z "${cmd}" ]]; then
    usage
    exit 1
fi
shift || true

case "${cmd}" in
    start)
        start "$@"
        ;;
    stop)
        stop
        ;;
    status)
        status
        ;;
    once)
        once
        ;;
    __run)
        run_forever "$@"
        ;;
    --help|-h)
        usage
        ;;
    *)
        echo "unknown command: ${cmd}" >&2
        usage >&2
        exit 1
        ;;
esac
