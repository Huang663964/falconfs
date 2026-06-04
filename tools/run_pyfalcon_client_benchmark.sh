#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
BENCHMARK_ROOT="${BENCHMARK_ROOT:-${PERF_ROOT:-}}"
if [[ -n "$BENCHMARK_ROOT" ]]; then
    mkdir -p "$BENCHMARK_ROOT"
    BENCHMARK_ROOT="$(readlink -f "$BENCHMARK_ROOT")"
    OUT_DIR="${OUT_DIR:-$BENCHMARK_ROOT/pyfalcon_client_benchmark_$RUN_ID}"
    CACHE_ROOT="${CACHE_ROOT:-$BENCHMARK_ROOT/falcon_cache}"
    FIO_DIR="${FIO_DIR:-$BENCHMARK_ROOT/pyfalcon_fio_baseline}"
else
    OUT_DIR="${OUT_DIR:-/tmp/pyfalcon_client_benchmark_$RUN_ID}"
    CACHE_ROOT="${CACHE_ROOT:-/tmp/falcon_cache}"
    FIO_DIR="${FIO_DIR:-/tmp/pyfalcon_fio_baseline}"
fi
MOUNT_DIR="${MOUNT_DIR:-/tmp/falcon_mnt}"
CONFIG_FILE_PATH="${CONFIG_FILE_PATH:-/usr/local/falconfs/falcon_client/config/config.json}"
PYTHON_INTERFACE="${PYTHON_INTERFACE:-$ROOT_DIR/python_interface}"
CLIENTS="${CLIENTS:-4}"
FILES="${FILES:-6000}"
UNLINK_FILES="${UNLINK_FILES:-6000}"
FILE_SIZE="${FILE_SIZE:-2097152}"
WAIT_SEC="${WAIT_SEC:-45}"
FIO_SIZE="${FIO_SIZE:-2G}"
REQUIRE_NVME="${REQUIRE_NVME:-1}"
WRITE_THRESHOLD="${WRITE_THRESHOLD:-1}"
EVICT_THRESHOLD="${EVICT_THRESHOLD:-0.72}"
IDLE_WAIT_SEC="${IDLE_WAIT_SEC:-900}"
SCENARIO="${1:-all}"

usage() {
    cat <<EOF
Usage:
  $0 <scenario>

Scenarios:
  all    Run Python internal P-1/P-2/P-3/P-5 and fio B-1..B-5.
  python Run P-1, P-2, P-3 and P-5 only.
  fio    Run fio/local baseline B-1..B-5 only.
  P-1    Python internal 4-client write-only benchmark.
  P-2    Python internal 4-client unlink-only benchmark.
  P-3    Python internal 4-client write-triggered DiskCache evict benchmark.
  P-5    Python internal concurrent write + unlink benchmark.
  B-1    fio single-file direct write baseline.
  B-2    fio single-file direct read baseline.
  B-3    fio multi-file direct write baseline.
  B-4    fio multi-file direct read baseline.
  B-5    local multi-file delete baseline after fio create.

Environment overrides:
  BENCHMARK_ROOT=${BENCHMARK_ROOT:-}
  OUT_DIR=${OUT_DIR}
  CLIENTS=${CLIENTS}
  FILES=${FILES}
  UNLINK_FILES=${UNLINK_FILES}
  FILE_SIZE=${FILE_SIZE}
  WAIT_SEC=${WAIT_SEC}
  FIO_SIZE=${FIO_SIZE}
  FIO_DIR=${FIO_DIR}
  CACHE_ROOT=${CACHE_ROOT}
  REQUIRE_NVME=${REQUIRE_NVME}
  WRITE_THRESHOLD=${WRITE_THRESHOLD}
  EVICT_THRESHOLD=${EVICT_THRESHOLD}
  CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
  PYTHON_INTERFACE=${PYTHON_INTERFACE}

Example:
  BENCHMARK_ROOT=/data4/hxing CLIENTS=4 FILES=6000 UNLINK_FILES=6000 FIO_SIZE=2G $0 all
EOF
}

log() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

metadata_root() {
    (
        set +u
        source "$ROOT_DIR/deploy/meta/falcon_meta_config.sh" >/dev/null 2>&1
        printf '%s/metadata\n' "$workspace"
    )
}

safe_rm_rf() {
    local path="$1"
    case "$path" in
        ""|"/"|"/tmp"|"$HOME")
            echo "refuse to remove unsafe path: $path" >&2
            return 1
            ;;
    esac
    if [[ -n "${BENCHMARK_ROOT:-}" && "$path" == "$BENCHMARK_ROOT" ]]; then
        echo "refuse to remove BENCHMARK_ROOT itself: $path" >&2
        return 1
    fi
    sudo rm -rf "$path"
}

path_device_info() {
    local path="$1"
    local source fstype target
    target="$(findmnt -T "$path" -no TARGET 2>/dev/null || true)"
    source="$(findmnt -T "$path" -no SOURCE 2>/dev/null || true)"
    fstype="$(findmnt -T "$path" -no FSTYPE 2>/dev/null || true)"
    printf 'path=%s target=%s source=%s fstype=%s\n' "$path" "${target:-N/A}" "${source:-N/A}" "${fstype:-N/A}"
}

path_is_nvme() {
    local path="$1"
    local source
    source="$(findmnt -T "$path" -no SOURCE 2>/dev/null || true)"
    [[ -z "$source" ]] && return 1
    [[ "$source" == /dev/nvme* ]] && return 0
    lsblk -no NAME,PKNAME "$source" 2>/dev/null | grep -q 'nvme'
}

check_nvme_path() {
    local path="$1"
    local label="$2"
    mkdir -p "$path"
    log "storage check ${label}: $(path_device_info "$path")"
    if [[ "$REQUIRE_NVME" == "1" ]] && ! path_is_nvme "$path"; then
        echo "${label} is not on an NVMe device: $path" >&2
        echo "Set BENCHMARK_ROOT=/data4/hxing, or set REQUIRE_NVME=0 to allow non-NVMe testing." >&2
        exit 1
    fi
}

scenario_uses_falcon() {
    [[ "$SCENARIO" != "fio" && "$SCENARIO" != B-* ]]
}

write_storage_info() {
    local meta_root
    {
        echo "benchmark_root=${BENCHMARK_ROOT:-N/A}"
        path_device_info "$OUT_DIR"
        path_device_info "$FIO_DIR"
        if scenario_uses_falcon; then
            meta_root="$(metadata_root)"
            path_device_info "$CACHE_ROOT"
            path_device_info "$(dirname "$meta_root")"
            echo "metadata_root=$meta_root"
        else
            echo "cache_root=N/A"
            echo "metadata_root=N/A"
        fi
    } > "$OUT_DIR/storage_info.txt"
}

prepare_storage_targets() {
    local meta_root
    check_nvme_path "$OUT_DIR" "OUT_DIR"
    check_nvme_path "$FIO_DIR" "FIO_DIR"
    if scenario_uses_falcon; then
        meta_root="$(metadata_root)"
        check_nvme_path "$CACHE_ROOT" "CACHE_ROOT"
        check_nvme_path "$(dirname "$meta_root")" "metadata workspace"
    fi
    write_storage_info
}

cleanup_temp_dirs() {
    local meta_root
    rm -rf "$OUT_DIR"/work_* 2>/dev/null || true
    safe_rm_rf "$FIO_DIR" >/dev/null 2>&1 || true
    if scenario_uses_falcon; then
        meta_root="$(metadata_root)"
        safe_rm_rf "$CACHE_ROOT" >/dev/null 2>&1 || true
        safe_rm_rf "$meta_root" >/dev/null 2>&1 || true
    fi
}

on_exit() {
    stop_idle_server 2>/dev/null || true
    cleanup_temp_dirs 2>/dev/null || true
}

port_listeners() {
    local pattern="$1"
    local use_sudo="${2:-}"
    local ss_cmd=(ss -ltnp)
    if [[ "$use_sudo" == "sudo" ]]; then
        ss_cmd=(sudo ss -ltnp)
    fi

    if command -v rg >/dev/null 2>&1; then
        "${ss_cmd[@]}" | rg "$pattern" || true
    else
        "${ss_cmd[@]}" | grep -E "$pattern" || true
    fi
}

falcon_port_listeners() {
    port_listeners ':(55500|55510|55520|55530|56039)([[:space:]]|$)' sudo
}

idle_server_port_listeners() {
    port_listeners ':(56039)([[:space:]]|$)'
}

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing command: $1" >&2
        exit 1
    fi
}

source_falcon_env() {
    set +u
    source "$ROOT_DIR/deploy/falcon_env.sh"
    set -u
}

ensure_binary() {
    if [[ ! -x "$BUILD_DIR/internal_perf/falcon_internal_perf" ]]; then
        log "building falcon_internal_perf"
        ninja -C "$BUILD_DIR" falcon_internal_perf
    fi
}

release_ports() {
    for _ in $(seq 1 20); do
        if [[ -z "$(falcon_port_listeners)" ]]; then
            return 0
        fi
        sudo fuser -k 55500/tcp 55510/tcp 55520/tcp 55530/tcp 56039/tcp >/dev/null 2>&1 || true
        sleep 0.5
    done
    if [[ -n "$(falcon_port_listeners)" ]]; then
        echo "default Falcon ports are still busy:" >&2
        falcon_port_listeners >&2 || true
        exit 1
    fi
}

clean_runtime() {
    log "cleaning Falcon runtime"
    sudo "$ROOT_DIR/deploy/falcon_stop.sh" >/dev/null 2>&1 || true
    sudo umount -l "$MOUNT_DIR" >/dev/null 2>&1 || true
    release_ports
    local meta_root
    meta_root="$(metadata_root)"
    safe_rm_rf "$CACHE_ROOT" >/dev/null 2>&1 || true
    sudo rm -rf "$MOUNT_DIR"
    safe_rm_rf "$meta_root" >/dev/null 2>&1 || true
    sudo rm -f /tmp/.s.PGSQL.55500* /tmp/.s.PGSQL.55510* /tmp/.s.PGSQL.55520* /tmp/.s.PGSQL.55530*
}

prepare_cache_dirs() {
    mkdir -p "$CACHE_ROOT"
    for shard in $(seq 0 100); do
        mkdir -p "$CACHE_ROOT/$shard"
    done
}

start_meta() {
    local threshold="$1"
    log "starting Falcon meta services, STORAGE_THRESHOLD=${threshold}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        STORAGE_THRESHOLD="$threshold" bash "$ROOT_DIR/deploy/meta/falcon_meta_start.sh"
    ) >/tmp/pyfalcon_benchmark_meta.log 2>&1
    sleep 3
}

start_idle_server() {
    local threshold="$1"
    local case_id="$2"
    log "starting idle RemoteIOServer for ${case_id}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        STORAGE_THRESHOLD="$threshold" "$BUILD_DIR/internal_perf/falcon_internal_perf" \
            --mode idle_server \
            --dir "/py_idle_${case_id}" \
            --output "$OUT_DIR/python/${case_id}-idle.json" \
            --files 1 \
            --file-size 1 \
            --wait-sec "$IDLE_WAIT_SEC"
    ) >"$OUT_DIR/python/${case_id}-idle.log" 2>&1 &
    IDLE_PID=$!
    for _ in $(seq 1 30); do
        if [[ -n "$(idle_server_port_listeners)" ]]; then
            return 0
        fi
        sleep 1
    done
    echo "idle RemoteIOServer did not start for ${case_id}" >&2
    cat "$OUT_DIR/python/${case_id}-idle.log" >&2 || true
    kill "$IDLE_PID" 2>/dev/null || true
    exit 1
}

stop_idle_server() {
    if [[ -n "${IDLE_PID:-}" ]]; then
        kill "$IDLE_PID" 2>/dev/null || true
        wait "$IDLE_PID" 2>/dev/null || true
        IDLE_PID=""
    fi
}

run_fio_case() {
    local case_id="$1"
    local fio_files_per_job=$(( (FILES + CLIENTS - 1) / CLIENTS ))
    local fio_unlink_files_per_job=$(( (UNLINK_FILES + CLIENTS - 1) / CLIENTS ))
    require_cmd fio
    mkdir -p "$OUT_DIR/fio"
    case "$case_id" in
        B-1)
            log "running B-1: fio single-file direct write, jobs=${CLIENTS}, size=${FIO_SIZE}"
            rm -rf "$FIO_DIR/single_write"
            mkdir -p "$FIO_DIR/single_write"
            fio --output="$OUT_DIR/fio/B-1.json" --output-format=json \
                --name=pyfalcon_fio_single_write --directory="$FIO_DIR/single_write" \
                --nrfiles=1 --rw=write --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$CLIENTS" --direct=1 --group_reporting=1
            rm -rf "$FIO_DIR/single_write"
            ;;
        B-2)
            log "running B-2: fio single-file direct read, jobs=${CLIENTS}, size=${FIO_SIZE}"
            rm -rf "$FIO_DIR/single_read"
            mkdir -p "$FIO_DIR/single_read"
            fio --output="$OUT_DIR/fio/B-2-prepare.json" --output-format=json \
                --name=pyfalcon_fio_single_read_prepare --directory="$FIO_DIR/single_read" \
                --nrfiles=1 --rw=write --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$CLIENTS" --direct=1 --group_reporting=1
            fio --output="$OUT_DIR/fio/B-2.json" --output-format=json \
                --name=pyfalcon_fio_single_read --directory="$FIO_DIR/single_read" \
                --nrfiles=1 --rw=read --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$CLIENTS" --direct=1 --group_reporting=1
            rm -rf "$FIO_DIR/single_read"
            ;;
        B-3)
            log "running B-3: fio multi-file direct write, jobs=${CLIENTS}, files=${FILES}, file_size=${FILE_SIZE}"
            rm -rf "$FIO_DIR/multi"
            mkdir -p "$FIO_DIR/multi"
            fio --output="$OUT_DIR/fio/B-3.json" --output-format=json \
                --name=pyfalcon_fio_multi_write --directory="$FIO_DIR/multi" \
                --nrfiles="$fio_files_per_job" --filesize="$FILE_SIZE" \
                --rw=write --bs=2M --iodepth=1 --numjobs="$CLIENTS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0
            ;;
        B-4)
            log "running B-4: fio multi-file direct read, jobs=${CLIENTS}, files=${FILES}, file_size=${FILE_SIZE}"
            if [[ ! -d "$FIO_DIR/multi" ]] || [[ "$(find "$FIO_DIR/multi" -type f | wc -l)" -eq 0 ]]; then
                run_fio_case B-3
            fi
            fio --output="$OUT_DIR/fio/B-4.json" --output-format=json \
                --name=pyfalcon_fio_multi_read --directory="$FIO_DIR/multi" \
                --nrfiles="$fio_files_per_job" --filesize="$FILE_SIZE" \
                --rw=read --bs=2M --iodepth=1 --numjobs="$CLIENTS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0
            rm -rf "$FIO_DIR/multi"
            ;;
        B-5)
            log "running B-5: local multi-file delete baseline, jobs=${CLIENTS}, unlink_files=${UNLINK_FILES}, file_size=${FILE_SIZE}"
            rm -rf "$FIO_DIR/delete"
            mkdir -p "$FIO_DIR/delete"
            fio --output="$OUT_DIR/fio/B-5-create.json" --output-format=json \
                --name=pyfalcon_fio_delete_prepare --directory="$FIO_DIR/delete" \
                --nrfiles="$fio_unlink_files_per_job" --filesize="$FILE_SIZE" \
                --rw=write --bs=2M --iodepth=1 --numjobs="$CLIENTS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0
            local count_before start_ns end_ns elapsed_ns count_after
            count_before="$(find "$FIO_DIR/delete" -type f | wc -l)"
            start_ns="$(date +%s%N)"
            find "$FIO_DIR/delete" -type f -print0 | xargs -0 -n 100 -P "$CLIENTS" rm -f
            end_ns="$(date +%s%N)"
            count_after="$(find "$FIO_DIR/delete" -type f | wc -l)"
            elapsed_ns=$((end_ns - start_ns))
            python3 - "$OUT_DIR/fio/B-5.json" "$count_before" "$count_after" "$elapsed_ns" "$FILE_SIZE" <<'PY2'
import json
import sys
out, before, after, elapsed_ns, size = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
elapsed = elapsed_ns / 1_000_000_000
deleted = before - after
result = {
    "mode": "local_delete",
    "created_files": before,
    "remaining_files": after,
    "deleted_files": deleted,
    "file_size_bytes": size,
    "delete_elapsed_sec": elapsed,
    "delete_files_per_sec": deleted / elapsed if elapsed > 0 else 0.0,
    "delete_mib_per_sec": deleted * size / elapsed / 1048576.0 if elapsed > 0 else 0.0,
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, sort_keys=True)
    f.write("\n")
PY2
            rm -rf "$FIO_DIR/delete"
            ;;
        *) echo "unknown fio case: ${case_id}" >&2; exit 1 ;;
    esac
}

run_python_group() {
    run_case P-1 write_only "$WRITE_THRESHOLD"
    run_case P-2 unlink_only "$WRITE_THRESHOLD"
    run_case P-3 create_evict "$EVICT_THRESHOLD"
    run_case P-5 concurrent_unlink "$WRITE_THRESHOLD"
}

run_fio_group() {
    run_fio_case B-1
    run_fio_case B-2
    run_fio_case B-3
    run_fio_case B-4
    run_fio_case B-5
}

write_summary() {
    python3 "$ROOT_DIR/tools/pyfalcon_benchmark_summary.py" \
        --out-dir "$OUT_DIR" \
        --scenario "$SCENARIO" \
        --clients "$CLIENTS" \
        --files "$FILES" \
        --unlink-files "$UNLINK_FILES" \
        --file-size "$FILE_SIZE" \
        --wait-sec "$WAIT_SEC" \
        --fio-size "$FIO_SIZE"
}

run_case() {
    local case_id="$1"
    local mode="$2"
    local threshold="$3"
    ensure_binary
    clean_runtime
    prepare_cache_dirs
    mkdir -p "$OUT_DIR/python" "$OUT_DIR/work_${case_id}"
    start_meta "$threshold"
    start_idle_server "$threshold" "$case_id"
    log "running ${case_id}: mode=${mode}, clients=${CLIENTS}, files=${FILES}, file_size=${FILE_SIZE}"
    (
        cd "$ROOT_DIR"
        STORAGE_THRESHOLD="$threshold" python3 "$ROOT_DIR/tools/pyfalcon_client_perf.py" \
            --mode "$mode" \
            --clients "$CLIENTS" \
            --files "$FILES" \
            --unlink-files "$UNLINK_FILES" \
            --file-size "$FILE_SIZE" \
            --wait-sec "$WAIT_SEC" \
            --dir "/py_${case_id}" \
            --workspace "$OUT_DIR/work_${case_id}" \
            --config "$CONFIG_FILE_PATH" \
            --python-interface "$PYTHON_INTERFACE" \
            --output "$OUT_DIR/python/${case_id}.json"
    )
    stop_idle_server
}

run_group() {
    case "$1" in
        all)
            run_python_group
            run_fio_group
            ;;
        python) run_python_group ;;
        fio) run_fio_group ;;
        P-1) run_case P-1 write_only "$WRITE_THRESHOLD" ;;
        P-2) run_case P-2 unlink_only "$WRITE_THRESHOLD" ;;
        P-3) run_case P-3 create_evict "$EVICT_THRESHOLD" ;;
        P-5) run_case P-5 concurrent_unlink "$WRITE_THRESHOLD" ;;
        B-1|B-2|B-3|B-4|B-5) run_fio_case "$1" ;;
        help|-h|--help) usage ;;
        *) usage; exit 1 ;;
    esac
}

if [[ "$SCENARIO" == "help" || "$SCENARIO" == "-h" || "$SCENARIO" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$OUT_DIR/python" "$OUT_DIR/fio"
RUN_LOG="${RUN_LOG:-$OUT_DIR/run.log}"
: > "$RUN_LOG"
exec > >(tee -a "$RUN_LOG") 2>&1
trap on_exit EXIT
log "output directory: $OUT_DIR"
log "run log: $RUN_LOG"
prepare_storage_targets
run_group "$SCENARIO"
if scenario_uses_falcon; then
    clean_runtime
fi
cleanup_temp_dirs
write_summary
log "summary: $OUT_DIR/benchmark_summary.md"
log "summary log: $OUT_DIR/benchmark_summary.log"
log "done"
