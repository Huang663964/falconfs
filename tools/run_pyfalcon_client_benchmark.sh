#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
DEFAULT_BENCHMARK_ROOT="${DEFAULT_BENCHMARK_ROOT:-/data4/hxing}"
BENCHMARK_ROOT="${BENCHMARK_ROOT:-${PERF_ROOT:-}}"
if [[ -z "$BENCHMARK_ROOT" && -d "$DEFAULT_BENCHMARK_ROOT" && -w "$DEFAULT_BENCHMARK_ROOT" ]]; then
    BENCHMARK_ROOT="$DEFAULT_BENCHMARK_ROOT"
fi
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
BENCHMARK_CLUSTER_VIEW="${BENCHMARK_CLUSTER_VIEW:-127.0.0.1:56039}"
PYTHON_INTERFACE="${PYTHON_INTERFACE:-$ROOT_DIR/python_interface}"
CLIENTS="${CLIENTS:-4}"
FILES="${FILES:-6000}"
READ_FILES="${READ_FILES:-$FILES}"
PINNED_READ_FILES="${PINNED_READ_FILES:-256}"
SHARED_PINNED_READ_DIR="${SHARED_PINNED_READ_DIR:-1}"
UNLINK_FILES="${UNLINK_FILES:-6000}"
P5_UNLINK_FILES="${P5_UNLINK_FILES:-200000}"
P5_WRITE_FILES="${P5_WRITE_FILES:-$P5_UNLINK_FILES}"
P5_PREPARE_MAX_AVAIL_RATIO="${P5_PREPARE_MAX_AVAIL_RATIO:-0.45}"
FILE_SIZE="${FILE_SIZE:-2097152}"
WAIT_SEC="${WAIT_SEC:-45}"
MIXED_DURATION_SEC="${MIXED_DURATION_SEC:-120}"
HOT_READ_WINDOW="${HOT_READ_WINDOW:-1024}"
HOT_READ_LAG="${HOT_READ_LAG:-128}"
HOT_READ_MIN_FILES="${HOT_READ_MIN_FILES:-$HOT_READ_WINDOW}"
FIO_SIZE="${FIO_SIZE:-2G}"
FIO_LARGE_SIZE="${FIO_LARGE_SIZE:-10G}"
FIO_LARGE_RUNTIME="${FIO_LARGE_RUNTIME:-10}"
FIO_NUMJOBS_LIST="${FIO_NUMJOBS_LIST:-1 4 8 16}"
FIO_MATRIX_SIZE="${FIO_MATRIX_SIZE:-$FIO_SIZE}"
FIO_UNALIGNED_SIZE="${FIO_UNALIGNED_SIZE:-64M}"
REQUIRE_NVME="${REQUIRE_NVME:-1}"
WRITE_THRESHOLD="${WRITE_THRESHOLD:-1}"
EVICT_THRESHOLD="${EVICT_THRESHOLD:-0.72}"
AUTO_EVICT_CONFIG="${AUTO_EVICT_CONFIG:-1}"
AUTO_EVICT_WRITE_RATIO="${AUTO_EVICT_WRITE_RATIO:-0.01}"
AUTO_EVICT_MIN_WRITE_BYTES="${AUTO_EVICT_MIN_WRITE_BYTES:-12884901888}"
AUTO_EVICT_MAX_WRITE_BYTES="${AUTO_EVICT_MAX_WRITE_BYTES:-0}"
AUTO_EVICT_MAX_AVAIL_RATIO="${AUTO_EVICT_MAX_AVAIL_RATIO:-0.60}"
AUTO_EVICT_TRIGGER_RATIO="${AUTO_EVICT_TRIGGER_RATIO:-0.90}"
AUTO_EVICT_INIT_MARGIN_BYTES="${AUTO_EVICT_INIT_MARGIN_BYTES:-1073741824}"
AUTO_EVICT_START_MARGIN_RATIO="${AUTO_EVICT_START_MARGIN_RATIO:-0.12}"
AUTO_EVICT_MAX_THRESHOLD="${AUTO_EVICT_MAX_THRESHOLD:-0.98}"
IDLE_WAIT_SEC="${IDLE_WAIT_SEC:-900}"
P3_MONITOR="${P3_MONITOR:-1}"
P3_MONITOR_INTERVAL="${P3_MONITOR_INTERVAL:-30}"
P3_STALL_SECONDS="${P3_STALL_SECONDS:-180}"
KEEP_WORK_DIRS="${KEEP_WORK_DIRS:-1}"
MAX_LOCAL_DISK_SIZE="${MAX_LOCAL_DISK_SIZE:-16}"
FALCON_LOG_LEVEL="${FALCON_LOG_LEVEL:-}"
PREPARE_ENV="${PREPARE_ENV:-1}"
PREPARE_PG="${PREPARE_PG:-auto}"
PREPARE_FALCON="${PREPARE_FALCON:-1}"
PREPARE_INSTALL="${PREPARE_INSTALL:-1}"
PREPARE_INTERNAL_BENCH="${PREPARE_INTERNAL_BENCH:-1}"
PREPARE_CLEAN="${PREPARE_CLEAN:-0}"
PREPARE_FIX_BUILD_PERMS="${PREPARE_FIX_BUILD_PERMS:-1}"
CASE_SPACE_MARGIN_BYTES="${CASE_SPACE_MARGIN_BYTES:-1073741824}"
SCENARIO="${1:-all}"
FAILED_CASES=()

usage() {
    cat <<EOF
Usage:
  $0 <scenario>

Scenarios:
  all    Run Python internal P-LOCK/P-1/P-2/P-3/P-5/P-R1/P-RW/P-RWE/P-DIO and fio B-1..B-6 plus fio matrix/direct probes.
  python Run Python internal cases only.
  fio    Run fio/local baseline B-1..B-6, fio numjobs matrix, and fio direct-unaligned probe.
  P-LOCK Run DiskCache shared-cache process-lock regression test.
  P-1    Python internal 4-client write-only benchmark.
  P-2    Python internal 4-client unlink-only benchmark.
  P-3    Python internal 4-client write-triggered DiskCache evict benchmark.
  P-5    Python internal concurrent write + unlink benchmark.
  P-R1   Python internal read-only benchmark.
  P-RW   Python internal concurrent read + write benchmark without evict.
  P-RWE  Python internal concurrent read + write + DiskCache evict benchmark.
  pwe    Shortcut for P-RWE only.
  P-DIO  Python internal O_DIRECT alignment probe.
  B-1    fio single-file direct write baseline.
  B-2    fio single-file direct read baseline.
  B-3    fio multi-file direct write baseline.
  B-4    fio multi-file direct read baseline.
  B-5    local multi-file delete baseline after fio create.
  B-6    fio large-file 2MiB psync sequential write baseline.
  B-MATRIX fio numjobs matrix for write/read/rw, using FIO_NUMJOBS_LIST.
  B-DIO  fio direct I/O unaligned-write probe.

Environment overrides:
  DEFAULT_BENCHMARK_ROOT=${DEFAULT_BENCHMARK_ROOT}
  BENCHMARK_ROOT=${BENCHMARK_ROOT:-}
  OUT_DIR=${OUT_DIR}
  CLIENTS=${CLIENTS}
  FILES=${FILES}
  READ_FILES=${READ_FILES}
  PINNED_READ_FILES=${PINNED_READ_FILES}
  SHARED_PINNED_READ_DIR=${SHARED_PINNED_READ_DIR}
  UNLINK_FILES=${UNLINK_FILES}
  P5_UNLINK_FILES=${P5_UNLINK_FILES}
  P5_WRITE_FILES=${P5_WRITE_FILES}
  P5_PREPARE_MAX_AVAIL_RATIO=${P5_PREPARE_MAX_AVAIL_RATIO}
  FILE_SIZE=${FILE_SIZE}
  WAIT_SEC=${WAIT_SEC}
  MIXED_DURATION_SEC=${MIXED_DURATION_SEC}
  HOT_READ_WINDOW=${HOT_READ_WINDOW}
  HOT_READ_LAG=${HOT_READ_LAG}
  HOT_READ_MIN_FILES=${HOT_READ_MIN_FILES}
  FIO_SIZE=${FIO_SIZE}
  FIO_LARGE_SIZE=${FIO_LARGE_SIZE}
  FIO_LARGE_RUNTIME=${FIO_LARGE_RUNTIME}
  FIO_NUMJOBS_LIST=${FIO_NUMJOBS_LIST}
  FIO_MATRIX_SIZE=${FIO_MATRIX_SIZE}
  FIO_UNALIGNED_SIZE=${FIO_UNALIGNED_SIZE}
  FIO_DIR=${FIO_DIR}
  CACHE_ROOT=${CACHE_ROOT}
  REQUIRE_NVME=${REQUIRE_NVME}
  WRITE_THRESHOLD=${WRITE_THRESHOLD}
  EVICT_THRESHOLD=${EVICT_THRESHOLD}
  AUTO_EVICT_CONFIG=${AUTO_EVICT_CONFIG}
  AUTO_EVICT_WRITE_RATIO=${AUTO_EVICT_WRITE_RATIO}
  AUTO_EVICT_MIN_WRITE_BYTES=${AUTO_EVICT_MIN_WRITE_BYTES}
  AUTO_EVICT_MAX_WRITE_BYTES=${AUTO_EVICT_MAX_WRITE_BYTES}
  AUTO_EVICT_MAX_AVAIL_RATIO=${AUTO_EVICT_MAX_AVAIL_RATIO}
  AUTO_EVICT_TRIGGER_RATIO=${AUTO_EVICT_TRIGGER_RATIO}
  AUTO_EVICT_INIT_MARGIN_BYTES=${AUTO_EVICT_INIT_MARGIN_BYTES}
  AUTO_EVICT_START_MARGIN_RATIO=${AUTO_EVICT_START_MARGIN_RATIO}
  AUTO_EVICT_MAX_THRESHOLD=${AUTO_EVICT_MAX_THRESHOLD}
  P3_MONITOR=${P3_MONITOR}
  P3_MONITOR_INTERVAL=${P3_MONITOR_INTERVAL}
  P3_STALL_SECONDS=${P3_STALL_SECONDS}
  MAX_LOCAL_DISK_SIZE=${MAX_LOCAL_DISK_SIZE}
  FALCON_LOG_LEVEL=${FALCON_LOG_LEVEL}
  PREPARE_ENV=${PREPARE_ENV}
  PREPARE_PG=${PREPARE_PG}
  PREPARE_FALCON=${PREPARE_FALCON}
  PREPARE_INSTALL=${PREPARE_INSTALL}
  PREPARE_INTERNAL_BENCH=${PREPARE_INTERNAL_BENCH}
  PREPARE_CLEAN=${PREPARE_CLEAN}
  PREPARE_FIX_BUILD_PERMS=${PREPARE_FIX_BUILD_PERMS}
  CASE_SPACE_MARGIN_BYTES=${CASE_SPACE_MARGIN_BYTES}
  CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
  BENCHMARK_CLUSTER_VIEW=${BENCHMARK_CLUSTER_VIEW}
  PYTHON_INTERFACE=${PYTHON_INTERFACE}

Example:
  BENCHMARK_ROOT=/data4/hxing CLIENTS=4 FILES=6000 UNLINK_FILES=6000 FIO_SIZE=2G FIO_LARGE_SIZE=10G FIO_LARGE_RUNTIME=10 $0 all
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
    [[ "$SCENARIO" != "fio" && "$SCENARIO" != "fio-matrix" && "$SCENARIO" != B-* ]]
}

write_storage_info() {
    local meta_root
    {
        echo "benchmark_root=${BENCHMARK_ROOT:-N/A}"
        echo "auto_evict_config=$AUTO_EVICT_CONFIG"
        echo "max_local_disk_size_gib=$MAX_LOCAL_DISK_SIZE"
        echo "p5_unlink_files=$P5_UNLINK_FILES"
        echo "p5_write_files=$P5_WRITE_FILES"
        echo "p5_prepare_max_avail_ratio=$P5_PREPARE_MAX_AVAIL_RATIO"
        echo "read_files=$READ_FILES"
        echo "pinned_read_files=$PINNED_READ_FILES"
        echo "shared_pinned_read_dir=$SHARED_PINNED_READ_DIR"
        echo "mixed_duration_sec=$MIXED_DURATION_SEC"
        echo "hot_read_window=$HOT_READ_WINDOW"
        echo "hot_read_lag=$HOT_READ_LAG"
        echo "hot_read_min_files=$HOT_READ_MIN_FILES"
        echo "fio_numjobs_list=$FIO_NUMJOBS_LIST"
        echo "fio_matrix_size=$FIO_MATRIX_SIZE"
        echo "fio_unaligned_size=$FIO_UNALIGNED_SIZE"
        echo "auto_evict_write_ratio=$AUTO_EVICT_WRITE_RATIO"
        echo "auto_evict_min_write_bytes=$AUTO_EVICT_MIN_WRITE_BYTES"
        echo "auto_evict_max_write_bytes=$AUTO_EVICT_MAX_WRITE_BYTES"
        echo "auto_evict_trigger_ratio=$AUTO_EVICT_TRIGGER_RATIO"
        echo "auto_evict_start_margin_ratio=$AUTO_EVICT_START_MARGIN_RATIO"
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
    if [[ "$KEEP_WORK_DIRS" != "1" ]]; then
        rm -rf "$OUT_DIR"/work_* 2>/dev/null || true
    fi
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

ensure_pg_path() {
    if [[ -x /usr/local/pgsql/bin/pg_config ]]; then
        export PATH="/usr/local/pgsql/bin:$PATH"
        export LD_LIBRARY_PATH="/usr/local/pgsql/lib:${LD_LIBRARY_PATH:-}"
    fi
}

have_pg_tools() {
    command -v pg_config >/dev/null 2>&1 &&
        command -v pg_ctl >/dev/null 2>&1 &&
        command -v psql >/dev/null 2>&1
}

prepare_postgres() {
    ensure_pg_path
    if have_pg_tools && [[ "$PREPARE_PG" != "1" ]]; then
        return 0
    fi
    if [[ "$PREPARE_PG" == "0" ]]; then
        have_pg_tools || {
            echo "missing PostgreSQL tools: pg_config/pg_ctl/psql" >&2
            echo "Set PREPARE_PG=auto or add /usr/local/pgsql/bin to PATH." >&2
            return 1
        }
        return 0
    fi

    log "preparing PostgreSQL under /usr/local/pgsql"
    (
        cd "$ROOT_DIR/third_party/postgres"
        if [[ ! -f Makefile ]]; then
            ./configure --prefix=/usr/local/pgsql --without-icu --enable-debug
        fi
        make -j"$(nproc)"
        sudo make install
    )
    ensure_pg_path
    have_pg_tools || {
        echo "PostgreSQL prepare finished but pg_config/pg_ctl/psql are still unavailable" >&2
        return 1
    }
}

ensure_build_dir_writable() {
    [[ -e "$BUILD_DIR" ]] || return 0

    local abs_build abs_root needs_fix=0
    abs_build="$(readlink -f "$BUILD_DIR")"
    abs_root="$(readlink -f "$ROOT_DIR")"
    case "$abs_build" in
        "$abs_root"/*) ;;
        *)
            echo "BUILD_DIR is outside repo, refuse to change permissions: $abs_build" >&2
            return 1
            ;;
    esac

    [[ -w "$abs_build" ]] || needs_fix=1
    for file in "$abs_build/.ninja_deps" "$abs_build/.ninja_log"; do
        [[ ! -e "$file" || -w "$file" ]] || needs_fix=1
    done
    if [[ "$needs_fix" == "0" ]]; then
        return 0
    fi
    if [[ "$PREPARE_FIX_BUILD_PERMS" != "1" ]]; then
        echo "build directory is not writable by current user: $abs_build" >&2
        echo "Run: sudo chown -R $(id -u):$(id -g) '$abs_build'" >&2
        echo "Or set PREPARE_CLEAN=1 after fixing permissions." >&2
        return 1
    fi

    log "fixing build directory permissions: $abs_build"
    sudo chown -R "$(id -u):$(id -g)" "$abs_build"
}

prepare_benchmark_env() {
    [[ "$PREPARE_ENV" == "1" ]] || return 0
    require_cmd python3
    require_cmd awk
    require_cmd sed
    require_cmd findmnt
    require_cmd df
    require_cmd ss

    if ! scenario_uses_falcon; then
        return 0
    fi

    prepare_postgres
    ensure_build_dir_writable
    if [[ "$PREPARE_CLEAN" == "1" ]]; then
        log "cleaning Falcon build"
        "$ROOT_DIR/build.sh" clean falcon
    fi
    if [[ "$PREPARE_FALCON" == "1" ]]; then
        log "building Falcon"
        "$ROOT_DIR/build.sh" build falcon
    fi
    if [[ "$PREPARE_INSTALL" == "1" ]]; then
        log "installing Falcon"
        sudo "$ROOT_DIR/build.sh" install falcon
    fi
    if [[ "$PREPARE_INTERNAL_BENCH" == "1" ]]; then
        log "building falcon_internal_perf"
        ninja -C "$BUILD_DIR" falcon_internal_perf
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
        return 1
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
    local case_id="${2:-meta}"
    local meta_log="$OUT_DIR/python/${case_id}-meta.log"
    local meta_root
    meta_root="$(metadata_root 2>/dev/null || true)"
    log "starting Falcon meta services, STORAGE_THRESHOLD=${threshold}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        STORAGE_THRESHOLD="$threshold" bash "$ROOT_DIR/deploy/meta/falcon_meta_start.sh"
    ) >"$meta_log" 2>&1 || {
        echo "failed to start Falcon meta services" >&2
        cat "$meta_log" >&2 || true
        return 1
    }
    sleep 3
}

create_case_config() {
    local case_id="$1"
    local case_config="$OUT_DIR/python/${case_id}-config.json"
    mkdir -p "$OUT_DIR/python"
    BASE_CONFIG="$CONFIG_FILE_PATH" \
    CASE_CONFIG="$case_config" \
    CACHE_ROOT_VALUE="$CACHE_ROOT" \
    BENCHMARK_CLUSTER_VIEW_VALUE="$BENCHMARK_CLUSTER_VIEW" \
    CASE_LOG_DIR="$OUT_DIR/python/${case_id}-falcon-log" \
    MAX_LOCAL_DISK_SIZE_VALUE="$MAX_LOCAL_DISK_SIZE" \
    FALCON_LOG_LEVEL_VALUE="$FALCON_LOG_LEVEL" \
    python3 - <<'PYCONFIG'
import json
import os

base = os.environ["BASE_CONFIG"]
out = os.environ["CASE_CONFIG"]
cluster_view = [item.strip() for item in os.environ["BENCHMARK_CLUSTER_VIEW_VALUE"].split(",") if item.strip()]
if not cluster_view:
    raise SystemExit("BENCHMARK_CLUSTER_VIEW cannot be empty")
with open(base, "r", encoding="utf-8") as src:
    config = json.load(src)
main = config.setdefault("main", {})
main["falcon_cache_root"] = os.environ["CACHE_ROOT_VALUE"]
main["falcon_cluster_view"] = cluster_view
main["falcon_log_dir"] = os.environ["CASE_LOG_DIR"]
max_local_disk_size = os.environ.get("MAX_LOCAL_DISK_SIZE_VALUE", "")
if max_local_disk_size:
    main["max_local_disk_size"] = int(max_local_disk_size)
falcon_log_level = os.environ.get("FALCON_LOG_LEVEL_VALUE", "")
if falcon_log_level:
    main["falcon_log_level"] = falcon_log_level
os.makedirs(os.path.dirname(out), exist_ok=True)
os.makedirs(main["falcon_log_dir"], exist_ok=True)
with open(out, "w", encoding="utf-8") as dst:
    json.dump(config, dst, indent=4, ensure_ascii=False)
    dst.write("\n")
PYCONFIG
    printf "%s\n" "$case_config"
}

start_idle_server() {
    local threshold="$1"
    local case_id="$2"
    local case_config="$3"
    log "starting idle RemoteIOServer for ${case_id}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        CONFIG_FILE="$case_config" STORAGE_THRESHOLD="$threshold" "$BUILD_DIR/internal_perf/falcon_internal_perf" \
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
    wait "$IDLE_PID" 2>/dev/null || true
    IDLE_PID=""
    return 1
}

stop_idle_server() {
    if [[ -n "${IDLE_PID:-}" ]]; then
        kill "$IDLE_PID" 2>/dev/null || true
        wait "$IDLE_PID" 2>/dev/null || true
        IDLE_PID=""
    fi
}

bytes_to_mib() {
    awk -v bytes="$1" 'BEGIN { printf "%.2f", bytes / 1048576.0 }'
}

collect_proc_tree() {
    local root="$1"
    local child
    [[ -d "/proc/$root" ]] || return 0
    printf '%s\n' "$root"
    for child in $(ps -o pid= --ppid "$root" 2>/dev/null || true); do
        collect_proc_tree "$child"
    done
}

sum_proc_io_field() {
    local field="$1"
    shift
    local pid value total=0
    for pid in "$@"; do
        [[ -r "/proc/$pid/io" ]] || continue
        value="$(awk -v key="${field}:" '$1 == key {print $2}' "/proc/$pid/io" 2>/dev/null || true)"
        [[ -n "$value" ]] || value=0
        total=$((total + value))
    done
    printf '%s\n' "$total"
}

cache_size_bytes() {
    if [[ -d "$CACHE_ROOT" ]]; then
        du -sb "$CACHE_ROOT" 2>/dev/null | awk '{print $1}'
    else
        printf '0\n'
    fi
}

print_case_monitor_sample() {
    local case_id="$1"
    local root_pid="$2"
    local case_log="$3"
    local monitor_log="$4"
    local last_progress_value="$5"
    local last_change_ts="$6"
    local now elapsed_since_change read_bytes write_bytes syscr syscw cache_bytes cache_mib progress_value
    local pid state wchan cmd
    local pids=()
    now="$(date +%s)"
    mapfile -t pids < <(collect_proc_tree "$root_pid")
    if (( ${#pids[@]} == 0 )); then
        return 1
    fi

    read_bytes="$(sum_proc_io_field read_bytes "${pids[@]}")"
    write_bytes="$(sum_proc_io_field write_bytes "${pids[@]}")"
    syscr="$(sum_proc_io_field syscr "${pids[@]}")"
    syscw="$(sum_proc_io_field syscw "${pids[@]}")"
    cache_bytes="$(cache_size_bytes)"
    cache_mib="$(bytes_to_mib "$cache_bytes")"

    progress_value="${write_bytes}:${syscw}:${cache_bytes}"
    if [[ "$progress_value" != "$last_progress_value" ]]; then
        last_change_ts="$now"
    fi
    elapsed_since_change=$((now - last_change_ts))

    {
        printf '[%s] monitor %s: pids=%s read_bytes=%s write_bytes=%s syscr=%s syscw=%s cache=%sMiB unchanged_write_sec=%s\n' \
            "$(date '+%F %T')" "$case_id" "${pids[*]}" "$read_bytes" "$write_bytes" "$syscr" "$syscw" "$cache_mib" "$elapsed_since_change"
        for pid in "${pids[@]}"; do
            [[ -d "/proc/$pid" ]] || continue
            state="$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)"
            wchan="$(cat "/proc/$pid/wchan" 2>/dev/null || true)"
            cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | sed 's/[[:space:]]*$//')"
            printf '  pid=%s state=%s wchan=%s cmd=%s\n' "$pid" "${state:-N/A}" "${wchan:-N/A}" "${cmd:-N/A}"
        done
        if (( elapsed_since_change >= P3_STALL_SECONDS )); then
            printf '  possible stall: write_bytes/syscw/cache size have not changed for %ss, inspect %s and Falcon logs under %s/python/%s-falcon-log\n' \
                "$elapsed_since_change" "$case_log" "$OUT_DIR" "$case_id"
        fi
        if [[ -f "$case_log" ]]; then
            printf '  latest benchmark log:\n'
            tail -n 5 "$case_log" | sed 's/^/    /'
        fi
    } | tee -a "$monitor_log"

    P3_MONITOR_LAST_PROGRESS_VALUE="$progress_value"
    P3_MONITOR_LAST_CHANGE_TS="$last_change_ts"
    return 0
}

monitor_p3_case() {
    local case_id="$1"
    local root_pid="$2"
    local case_log="$3"
    local monitor_log="$4"
    P3_MONITOR_LAST_PROGRESS_VALUE="-1"
    P3_MONITOR_LAST_CHANGE_TS="$(date +%s)"
    : > "$monitor_log"
    log "P-3 monitor enabled: interval=${P3_MONITOR_INTERVAL}s, stall_seconds=${P3_STALL_SECONDS}, log=${monitor_log}"
    while [[ -d "/proc/$root_pid" ]]; do
        print_case_monitor_sample "$case_id" "$root_pid" "$case_log" "$monitor_log" \
            "$P3_MONITOR_LAST_PROGRESS_VALUE" "$P3_MONITOR_LAST_CHANGE_TS" || break
        sleep "$P3_MONITOR_INTERVAL"
    done
}

configure_auto_evict_case() {
    local current_threshold="$1"
    local total used avail inodes_total inodes_used inodes_avail
    local plan_file="$OUT_DIR/evict_config.txt"
    local plan_error_file="$OUT_DIR/evict_config.err"

    if [[ "$AUTO_EVICT_CONFIG" != "1" ]]; then
        AUTO_EVICT_CASE_THRESHOLD="$current_threshold"
        AUTO_EVICT_CASE_FILES="$FILES"
        {
            echo "auto_evict_config=0"
            echo "threshold=$AUTO_EVICT_CASE_THRESHOLD"
            echo "files=$AUTO_EVICT_CASE_FILES"
            echo "reason=disabled"
        } > "$plan_file"
        log "auto evict disabled: threshold=${AUTO_EVICT_CASE_THRESHOLD}, files=${AUTO_EVICT_CASE_FILES}"
        return 0
    fi

    mkdir -p "$CACHE_ROOT"
    read -r total used avail < <(df -B1 --output=size,used,avail "$CACHE_ROOT" | awk 'NR==2 {print $1, $2, $3}')
    read -r inodes_total inodes_used inodes_avail < <(df -Pi "$CACHE_ROOT" | awk 'NR==2 {print $2, $3, $4}')
    if [[ -z "${total:-}" || -z "${used:-}" || -z "${avail:-}" || -z "${inodes_total:-}" || -z "${inodes_used:-}" || -z "${inodes_avail:-}" ]]; then
        echo "failed to read filesystem space for CACHE_ROOT=$CACHE_ROOT" >&2
        {
            echo "auto_evict_config=1"
            echo "status=failed"
            echo "reason=failed to read filesystem space for CACHE_ROOT=$CACHE_ROOT"
        } > "$plan_file"
        return 1
    fi

    local plan
    plan="$(
        TOTAL_BYTES="$total" \
        USED_BYTES="$used" \
        AVAIL_BYTES="$avail" \
        INODES_TOTAL="$inodes_total" \
        INODES_USED="$inodes_used" \
        INODES_AVAIL="$inodes_avail" \
        FILES_VALUE="$FILES" \
        FILE_SIZE_VALUE="$FILE_SIZE" \
        AUTO_EVICT_WRITE_RATIO_VALUE="$AUTO_EVICT_WRITE_RATIO" \
        AUTO_EVICT_MIN_WRITE_BYTES_VALUE="$AUTO_EVICT_MIN_WRITE_BYTES" \
        AUTO_EVICT_MAX_WRITE_BYTES_VALUE="$AUTO_EVICT_MAX_WRITE_BYTES" \
        AUTO_EVICT_MAX_AVAIL_RATIO_VALUE="$AUTO_EVICT_MAX_AVAIL_RATIO" \
        AUTO_EVICT_TRIGGER_RATIO_VALUE="$AUTO_EVICT_TRIGGER_RATIO" \
        AUTO_EVICT_INIT_MARGIN_BYTES_VALUE="$AUTO_EVICT_INIT_MARGIN_BYTES" \
        AUTO_EVICT_START_MARGIN_RATIO_VALUE="$AUTO_EVICT_START_MARGIN_RATIO" \
        AUTO_EVICT_MAX_THRESHOLD_VALUE="$AUTO_EVICT_MAX_THRESHOLD" \
        python3 - 2>"$plan_error_file" <<'PYAUTO'
import math
import os

total = int(os.environ["TOTAL_BYTES"])
used = int(os.environ["USED_BYTES"])
avail = int(os.environ["AVAIL_BYTES"])
inodes_total = int(os.environ["INODES_TOTAL"])
inodes_used = int(os.environ["INODES_USED"])
inodes_avail = int(os.environ["INODES_AVAIL"])
files = int(os.environ["FILES_VALUE"])
file_size = int(os.environ["FILE_SIZE_VALUE"])
write_ratio = float(os.environ["AUTO_EVICT_WRITE_RATIO_VALUE"])
min_write = int(os.environ["AUTO_EVICT_MIN_WRITE_BYTES_VALUE"])
max_write = int(os.environ["AUTO_EVICT_MAX_WRITE_BYTES_VALUE"])
max_avail_ratio = float(os.environ["AUTO_EVICT_MAX_AVAIL_RATIO_VALUE"])
trigger_ratio = float(os.environ["AUTO_EVICT_TRIGGER_RATIO_VALUE"])
init_margin = int(os.environ["AUTO_EVICT_INIT_MARGIN_BYTES_VALUE"])
start_margin_ratio = float(os.environ["AUTO_EVICT_START_MARGIN_RATIO_VALUE"])
max_threshold = float(os.environ["AUTO_EVICT_MAX_THRESHOLD_VALUE"])

if total <= 0 or file_size <= 0 or inodes_total <= 0:
    raise SystemExit("invalid total bytes, inode count, or file size")
if not (0.0 < trigger_ratio < 1.0):
    raise SystemExit("AUTO_EVICT_TRIGGER_RATIO must be in (0, 1)")
if not (0.0 < max_avail_ratio <= 1.0):
    raise SystemExit("AUTO_EVICT_MAX_AVAIL_RATIO must be in (0, 1]")
if not (0.0 < max_threshold < 1.0):
    raise SystemExit("AUTO_EVICT_MAX_THRESHOLD must be in (0, 1)")
if not (0.0 < start_margin_ratio < 1.0):
    raise SystemExit("AUTO_EVICT_START_MARGIN_RATIO must be in (0, 1)")

requested_write = files * file_size
ratio_target = int(total * write_ratio)
soft_target = max(ratio_target, min_write)
if max_write > 0:
    soft_target = min(soft_target, max_write)

desired_write = max(requested_write, soft_target)
used_ratio = used / total
block_avail_ratio = avail / total
block_start_used_ratio = 1.0 - block_avail_ratio
inode_avail_ratio = inodes_avail / inodes_total
inode_used_ratio = inodes_used / inodes_total
inode_start_used_ratio = 1.0 - inode_avail_ratio
startup_used_ratio = max(block_start_used_ratio, inode_start_used_ratio)
minimum_start_threshold = startup_used_ratio + 0.100001
if minimum_start_threshold >= max_threshold:
    raise SystemExit(
        "auto evict cannot satisfy Falcon startup check: max(block_start_used_ratio, inode_start_used_ratio)=%.6f, "
        "minimum threshold is %.6f, but AUTO_EVICT_MAX_THRESHOLD is %.6f"
        % (startup_used_ratio, minimum_start_threshold, max_threshold)
    )
start_used_bytes = int(total * startup_used_ratio)
start_floor_ratio = min(max_threshold, startup_used_ratio + start_margin_ratio)
start_floor_bytes = int(total * start_floor_ratio)
required_to_cross_start_floor = max(0, start_floor_bytes - start_used_bytes)
required_write_for_threshold = int(math.ceil(required_to_cross_start_floor / trigger_ratio)) if required_to_cross_start_floor > 0 else 0
desired_write = max(desired_write, required_write_for_threshold)

if max_write > 0 and desired_write > max_write:
    raise SystemExit(
        "auto evict needs %.2f MiB to cross startup-safe threshold, but AUTO_EVICT_MAX_WRITE_BYTES allows only %.2f MiB; "
        "increase AUTO_EVICT_MAX_WRITE_BYTES or reduce AUTO_EVICT_START_MARGIN_RATIO"
        % (desired_write / 1048576.0, max_write / 1048576.0)
    )

safe_auto_write = int(avail * max_avail_ratio)
if safe_auto_write > 0 and desired_write > safe_auto_write:
    raise SystemExit(
        "auto evict needs %.2f MiB to cross startup-safe threshold, but safe limit is %.2f MiB; "
        "increase free space, reduce AUTO_EVICT_START_MARGIN_RATIO, or increase AUTO_EVICT_MAX_AVAIL_RATIO"
        % (desired_write / 1048576.0, safe_auto_write / 1048576.0)
    )

auto_files = max(files, int(math.ceil(desired_write / file_size)))
write_bytes = auto_files * file_size
margin = max(init_margin, file_size * 4)
low = max(start_used_bytes + margin, start_floor_bytes)
high = start_used_bytes + write_bytes - margin
if high <= low:
    needed = int(math.ceil(((low - start_used_bytes) + margin) / file_size))
    auto_files = max(auto_files, needed)
    write_bytes = auto_files * file_size
    high = start_used_bytes + write_bytes - margin
if high <= low:
    raise SystemExit("not enough write bytes to place an evict threshold safely")

threshold_bytes = start_used_bytes + int(write_bytes * trigger_ratio)
threshold_bytes = max(threshold_bytes, low)
threshold_bytes = min(threshold_bytes, high)
max_threshold_bytes = int(total * max_threshold)
if threshold_bytes > max_threshold_bytes:
    threshold_bytes = max_threshold_bytes
if threshold_bytes <= start_used_bytes or threshold_bytes >= start_used_bytes + write_bytes:
    raise SystemExit("cannot compute threshold between current usage and planned final usage")

threshold = threshold_bytes / total
final_ratio = (start_used_bytes + write_bytes) / total
write_ratio_actual = write_bytes / total
print(f"threshold={threshold:.6f}")
print(f"files={auto_files}")
print(f"total_bytes={total}")
print(f"used_bytes={used}")
print(f"avail_bytes={avail}")
print(f"inodes_total={inodes_total}")
print(f"inodes_used={inodes_used}")
print(f"inodes_avail={inodes_avail}")
print(f"write_bytes={write_bytes}")
print(f"requested_files={files}")
print(f"requested_write_bytes={requested_write}")
print(f"used_ratio={used_ratio:.6f}")
print(f"block_avail_ratio={block_avail_ratio:.6f}")
print(f"block_start_used_ratio={block_start_used_ratio:.6f}")
print(f"inode_used_ratio={inode_used_ratio:.6f}")
print(f"inode_avail_ratio={inode_avail_ratio:.6f}")
print(f"inode_start_used_ratio={inode_start_used_ratio:.6f}")
print(f"startup_used_ratio={startup_used_ratio:.6f}")
print(f"start_used_bytes={start_used_bytes}")
print(f"minimum_start_threshold={minimum_start_threshold:.6f}")
print(f"final_ratio={final_ratio:.6f}")
print(f"write_ratio={write_ratio_actual:.6f}")
print(f"trigger_ratio={trigger_ratio:.6f}")
print(f"start_margin_ratio={start_margin_ratio:.6f}")
print(f"start_floor_ratio={start_floor_ratio:.6f}")
print(f"required_write_for_threshold_bytes={required_write_for_threshold}")
PYAUTO
    )" || {
        {
            echo "auto_evict_config=1"
            echo "status=failed"
            echo "cache_root=$CACHE_ROOT"
            echo "total_bytes=$total"
            echo "used_bytes=$used"
            echo "avail_bytes=$avail"
            echo "inodes_total=$inodes_total"
            echo "inodes_used=$inodes_used"
            echo "inodes_avail=$inodes_avail"
            printf 'reason='
            tr '\n' ' ' < "$plan_error_file"
            printf '\n'
        } > "$plan_file"
        cat "$plan_error_file" >&2 || true
        return 1
    }

    AUTO_EVICT_CASE_THRESHOLD=""
    AUTO_EVICT_CASE_FILES=""
    while IFS='=' read -r key value; do
        case "$key" in
            threshold) AUTO_EVICT_CASE_THRESHOLD="$value" ;;
            files) AUTO_EVICT_CASE_FILES="$value" ;;
        esac
    done <<< "$plan"

    if [[ -z "$AUTO_EVICT_CASE_THRESHOLD" || -z "$AUTO_EVICT_CASE_FILES" ]]; then
        echo "invalid auto evict plan:" >&2
        echo "$plan" >&2
        return 1
    fi

    {
        echo "auto_evict_config=1"
        echo "$plan"
        echo "cache_root=$CACHE_ROOT"
    } > "$plan_file"

    log "auto evict plan: threshold=${AUTO_EVICT_CASE_THRESHOLD}, files=${AUTO_EVICT_CASE_FILES}, write=$(bytes_to_mib $((AUTO_EVICT_CASE_FILES * FILE_SIZE)))MiB, cache_fs=$(path_device_info "$CACHE_ROOT")"
    if (( AUTO_EVICT_CASE_FILES > FILES )); then
        log "auto evict increased P-3 files from ${FILES} to ${AUTO_EVICT_CASE_FILES} to cross the evict threshold on this filesystem"
    fi
    return 0
}

planned_case_write_bytes() {
    local mode="$1"
    local case_files="$2"
    case "$mode" in
        write_only)
            echo $((case_files * FILE_SIZE))
            ;;
        read_only)
            echo $((READ_FILES * FILE_SIZE))
            ;;
        unlink_only)
            echo $((UNLINK_FILES * FILE_SIZE))
            ;;
        concurrent_unlink)
            local case_unlink_files="${3:-$UNLINK_FILES}"
            echo $(((case_unlink_files + case_files) * FILE_SIZE))
            ;;
        read_write)
            echo $(((PINNED_READ_FILES * CLIENTS + case_files) * FILE_SIZE))
            ;;
        direct_unaligned)
            echo $((4 * FILE_SIZE))
            ;;
        *)
            echo 0
            ;;
    esac
}

check_case_space() {
    local case_id="$1"
    local mode="$2"
    local case_files="$3"
    local case_unlink_files="${4:-$UNLINK_FILES}"
    local write_bytes avail_bytes required_bytes
    write_bytes="$(planned_case_write_bytes "$mode" "$case_files" "$case_unlink_files")"
    if (( write_bytes <= 0 )); then
        return 0
    fi
    mkdir -p "$CACHE_ROOT"
    avail_bytes="$(df -B1 --output=avail "$CACHE_ROOT" | awk 'NR==2 {print $1}')"
    required_bytes=$((write_bytes + CASE_SPACE_MARGIN_BYTES))
    log "case ${case_id} space plan: write=$(bytes_to_mib "$write_bytes")MiB, margin=$(bytes_to_mib "$CASE_SPACE_MARGIN_BYTES")MiB, avail=$(bytes_to_mib "$avail_bytes")MiB"
    if (( required_bytes > avail_bytes )); then
        echo "case ${case_id} needs about $(bytes_to_mib "$required_bytes")MiB free under CACHE_ROOT=$CACHE_ROOT, but only $(bytes_to_mib "$avail_bytes")MiB is available" >&2
        echo "Use BENCHMARK_ROOT on a larger filesystem, reduce FILES/READ_FILES/UNLINK_FILES, or lower CASE_SPACE_MARGIN_BYTES." >&2
        return 1
    fi
}

configure_p5_case() {
    P5_CASE_UNLINK_FILES="$P5_UNLINK_FILES"
    P5_CASE_WRITE_FILES="$P5_WRITE_FILES"
    if [[ "$P5_PREPARE_MAX_AVAIL_RATIO" == "0" || "$P5_PREPARE_MAX_AVAIL_RATIO" == "0.0" ]]; then
        log "P-5 prepare cap disabled: unlink_files=${P5_CASE_UNLINK_FILES}, write_files=${P5_CASE_WRITE_FILES}"
        return 0
    fi

    mkdir -p "$CACHE_ROOT"
    local avail max_prepare_bytes max_prepare_files
    avail="$(df -B1 --output=avail "$CACHE_ROOT" | awk 'NR==2 {print $1}')"
    max_prepare_bytes="$(awk -v avail="$avail" -v ratio="$P5_PREPARE_MAX_AVAIL_RATIO" 'BEGIN { printf "%d", avail * ratio }')"
    max_prepare_files=$((max_prepare_bytes / FILE_SIZE))
    if (( max_prepare_files < 1 )); then
        echo "P-5 cannot prepare delete dataset: avail=$(bytes_to_mib "$avail")MiB, ratio=${P5_PREPARE_MAX_AVAIL_RATIO}, file_size=${FILE_SIZE}" >&2
        return 1
    fi
    if (( P5_CASE_UNLINK_FILES > max_prepare_files )); then
        log "P-5 reduced unlink prepare files from ${P5_CASE_UNLINK_FILES} to ${max_prepare_files} to stay within ${P5_PREPARE_MAX_AVAIL_RATIO} of available space"
        P5_CASE_UNLINK_FILES="$max_prepare_files"
    fi
    log "P-5 plan: write_files=${P5_CASE_WRITE_FILES}, unlink_files=${P5_CASE_UNLINK_FILES}, duration=${MIXED_DURATION_SEC}s"
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
                --iodepth=1 --numjobs="$CLIENTS" --direct=1 --group_reporting=1 || return 1
            rm -rf "$FIO_DIR/single_write"
            ;;
        B-2)
            log "running B-2: fio single-file direct read, jobs=${CLIENTS}, size=${FIO_SIZE}"
            rm -rf "$FIO_DIR/single_read"
            mkdir -p "$FIO_DIR/single_read"
            fio --output="$OUT_DIR/fio/B-2-prepare.json" --output-format=json \
                --name=pyfalcon_fio_single_read_prepare --directory="$FIO_DIR/single_read" \
                --nrfiles=1 --rw=write --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$CLIENTS" --direct=1 --group_reporting=1 || return 1
            fio --output="$OUT_DIR/fio/B-2.json" --output-format=json \
                --name=pyfalcon_fio_single_read --directory="$FIO_DIR/single_read" \
                --nrfiles=1 --rw=read --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$CLIENTS" --direct=1 --group_reporting=1 || return 1
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
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0 || return 1
            ;;
        B-4)
            log "running B-4: fio multi-file direct read, jobs=${CLIENTS}, files=${FILES}, file_size=${FILE_SIZE}"
            if [[ ! -d "$FIO_DIR/multi" ]] || [[ "$(find "$FIO_DIR/multi" -type f | wc -l)" -eq 0 ]]; then
                run_fio_case B-3 || return 1
            fi
            fio --output="$OUT_DIR/fio/B-4.json" --output-format=json \
                --name=pyfalcon_fio_multi_read --directory="$FIO_DIR/multi" \
                --nrfiles="$fio_files_per_job" --filesize="$FILE_SIZE" \
                --rw=read --bs=2M --iodepth=1 --numjobs="$CLIENTS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0 || return 1
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
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0 || return 1
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
        B-6)
            log "running B-6: fio large-file psync sequential write, bs=2M, jobs=1, size=${FIO_LARGE_SIZE}, runtime=${FIO_LARGE_RUNTIME}s"
            rm -rf "$FIO_DIR/large_write_2m"
            mkdir -p "$FIO_DIR/large_write_2m"
            fio --output="$OUT_DIR/fio/B-6.json" --output-format=json \
                --name=write_2m --directory="$FIO_DIR/large_write_2m" \
                --direct=1 --iodepth=1 --thread --rw=write --ioengine=psync \
                --bs=2M --numjobs=1 --size="$FIO_LARGE_SIZE" --group_reporting=1 \
                --time_based --runtime="$FIO_LARGE_RUNTIME" || return 1
            rm -rf "$FIO_DIR/large_write_2m"
            ;;
        B-DIO)
            run_fio_direct_unaligned
            ;;
        B-MATRIX)
            run_fio_matrix
            ;;
        *) echo "unknown fio case: ${case_id}" >&2; exit 1 ;;
    esac
}


run_fio_matrix_one() {
    local kind="$1"
    local jobs="$2"
    local out_name="B-${kind}-N${jobs}"
    local work_dir="$FIO_DIR/matrix/${kind}_${jobs}"
    rm -rf "$work_dir"
    mkdir -p "$work_dir"
    case "$kind" in
        W)
            log "running ${out_name}: fio write matrix, jobs=${jobs}, size=${FIO_MATRIX_SIZE}"
            fio --output="$OUT_DIR/fio/${out_name}.json" --output-format=json \
                --name="write_2m_jobs${jobs}" --directory="$work_dir" \
                --direct=1 --iodepth=1 --thread --rw=write --ioengine=psync \
                --bs=2M --numjobs="$jobs" --size="$FIO_MATRIX_SIZE" --group_reporting=1 || return 1
            ;;
        R)
            log "running ${out_name}: fio read matrix, jobs=${jobs}, size=${FIO_MATRIX_SIZE}"
            fio --output="$OUT_DIR/fio/${out_name}-prepare.json" --output-format=json \
                --name="read_2m_jobs${jobs}_prepare" --directory="$work_dir" \
                --direct=1 --iodepth=1 --thread --rw=write --ioengine=psync \
                --bs=2M --numjobs="$jobs" --size="$FIO_MATRIX_SIZE" --group_reporting=1 || return 1
            fio --output="$OUT_DIR/fio/${out_name}.json" --output-format=json \
                --name="read_2m_jobs${jobs}" --directory="$work_dir" \
                --direct=1 --iodepth=1 --thread --rw=read --ioengine=psync \
                --bs=2M --numjobs="$jobs" --size="$FIO_MATRIX_SIZE" --group_reporting=1 || return 1
            ;;
        RW)
            log "running ${out_name}: fio rw matrix, jobs=${jobs}, size=${FIO_MATRIX_SIZE}"
            fio --output="$OUT_DIR/fio/${out_name}.json" --output-format=json \
                --name="rw_2m_jobs${jobs}" --directory="$work_dir" \
                --direct=1 --iodepth=1 --thread --rw=rw --rwmixread=50 --ioengine=psync \
                --bs=2M --numjobs="$jobs" --size="$FIO_MATRIX_SIZE" --group_reporting=1 || return 1
            ;;
        *) echo "unknown fio matrix kind: $kind" >&2; return 1 ;;
    esac
    rm -rf "$work_dir"
}

run_fio_matrix() {
    require_cmd fio
    mkdir -p "$OUT_DIR/fio"
    local jobs kind
    for jobs in $FIO_NUMJOBS_LIST; do
        for kind in W R RW; do
            run_fio_matrix_one "$kind" "$jobs" || return 1
        done
    done
}

run_fio_direct_unaligned() {
    require_cmd fio
    mkdir -p "$OUT_DIR/fio"
    rm -rf "$FIO_DIR/direct_unaligned"
    mkdir -p "$FIO_DIR/direct_unaligned"
    log "running B-DIO: fio direct I/O unaligned write probe, bs=4097, size=${FIO_UNALIGNED_SIZE}"
    set +e
    fio --output="$OUT_DIR/fio/B-DIO.json" --output-format=json \
        --name=direct_unaligned_len --directory="$FIO_DIR/direct_unaligned" \
        --direct=1 --iodepth=1 --thread --rw=write --ioengine=psync \
        --bs=4097 --numjobs=1 --size="$FIO_UNALIGNED_SIZE" --group_reporting=1 \
        >"$OUT_DIR/fio/B-DIO.stdout" 2>"$OUT_DIR/fio/B-DIO.stderr"
    local status=$?
    set -e
    python3 - "$OUT_DIR/fio/B-DIO-status.json" "$OUT_DIR/fio/B-DIO.json" "$status" <<'PYDIO'
import json
import sys
status_out, json_out, status = sys.argv[1], sys.argv[2], int(sys.argv[3])
result = {"mode": "fio_direct_unaligned", "exit_status": status, "expected_may_fail": True}
for out in (status_out, json_out):
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")
PYDIO
    rm -rf "$FIO_DIR/direct_unaligned"
    return 0
}

run_cache_lock_ut() {
    mkdir -p "$OUT_DIR/unit"
    log "building DiskCacheUT"
    ninja -C "$BUILD_DIR" DiskCacheUT
    "$BUILD_DIR/tests/falcon_store/DiskCacheUT" \
        --gtest_filter=DiskCacheUT.ProcessLockBlocksExclusiveEvictLockAcrossProcesses \
        >"$OUT_DIR/unit/P-LOCK.log" 2>"$OUT_DIR/unit/P-LOCK.err"
}

run_python_group() {
    run_step P-LOCK run_cache_lock_ut
    run_step P-1 run_case P-1 write_only "$WRITE_THRESHOLD"
    run_step P-2 run_case P-2 unlink_only "$WRITE_THRESHOLD"
    run_step P-3 run_case P-3 create_evict "$EVICT_THRESHOLD"
    run_step P-5 run_case P-5 concurrent_unlink "$WRITE_THRESHOLD"
    run_step P-R1 run_case P-R1 read_only "$WRITE_THRESHOLD"
    run_step P-RW run_case P-RW read_write "$WRITE_THRESHOLD"
    run_step P-RWE run_case P-RWE read_write_evict "$EVICT_THRESHOLD"
    run_step P-DIO run_case P-DIO direct_unaligned "$WRITE_THRESHOLD"
}

run_fio_group() {
    run_step B-1 run_fio_case B-1
    run_step B-2 run_fio_case B-2
    run_step B-3 run_fio_case B-3
    run_step B-4 run_fio_case B-4
    run_step B-5 run_fio_case B-5
    run_step B-6 run_fio_case B-6
    run_step B-MATRIX run_fio_matrix
    run_step B-DIO run_fio_direct_unaligned
}


write_final_report() {
    local report="$OUT_DIR/final_report.log"
    {
        echo "# PyFalcon benchmark final report"
        echo "generated_at=$(date '+%F %T')"
        echo "out_dir=$OUT_DIR"
        echo "scenario=$SCENARIO"
        echo "clients=$CLIENTS"
        echo "files=$FILES"
        echo "read_files=$READ_FILES"
        echo "pinned_read_files=$PINNED_READ_FILES"
        echo "shared_pinned_read_dir=$SHARED_PINNED_READ_DIR"
        echo "unlink_files=$UNLINK_FILES"
        echo "p5_unlink_files=$P5_UNLINK_FILES"
        echo "p5_write_files=$P5_WRITE_FILES"
        echo "max_local_disk_size_gib=$MAX_LOCAL_DISK_SIZE"
        echo "file_size=$FILE_SIZE"
        echo "wait_sec=$WAIT_SEC"
        echo "fio_size=$FIO_SIZE"
        echo "fio_large_size=$FIO_LARGE_SIZE"
        echo "fio_large_runtime=$FIO_LARGE_RUNTIME"
        echo "fio_numjobs_list=$FIO_NUMJOBS_LIST"
        echo

        echo "## benchmark_summary.md"
        if [[ -f "$OUT_DIR/benchmark_summary.md" ]]; then
            cat "$OUT_DIR/benchmark_summary.md"
        else
            echo "missing: $OUT_DIR/benchmark_summary.md"
        fi
        echo

        echo "## storage_info.txt"
        if [[ -f "$OUT_DIR/storage_info.txt" ]]; then
            cat "$OUT_DIR/storage_info.txt"
        else
            echo "missing: $OUT_DIR/storage_info.txt"
        fi
        echo

        echo "## evict_config.txt"
        if [[ -f "$OUT_DIR/evict_config.txt" ]]; then
            cat "$OUT_DIR/evict_config.txt"
        else
            echo "missing: $OUT_DIR/evict_config.txt"
        fi
        echo

        echo "## run logs"
        for file in "$OUT_DIR"/run*.log; do
            [[ -f "$file" ]] || continue
            echo "### $file"
            tail -n 240 "$file"
            echo
        done

        echo "## failure diagnostics"
        if [[ -f "$OUT_DIR/failure_diagnostics.log" ]]; then
            cat "$OUT_DIR/failure_diagnostics.log"
            echo
        fi

        echo "## python monitor logs"
        for file in "$OUT_DIR/python/P-3-monitor.log" "$OUT_DIR/python/P-RWE-monitor.log"; do
            if [[ -f "$file" ]]; then
                echo "### $file"
                cat "$file"
                echo
            fi
        done

        echo "## key python json"
        for file in "$OUT_DIR/python/P-3.json" "$OUT_DIR/python/P-RWE.json" "$OUT_DIR/python/P-DIO.json"; do
            if [[ -f "$file" ]]; then
                echo "### $file"
                cat "$file"
                echo
            fi
        done

        echo "## direct I/O fio probe"
        for file in "$OUT_DIR/fio/B-DIO-status.json" "$OUT_DIR/fio/B-DIO.stderr" "$OUT_DIR/fio/B-DIO.stdout"; do
            if [[ -f "$file" ]]; then
                echo "### $file"
                cat "$file"
                echo
            fi
        done

        echo "## fio json inventory"
        find "$OUT_DIR/fio" -maxdepth 1 -type f -name '*.json' -printf '%f\n' 2>/dev/null | sort || true
        echo

        echo "## output inventory"
        find "$OUT_DIR" -maxdepth 2 -type f -printf '%P %s bytes\n' 2>/dev/null | sort || true
    } > "$report"
    log "final report: $report"
}

write_summary() {
    python3 "$ROOT_DIR/tools/pyfalcon_benchmark_summary.py" \
        --out-dir "$OUT_DIR" \
        --scenario "$SCENARIO" \
        --clients "$CLIENTS" \
        --files "$FILES" \
        --read-files "$READ_FILES" \
        --unlink-files "$UNLINK_FILES" \
        --max-local-disk-size "$MAX_LOCAL_DISK_SIZE" \
        --file-size "$FILE_SIZE" \
        --wait-sec "$WAIT_SEC" \
        --mixed-duration-sec "$MIXED_DURATION_SEC" \
        --fio-size "$FIO_SIZE" \
        --fio-large-size "$FIO_LARGE_SIZE" \
        --fio-large-runtime "$FIO_LARGE_RUNTIME"
}


write_case_failure_diagnostics() {
    local case_id="$1"
    local diag="$OUT_DIR/failure_diagnostics.log"
    local case_log="$OUT_DIR/python/${case_id}.log"
    local monitor_log="$OUT_DIR/python/${case_id}-monitor.log"
    local falcon_log_dir="$OUT_DIR/python/${case_id}-falcon-log"
    local meta_log="$OUT_DIR/python/${case_id}-meta.log"
    local meta_root
    meta_root="$(metadata_root 2>/dev/null || true)"

    {
        echo "# ${case_id} failure diagnostics"
        echo "generated_at=$(date '+%F %T')"
        echo "out_dir=$OUT_DIR"
        echo "cache_root=$CACHE_ROOT"
        echo "clients=$CLIENTS files=$FILES read_files=$READ_FILES pinned_read_files=$PINNED_READ_FILES shared_pinned_read_dir=$SHARED_PINNED_READ_DIR file_size=$FILE_SIZE"
        echo "mixed_duration_sec=$MIXED_DURATION_SEC max_local_disk_size_gib=$MAX_LOCAL_DISK_SIZE evict_threshold=$EVICT_THRESHOLD"
        echo

        echo "## space"
        for path in "$CACHE_ROOT" "$OUT_DIR" "${meta_root:+$(dirname "$meta_root")}"; do
            [[ -e "$path" ]] || continue
            echo "### $path"
            df -h "$path" || true
            df -ih "$path" || true
        done
        if [[ -d "$CACHE_ROOT" ]]; then
            echo "cache_size=$(du -sh "$CACHE_ROOT" 2>/dev/null | awk '{print $1}')"
            echo "cache_data_files=$(find "$CACHE_ROOT" -type f ! -path '*/.falcon_cache_locks/*' 2>/dev/null | wc -l)"
            if [[ -d "$CACHE_ROOT/.falcon_cache_locks" ]]; then
                echo "cache_lock_files=$(find "$CACHE_ROOT/.falcon_cache_locks" -type f 2>/dev/null | wc -l)"
            fi
        fi
        echo

        echo "## evict plan"
        if [[ -f "$OUT_DIR/evict_config.txt" ]]; then
            cat "$OUT_DIR/evict_config.txt"
        else
            echo "missing $OUT_DIR/evict_config.txt"
        fi
        echo

        echo "## case error summary"
        if [[ -f "$OUT_DIR/python/${case_id}.json" ]]; then
            python3 - "$OUT_DIR/python/${case_id}.json" <<'PYFAILJSON' || true
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as f:
    data = json.load(f)
print(f"mode={data.get('mode')} error_count={data.get('error_count')} timed={data.get('timed')}")
for group_name in ("prepare", "reader", "writer", "deleter", "create", "unlink"):
    group = data.get(group_name)
    if not isinstance(group, dict):
        continue
    print(
        f"{group_name}: files={group.get('files')} error_count={group.get('error_count')} "
        f"elapsed={group.get('elapsed_sec')} max_worker_elapsed={group.get('max_worker_elapsed_sec')} "
        f"files_per_sec={group.get('files_per_sec')}"
    )
    for item in (group.get("errors") or [])[:8]:
        print(
            f"{group_name} error role={item.get('role')} client={item.get('client_id')} "
            f"files={item.get('files')} completed_before_error={item.get('completed_files_before_error')} "
            f"next_index={item.get('next_index')} failed_path={item.get('failed_path')} "
            f"elapsed={item.get('elapsed_sec')} stop_reason={item.get('stop_reason')} msg={item.get('error')}"
        )
PYFAILJSON
        else
            echo "missing $OUT_DIR/python/${case_id}.json"
        fi
        echo

        echo "## key error logs"
        rg -n "ERROR|FATAL|ENOSPC|No space|-28|DiskCache::Cleanup|CleanupForEvict|Evicted|process_lock_busy|metadata_unlink_failed|remove_failed|PreAllocSpace|HasFreeSpace|LEASE|failed" \
            "$falcon_log_dir" "$case_log" "$monitor_log" "$meta_log" "$ROOT_DIR/deploy/meta" 2>/dev/null | tail -n 200 || true
        echo
    } >> "$diag"
    log "failure diagnostics: $diag"
}

print_python_case_diagnostics() {
    local case_id="$1"
    local case_json="$OUT_DIR/python/${case_id}.json"
    local case_log="$OUT_DIR/python/${case_id}.log"
    local idle_log="$OUT_DIR/python/${case_id}-idle.log"
    local meta_log="$OUT_DIR/python/${case_id}-meta.log"

    [[ "$case_id" != P-* ]] && return 0
    log "diagnostics for ${case_id}"
    write_case_failure_diagnostics "$case_id" || true
    if [[ -f "$case_json" ]]; then
        python3 - "$case_json" <<'PYDIAG' || true
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"json={path}")
print(f"mode={data.get('mode', 'N/A')}, error_count={data.get('error_count', 'N/A')}")

def show_group(name, group):
    if not isinstance(group, dict):
        return
    errors = group.get("errors") or []
    per_client = group.get("per_client") or []
    if group.get("error_count", 0) or errors:
        print(f"{name}.error_count={group.get('error_count', 0)}")
    print(
        "%s summary files=%s elapsed=%s max_worker_elapsed=%s files_per_sec=%s mib_per_sec=%s"
        % (
            name,
            group.get("files", "N/A"),
            group.get("elapsed_sec", "N/A"),
            group.get("max_worker_elapsed_sec", "N/A"),
            group.get("files_per_sec", "N/A"),
            group.get("mib_per_sec", "N/A"),
        )
    )
    for item in errors[:10]:
        print(
            "%s error role=%s client=%s files=%s elapsed=%s stop_reason=%s msg=%s"
            % (
                name,
                item.get("role", "N/A"),
                item.get("client_id", "N/A"),
                item.get("files", "N/A"),
                item.get("elapsed_sec", "N/A"),
                item.get("stop_reason", "N/A"),
                item.get("error", ""),
            )
        )
        for key in ("completed_files_before_error", "next_index", "failed_path", "last_path"):
            if key in item:
                print(f"{name} error detail {key}={item.get(key)}")
    for item in per_client:
        if item.get("error") or item.get("stop_reason") == "max_files":
            print(
                "%s per_client role=%s client=%s files=%s elapsed=%s stop_reason=%s msg=%s"
                % (
                    name,
                    item.get("role", "N/A"),
                    item.get("client_id", "N/A"),
                    item.get("files", "N/A"),
                    item.get("elapsed_sec", "N/A"),
                    item.get("stop_reason", "N/A"),
                    item.get("error", ""),
                )
            )
            for key in ("completed_files_before_error", "next_index", "failed_path", "last_path"):
                if key in item:
                    print(f"{name} per_client detail role={item.get('role', 'N/A')} client={item.get('client_id', 'N/A')} {key}={item.get(key)}")

for name in ("create", "unlink", "prepare", "read", "reader", "writer", "deleter"):
    show_group(name, data.get(name))

for item in (data.get("errors") or [])[:10]:
    print(f"top error: {item}")
PYDIAG
    else
        log "missing json for ${case_id}: ${case_json}"
    fi

    for file in "$case_log" "$idle_log" "$meta_log" "$ROOT_DIR/deploy/meta"/cnlogfile*.log "$ROOT_DIR/deploy/meta"/workerlogfile*.log; do
        if [[ -f "$file" ]]; then
            log "tail ${file}"
            tail -n 80 "$file" || true
        fi
    done
}

run_step() {
    local case_id="$1"
    shift
    local start_ts end_ts elapsed status
    start_ts="$(date +%s)"
    log "case ${case_id} begin"
    set +e
    "$@"
    status=$?
    set -e
    end_ts="$(date +%s)"
    elapsed=$((end_ts - start_ts))
    if [[ "$status" -eq 0 ]]; then
        log "case ${case_id} finished, elapsed=${elapsed}s"
        return 0
    fi

    log "case ${case_id} failed, status=${status}, elapsed=${elapsed}s"
    FAILED_CASES+=("${case_id}:${status}")
    set +e
    print_python_case_diagnostics "$case_id"
    stop_idle_server 2>/dev/null
    if [[ "$case_id" == P-* ]]; then
        clean_runtime
    fi
    set -e
    return 0
}

cleanup_success_case_artifacts() {
    local case_id="$1"
    [[ "${KEEP_SUCCESS_CASE_LOGS:-0}" == "1" ]] && return 0
    rm -f "$OUT_DIR/python/${case_id}.log" \
        "$OUT_DIR/python/${case_id}-idle.log" \
        "$OUT_DIR/python/${case_id}-idle.json" \
        "$OUT_DIR/python/${case_id}-meta.log" \
        "$OUT_DIR/python/${case_id}-monitor.log" \
        "$OUT_DIR/python/${case_id}-config.json"
    rm -rf "$OUT_DIR/python/${case_id}-falcon-log"
}

run_case() {
    local case_id="$1"
    local mode="$2"
    local threshold="$3"
    local status case_files case_unlink_files case_threshold case_config case_log monitor_log bench_pid monitor_pid
    local -a shared_pinned_read_arg=()
    case_files="$FILES"
    case_unlink_files="$UNLINK_FILES"
    case_threshold="$threshold"
    if [[ "$SHARED_PINNED_READ_DIR" == "1" ]]; then
        shared_pinned_read_arg=(--shared-pinned-read-dir)
    fi
    ensure_binary || return 1
    clean_runtime || return 1
    if [[ "$mode" == "concurrent_unlink" ]]; then
        configure_p5_case || return 1
        case_files="$P5_CASE_WRITE_FILES"
        case_unlink_files="$P5_CASE_UNLINK_FILES"
    fi
    if [[ "$mode" == "create_evict" || "$mode" == "read_write_evict" ]]; then
        configure_auto_evict_case "$threshold" || return 1
        case_threshold="$AUTO_EVICT_CASE_THRESHOLD"
        case_files="$AUTO_EVICT_CASE_FILES"
    elif [[ "$mode" == "read_write" && "$MIXED_DURATION_SEC" != "0" && "$MIXED_DURATION_SEC" != "0.0" && "$AUTO_EVICT_CONFIG" == "1" ]]; then
        configure_auto_evict_case "$threshold" || return 1
        case_threshold="$threshold"
        case_files="$AUTO_EVICT_CASE_FILES"
        log "auto mixed write plan: keep threshold=${case_threshold}, files=${case_files}, write=$(bytes_to_mib $((case_files * FILE_SIZE)))MiB"
    fi
    check_case_space "$case_id" "$mode" "$case_files" "$case_unlink_files" || return 1
    prepare_cache_dirs || return 1
    safe_rm_rf "$OUT_DIR/work_${case_id}" >/dev/null 2>&1 || true
    mkdir -p "$OUT_DIR/python" "$OUT_DIR/work_${case_id}" || return 1
    case_config="$(create_case_config "$case_id")" || return 1
    case_log="$OUT_DIR/python/${case_id}.log"
    monitor_log="$OUT_DIR/python/${case_id}-monitor.log"
    log "case ${case_id} config: ${case_config}, cache_root=${CACHE_ROOT}, cluster_view=${BENCHMARK_CLUSTER_VIEW}"
    start_meta "$case_threshold" "$case_id" || return 1
    start_idle_server "$case_threshold" "$case_id" "$case_config" || return 1
    log "running ${case_id}: mode=${mode}, clients=${CLIENTS}, files=${case_files}, file_size=${FILE_SIZE}, threshold=${case_threshold}"
    (
        cd "$ROOT_DIR"
        STORAGE_THRESHOLD="$case_threshold" python3 "$ROOT_DIR/tools/pyfalcon_client_perf.py" \
            --mode "$mode" \
            --clients "$CLIENTS" \
            --files "$case_files" \
            --read-files "$READ_FILES" \
            --pinned-read-files "$PINNED_READ_FILES" \
            --unlink-files "$case_unlink_files" \
            --file-size "$FILE_SIZE" \
            --wait-sec "$WAIT_SEC" \
            --duration-sec "$MIXED_DURATION_SEC" \
            --hot-read-window "$HOT_READ_WINDOW" \
            --hot-read-lag "$HOT_READ_LAG" \
            --hot-read-min-files "$HOT_READ_MIN_FILES" \
            "${shared_pinned_read_arg[@]}" \
            --dir "/py_${case_id}" \
            --workspace "$OUT_DIR/work_${case_id}" \
            --config "$case_config" \
            --python-interface "$PYTHON_INTERFACE" \
            --output "$OUT_DIR/python/${case_id}.json"
    ) >"$case_log" 2>&1 &
    bench_pid=$!

    monitor_pid=""
    if [[ ( "$mode" == "create_evict" || "$mode" == "read_write_evict" ) && "$P3_MONITOR" == "1" ]]; then
        monitor_p3_case "$case_id" "$bench_pid" "$case_log" "$monitor_log" &
        monitor_pid=$!
    fi

    set +e
    wait "$bench_pid"
    status=$?
    set -e
    log "case ${case_id} benchmark process exited, status=${status}"

    if [[ -n "$monitor_pid" ]]; then
        kill "$monitor_pid" 2>/dev/null || true
        wait "$monitor_pid" 2>/dev/null || true
    fi
    stop_idle_server || true
    if [[ "$status" == "0" ]]; then
        cleanup_success_case_artifacts "$case_id"
    fi
    return "$status"
}

run_group() {
    case "$1" in
        all)
            run_python_group
            run_fio_group
            ;;
        python) run_python_group ;;
        fio) run_fio_group ;;
        P-LOCK) run_step P-LOCK run_cache_lock_ut ;;
        P-1) run_step P-1 run_case P-1 write_only "$WRITE_THRESHOLD" ;;
        P-2) run_step P-2 run_case P-2 unlink_only "$WRITE_THRESHOLD" ;;
        P-3) run_step P-3 run_case P-3 create_evict "$EVICT_THRESHOLD" ;;
        P-5) run_step P-5 run_case P-5 concurrent_unlink "$WRITE_THRESHOLD" ;;
        P-R1) run_step P-R1 run_case P-R1 read_only "$WRITE_THRESHOLD" ;;
        P-RW) run_step P-RW run_case P-RW read_write "$WRITE_THRESHOLD" ;;
        P-RWE|PWE|pwe) run_step P-RWE run_case P-RWE read_write_evict "$EVICT_THRESHOLD" ;;
        P-DIO) run_step P-DIO run_case P-DIO direct_unaligned "$WRITE_THRESHOLD" ;;
        B-1|B-2|B-3|B-4|B-5|B-6|B-DIO|B-MATRIX) run_step "$1" run_fio_case "$1" ;;
        fio-matrix) run_step B-MATRIX run_fio_matrix ;;
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
prepare_benchmark_env
prepare_storage_targets
run_group "$SCENARIO"
if scenario_uses_falcon; then
    clean_runtime || log "warning: final clean_runtime failed"
fi
write_summary || log "warning: write_summary failed"
log "summary: $OUT_DIR/benchmark_summary.md"
log "summary log: $OUT_DIR/benchmark_summary.log"
write_final_report || log "warning: write_final_report failed"
cleanup_temp_dirs || log "warning: cleanup_temp_dirs failed"
if (( ${#FAILED_CASES[@]} > 0 )); then
    log "failed cases: ${FAILED_CASES[*]}"
    exit 1
fi
log "done"
