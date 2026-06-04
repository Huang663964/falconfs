#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
OUT_DIR="${OUT_DIR:-/tmp/pyfalcon_client_benchmark_$(date +%Y%m%d_%H%M%S)}"
CACHE_ROOT="${CACHE_ROOT:-/tmp/falcon_cache}"
MOUNT_DIR="${MOUNT_DIR:-/tmp/falcon_mnt}"
CONFIG_FILE_PATH="${CONFIG_FILE_PATH:-/usr/local/falconfs/falcon_client/config/config.json}"
PYTHON_INTERFACE="${PYTHON_INTERFACE:-$ROOT_DIR/python_interface}"
CLIENTS="${CLIENTS:-4}"
FILES="${FILES:-6000}"
UNLINK_FILES="${UNLINK_FILES:-6000}"
FILE_SIZE="${FILE_SIZE:-2097152}"
WAIT_SEC="${WAIT_SEC:-45}"
WRITE_THRESHOLD="${WRITE_THRESHOLD:-1}"
EVICT_THRESHOLD="${EVICT_THRESHOLD:-0.72}"
IDLE_WAIT_SEC="${IDLE_WAIT_SEC:-900}"
SCENARIO="${1:-all}"

usage() {
    cat <<EOF
Usage:
  $0 <scenario>

Scenarios:
  all   Run P-1, P-2, P-3 and P-5.
  P-1   Python internal 4-client write-only benchmark.
  P-2   Python internal 4-client unlink-only benchmark.
  P-3   Python internal 4-client write-triggered DiskCache evict benchmark.
  P-5   Python internal concurrent write + unlink benchmark.

Environment overrides:
  OUT_DIR=${OUT_DIR}
  CLIENTS=${CLIENTS}
  FILES=${FILES}
  UNLINK_FILES=${UNLINK_FILES}
  FILE_SIZE=${FILE_SIZE}
  WAIT_SEC=${WAIT_SEC}
  WRITE_THRESHOLD=${WRITE_THRESHOLD}
  EVICT_THRESHOLD=${EVICT_THRESHOLD}
  CONFIG_FILE_PATH=${CONFIG_FILE_PATH}
  PYTHON_INTERFACE=${PYTHON_INTERFACE}

Example:
  OUT_DIR=/tmp/pyfalcon_bench CLIENTS=4 FILES=6000 UNLINK_FILES=6000 $0 all
EOF
}

log() {
    printf '[%s] %s\n' "$(date '+%F %T')" "$*"
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
        if ! sudo ss -ltnp | rg ':(55500|55510|55520|55530|56039)\b' >/dev/null; then
            return 0
        fi
        sudo fuser -k 55500/tcp 55510/tcp 55520/tcp 55530/tcp 56039/tcp >/dev/null 2>&1 || true
        sleep 0.5
    done
    if sudo ss -ltnp | rg ':(55500|55510|55520|55530|56039)\b' >/dev/null; then
        echo "default Falcon ports are still busy:" >&2
        sudo ss -ltnp | rg ':(55500|55510|55520|55530|56039)\b' >&2 || true
        exit 1
    fi
}

clean_runtime() {
    log "cleaning Falcon runtime"
    sudo "$ROOT_DIR/deploy/falcon_stop.sh" >/dev/null 2>&1 || true
    sudo umount -l "$MOUNT_DIR" >/dev/null 2>&1 || true
    release_ports
    sudo rm -rf "$CACHE_ROOT" "$MOUNT_DIR" "$HOME/metadata"
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
        if ss -ltnp | rg ':(56039)\b' >/dev/null; then
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
    trap stop_idle_server EXIT
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
            run_case P-1 write_only "$WRITE_THRESHOLD"
            run_case P-2 unlink_only "$WRITE_THRESHOLD"
            run_case P-3 create_evict "$EVICT_THRESHOLD"
            run_case P-5 concurrent_unlink "$WRITE_THRESHOLD"
            ;;
        P-1) run_case P-1 write_only "$WRITE_THRESHOLD" ;;
        P-2) run_case P-2 unlink_only "$WRITE_THRESHOLD" ;;
        P-3) run_case P-3 create_evict "$EVICT_THRESHOLD" ;;
        P-5) run_case P-5 concurrent_unlink "$WRITE_THRESHOLD" ;;
        help|-h|--help) usage ;;
        *) usage; exit 1 ;;
    esac
}

if [[ "$SCENARIO" == "help" || "$SCENARIO" == "-h" || "$SCENARIO" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$OUT_DIR/python"
log "output directory: $OUT_DIR"
run_group "$SCENARIO"
clean_runtime
log "done"
