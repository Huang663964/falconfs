#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
OUT_DIR="${OUT_DIR:-/tmp/falcon_diskcache_perf_$(date +%Y%m%d_%H%M%S)}"
MOUNT_DIR="${MOUNT_DIR:-/tmp/falcon_mnt}"
CACHE_ROOT="${CACHE_ROOT:-/tmp/falcon_cache}"
FILE_SIZE="${FILE_SIZE:-2097152}"
FILES="${FILES:-6000}"
UNLINK_FILES="${UNLINK_FILES:-6000}"
WINDOW="${WINDOW:-500}"
WORKERS="${WORKERS:-4}"
WAIT_SEC="${WAIT_SEC:-45}"
FIO_SIZE="${FIO_SIZE:-8G}"
INTERNAL_EVICT_THRESHOLD="${INTERNAL_EVICT_THRESHOLD:-0.72}"
FUSE_EVICT_THRESHOLD="${FUSE_EVICT_THRESHOLD:-0.53}"
SCENARIO="${1:-help}"

usage() {
    cat <<EOF
Usage:
  $0 <scenario>

Scenarios:
  internal        Run I-1, I-2, I-3 and I-5 through Falcon internal APIs.
  fuse            Run F-1, F-2, F-3 and F-5 through FUSE mount ${MOUNT_DIR}.
  fio             Run B-1..B-5 local fio baseline under /tmp.
  all             Run internal, fuse, and fio.
  I-1/I-2/I-3/I-5 Run one internal scenario.
  F-1/F-2/F-3/F-5 Run one FUSE scenario.
  B-1..B-5        Run one fio/baseline scenario.

Environment overrides:
  OUT_DIR=${OUT_DIR}
  BUILD_DIR=${BUILD_DIR}
  FILES=${FILES}
  UNLINK_FILES=${UNLINK_FILES}
  FILE_SIZE=${FILE_SIZE}
  WINDOW=${WINDOW}
  WORKERS=${WORKERS}
  WAIT_SEC=${WAIT_SEC}
  FIO_SIZE=${FIO_SIZE}
  INTERNAL_EVICT_THRESHOLD=${INTERNAL_EVICT_THRESHOLD}
  FUSE_EVICT_THRESHOLD=${FUSE_EVICT_THRESHOLD}

Examples:
  FILES=100 UNLINK_FILES=50 WORKERS=4 WAIT_SEC=5 $0 all
  OUT_DIR=/tmp/falcon_perf_current $0 F-3
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

require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing command: $1" >&2
        exit 1
    fi
}

falcon_port_listeners() {
    local pattern=':(55500|55510|55520|55530|56039)([[:space:]]|$)'
    if command -v rg >/dev/null 2>&1; then
        sudo ss -ltnp | rg "$pattern" || true
    else
        sudo ss -ltnp | grep -E "$pattern" || true
    fi
}



effective_window() {
    local files="$1"
    if (( WINDOW < files )); then
        echo "$WINDOW"
    elif (( files > 1 )); then
        echo $((files / 2))
    else
        echo 1
    fi
}


release_default_ports() {
    local busy
    for _ in $(seq 1 20); do
        busy="$(falcon_port_listeners)"
        if [[ -z "$busy" ]]; then
            return 0
        fi
        sudo fuser -k 55500/tcp 55510/tcp 55520/tcp 55530/tcp 56039/tcp >/dev/null 2>&1 || true
        sleep 0.5
    done
    busy="$(falcon_port_listeners)"
    if [[ -n "$busy" ]]; then
        echo "default Falcon ports are still in use after cleanup:" >&2
        echo "$busy" >&2
        echo "stop the owning processes or rerun after the default ports are free." >&2
        exit 1
    fi
}

ensure_default_ports_free() {
    local busy
    busy="$(falcon_port_listeners)"
    if [[ -n "$busy" ]]; then
        echo "default Falcon ports are still in use after cleanup:" >&2
        echo "$busy" >&2
        echo "stop the owning processes or rerun after the default ports are free." >&2
        exit 1
    fi
}

ensure_internal_binary() {
    if [[ ! -x "$BUILD_DIR/internal_perf/falcon_internal_perf" ]]; then
        log "building falcon_internal_perf"
        ninja -C "$BUILD_DIR" falcon_internal_perf
    fi
}

clean_ports_and_runtime() {
    log "stopping Falcon services and cleaning runtime files"
    sudo "$ROOT_DIR/deploy/falcon_stop.sh" >/dev/null 2>&1 || true
    release_default_ports
    sudo umount -l "$MOUNT_DIR" >/dev/null 2>&1 || true
    sudo rm -rf "$CACHE_ROOT" "$MOUNT_DIR" "$HOME/metadata"
    sudo rm -f /tmp/.s.PGSQL.55500* /tmp/.s.PGSQL.55510* /tmp/.s.PGSQL.55520* /tmp/.s.PGSQL.55530*
    sudo rm -f /home/hx/metadata/coordinator0/postmaster.pid /home/hx/metadata/worker0/postmaster.pid
    ensure_default_ports_free
}

start_falcon_full() {
    local threshold="$1"
    clean_ports_and_runtime
    mkdir -p "$MOUNT_DIR"
    log "starting Falcon through deploy/falcon_start.sh, STORAGE_THRESHOLD=${threshold}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        STORAGE_THRESHOLD="$threshold" bash "$ROOT_DIR/deploy/falcon_start.sh"
    )
    sleep 3
    mountpoint -q "$MOUNT_DIR"
}

start_meta_for_internal() {
    local threshold="$1"
    clean_ports_and_runtime
    mkdir -p "$CACHE_ROOT"
    for shard in $(seq 0 100); do
        mkdir -p "$CACHE_ROOT/$shard"
    done
    log "starting Falcon meta services for internal test, STORAGE_THRESHOLD=${threshold}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        STORAGE_THRESHOLD="$threshold" bash "$ROOT_DIR/deploy/meta/falcon_meta_start.sh"
    )
    sleep 3
}

run_internal_case() {
    local id="$1"
    local mode threshold files wait dir case_window
    case "$id" in
        I-1) mode="write_only"; threshold="1"; files="$FILES"; wait="0"; dir="/internal_pure_write" ;;
        I-2) mode="unlink_only"; threshold="1"; files="$UNLINK_FILES"; wait="0"; dir="/internal_unlink_only" ;;
        I-3) mode="create_evict"; threshold="$INTERNAL_EVICT_THRESHOLD"; files="$FILES"; wait="$WAIT_SEC"; dir="/internal_create_evict" ;;
        I-5) mode="concurrent_unlink"; threshold="1"; files="$FILES"; wait="0"; dir="/internal_concurrent_unlink" ;;
        *) echo "unknown internal case: $id" >&2; exit 1 ;;
    esac
    case_window="$WINDOW"
    ensure_internal_binary
    start_meta_for_internal "$threshold"
    mkdir -p "$OUT_DIR/internal"
    log "running ${id}: mode=${mode}, workers=${WORKERS}, files=${files}, file_size=${FILE_SIZE}"
    (
        cd "$ROOT_DIR"
        source_falcon_env
        STORAGE_THRESHOLD="$threshold" "$BUILD_DIR/internal_perf/falcon_internal_perf" \
            --mode "$mode" \
            --dir "$dir" \
            --output "$OUT_DIR/internal/${id}.json" \
            --files "$files" \
            --file-size "$FILE_SIZE" \
            --window "$case_window" \
            --unlink-files "$UNLINK_FILES" \
            --wait-sec "$wait" \
            --workers "$WORKERS"
    )
}

run_fuse_case() {
    local id="$1"
    local mode threshold files wait dir case_window
    case "$id" in
        F-1) mode="write_only"; threshold="1"; files="$FILES"; wait="0"; dir="$MOUNT_DIR/fuse_pure_write" ;;
        F-2) mode="unlink_only"; threshold="1"; files="$UNLINK_FILES"; wait="0"; dir="$MOUNT_DIR/fuse_unlink_only" ;;
        F-3) mode="create_evict"; threshold="$FUSE_EVICT_THRESHOLD"; files="$FILES"; wait="$WAIT_SEC"; dir="$MOUNT_DIR/fuse_create_evict" ;;
        F-5) mode="concurrent_unlink"; threshold="1"; files="$FILES"; wait="0"; dir="$MOUNT_DIR/fuse_concurrent_unlink" ;;
        *) echo "unknown FUSE case: $id" >&2; exit 1 ;;
    esac
    case_window="$WINDOW"
    start_falcon_full "$threshold"
    mkdir -p "$OUT_DIR/fuse"
    log "running ${id}: mode=${mode}, workers=${WORKERS}, files=${files}, file_size=${FILE_SIZE}"
    python3 "$ROOT_DIR/tools/fuse_perf.py" \
        --mode "$mode" \
        --dir "$dir" \
        --output "$OUT_DIR/fuse/${id}.json" \
        --files "$files" \
        --file-size "$FILE_SIZE" \
        --window "$case_window" \
        --unlink-files "$UNLINK_FILES" \
        --wait-sec "$wait" \
        --workers "$WORKERS"
}

run_fio_case() {
    local id="$1"
    local fio_files_per_job=$(( (FILES + WORKERS - 1) / WORKERS ))
    require_cmd fio
    mkdir -p "$OUT_DIR/fio"
    case "$id" in
        B-1)
            log "running B-1 fio single-file direct write"
            rm -rf /tmp/ffio_single_write
            mkdir -p /tmp/ffio_single_write
            fio --output="$OUT_DIR/fio/B-1.json" --output-format=json \
                --name=fio_seq_write --directory=/tmp/ffio_single_write \
                --nrfiles=1 --rw=write --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$WORKERS" --direct=1 --group_reporting=1
            rm -rf /tmp/ffio_single_write
            ;;
        B-2)
            log "running B-2 fio single-file direct read"
            if [[ ! -d /tmp/ffio_single_read ]] || [[ "$(find /tmp/ffio_single_read -type f | wc -l)" -eq 0 ]]; then
                rm -rf /tmp/ffio_single_read
                mkdir -p /tmp/ffio_single_read
                fio --output=/tmp/ffio_seq_read_prepare.json --output-format=json \
                    --name=fio_seq_prepare --directory=/tmp/ffio_single_read \
                    --nrfiles=1 --rw=write --size="$FIO_SIZE" --bs=2M \
                    --iodepth=1 --numjobs="$WORKERS" --direct=1 --group_reporting=1
            fi
            fio --output="$OUT_DIR/fio/B-2.json" --output-format=json \
                --name=fio_seq_read --directory=/tmp/ffio_single_read \
                --nrfiles=1 --rw=read --size="$FIO_SIZE" --bs=2M \
                --iodepth=1 --numjobs="$WORKERS" --direct=1 --group_reporting=1
            rm -rf /tmp/ffio_single_read
            ;;
        B-3)
            log "running B-3 fio multi-file direct write"
            rm -rf /tmp/ffio_multi
            mkdir -p /tmp/ffio_multi
            fio --output="$OUT_DIR/fio/B-3.json" --output-format=json \
                --name=ffio_multi --directory=/tmp/ffio_multi \
                --nrfiles="$fio_files_per_job" --filesize="${FILE_SIZE}" \
                --rw=write --bs=2M --iodepth=1 --numjobs="$WORKERS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0
            ;;
        B-4)
            log "running B-4 fio multi-file direct read"
            if [[ ! -d /tmp/ffio_multi ]] || [[ "$(find /tmp/ffio_multi -type f | wc -l)" -eq 0 ]]; then
                "$0" B-3
            fi
            fio --output="$OUT_DIR/fio/B-4.json" --output-format=json \
                --name=ffio_multi --directory=/tmp/ffio_multi \
                --nrfiles="$fio_files_per_job" --filesize="${FILE_SIZE}" \
                --rw=read --bs=2M --iodepth=1 --numjobs="$WORKERS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0
            rm -rf /tmp/ffio_multi
            ;;
        B-5)
            log "running B-5 local multi-file delete baseline"
            rm -rf /tmp/ffio_delete
            mkdir -p /tmp/ffio_delete
            fio --output="$OUT_DIR/fio/B-5-create.json" --output-format=json \
                --name=ffio_delete --directory=/tmp/ffio_delete \
                --nrfiles="$fio_files_per_job" --filesize="${FILE_SIZE}" \
                --rw=write --bs=2M --iodepth=1 --numjobs="$WORKERS" --direct=1 \
                --openfiles=1 --file_service_type=sequential --group_reporting=1 --unlink=0
            local count_before start_ns end_ns elapsed_ns count_after
            count_before="$(find /tmp/ffio_delete -type f | wc -l)"
            start_ns="$(date +%s%N)"
            find /tmp/ffio_delete -type f -print0 | xargs -0 -n 100 -P "$WORKERS" rm -f
            end_ns="$(date +%s%N)"
            count_after="$(find /tmp/ffio_delete -type f | wc -l)"
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
            rm -rf /tmp/ffio_delete
            ;;
        *) echo "unknown fio case: $id" >&2; exit 1 ;;
    esac
}

run_group() {
    local group="$1"
    case "$group" in
        internal)
            run_internal_case I-1
            run_internal_case I-2
            run_internal_case I-3
            run_internal_case I-5
            ;;
        fuse)
            run_fuse_case F-1
            run_fuse_case F-2
            run_fuse_case F-3
            run_fuse_case F-5
            ;;
        fio)
            run_fio_case B-1
            run_fio_case B-2
            run_fio_case B-3
            run_fio_case B-4
            run_fio_case B-5
            ;;
        all)
            run_group internal
            run_group fuse
            run_group fio
            ;;
        I-*) run_internal_case "$group" ;;
        F-*) run_fuse_case "$group" ;;
        B-*) run_fio_case "$group" ;;
        help|-h|--help) usage ;;
        *) usage; exit 1 ;;
    esac
}

if [[ "$SCENARIO" == "help" || "$SCENARIO" == "-h" || "$SCENARIO" == "--help" ]]; then
    usage
    exit 0
fi

mkdir -p "$OUT_DIR"
log "output directory: $OUT_DIR"
run_group "$SCENARIO"
log "done"
