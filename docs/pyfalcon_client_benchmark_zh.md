# Python internal client 并发 benchmark 使用说明

本文说明如何在服务器上通过 Python internal API 调用 Falcon 内部接口做并发性能测试。该 benchmark 不通过 FUSE 挂载点发起业务操作。

## 1. 测试目标

benchmark 覆盖 Python internal API 和 fio 本地基准两类场景。Python internal API 覆盖以下场景：

| 编号 | 场景 | 并发模型 |
| --- | --- | --- |
| P-LOCK | 共享 cache evict 锁回归 | 构建并运行 `DiskCacheUT` 中的跨进程 cache 锁用例，验证其他进程持有 cache 文件时 evict 无法拿到独占锁并跳过删除 |
| P-1 | 纯写 | 4 个 Python 进程，每个进程 1 个 `pyfalconfs.Client` |
| P-2 | 纯删除 | 先 4 client 预创建文件，再 4 client 并发 `FalconUnlink` |
| P-3 | 写触发 evict | 4 writer client 并发写，DiskCache 后台 cleanup 删除 cache |
| P-5 | 边写边删 | 先预创建待删数据集，4 writer client + 4 deleter client 固定运行 `MIXED_DURATION_SEC` 秒，分别统计写入和 unlink 真实吞吐 |
| P-R1 | 纯读 | 先预创建读数据集，再 4 reader client 并发 `FalconOpen/FalconRead/FalconClose` |
| P-RW | 读 + 写 | 先预创建并打开固定读热集，4 reader client 循环读已 pin 的热集，4 writer client 持续写，运行 `MIXED_DURATION_SEC` 秒，不触发 evict |
| P-RWE | 读 + 写 + evict | 先预创建并打开固定读热集，4 reader client 循环读已 pin 的热集，4 writer client 持续写，运行 `MIXED_DURATION_SEC` 秒，并通过 writer 写入推动后台 `DiskCache::Cleanup()` |
| P-DIO | O_DIRECT 对齐探测 | 通过 Python internal API 测试对齐写、长度未对齐、offset 未对齐、buffer 未对齐 |

fio 本地基准覆盖基础场景、并发矩阵和 direct I/O 未对齐探测：

| 编号 | 场景 | 说明 |
| --- | --- | --- |
| B-1 | 单文件 direct 顺序写 | `fio --rw=write`，`numjobs=CLIENTS` |
| B-2 | 单文件 direct 顺序读 | 先预写，再 `fio --rw=read` |
| B-3 | 多文件 direct 顺序写 | 按 `FILES/CLIENTS` 拆分多文件 |
| B-4 | 多文件 direct 顺序读 | 读取 B-3 创建的多文件 |
| B-5 | 本地多文件删除 | 按 `UNLINK_FILES/CLIENTS` 先用 fio 创建待删文件，再用并发 `rm -f` 删除 |
| B-6 | 大文件 2MiB psync 连续写 | 对齐用户给出的 fio 命令，测单 job 大文件连续写 |
| B-MATRIX | fio 多并发矩阵 | 按 `FIO_NUMJOBS_LIST=1 4 8 16` 分别测试 write/read/rw |
| B-DIO | fio direct I/O 未对齐写探测 | `bs=4097`，记录 fio 实际退出码和错误 |

Python client 调用链：

```text
pyfalconfs.Client
-> _pyfalconfs_internal.cpp
-> FalconCreate / FalconWrite / FalconClose / FalconUnlink
```

## 2. 为什么需要 FUSE brpc data node

`_pyfalconfs_internal.cpp` 只执行 `GetInit().Init()` 和 `FalconInit()`，不会启动 `RemoteIOServer`。Python internal benchmark 仍需要一个本机 `falcon_client` FUSE 进程作为 brpc data node 提供 store/data RPC endpoint，因此脚本会先启动一个监听 `56039` 的 FUSE 进程：

```bash
CONFIG_FILE=<case-config> STORAGE_THRESHOLD=<threshold> falcon_client \
  /tmp/falcon_mnt -f -o direct_io -brpc true -rpc_endpoint=0.0.0.0:56039
```

Python benchmark 的业务操作仍然通过 `pyfalconfs.Client -> _pyfalconfs_internal.cpp -> FalconCreate/FalconWrite/FalconClose/FalconUnlink` 发起，不通过 `/tmp/falcon_mnt` 做文件读写。脚本会给这些 Python 进程导出：

```bash
FALCON_CLUSTER_VIEW=127.0.0.1:56039
```

当前没有 node 级统一 DiskCache manager；每个 Python client 进程仍维护自己的 DiskCache 状态、水位线、LRU 和 evict 选择。`falcon_client` FUSE 进程只作为 brpc data node 和挂载探活使用，不承担统一淘汰管理。

## 3. 前置条件

一键脚本默认会自动执行 benchmark 需要的前置准备，不再要求先手动完成 FalconFS 编译安装。默认行为如下：

```text
PREPARE_ENV=1
PREPARE_PG=auto
PREPARE_FALCON=1
PREPARE_INSTALL=1
PREPARE_INTERNAL_BENCH=1
```

执行 Falcon 相关场景时，脚本会先检查 `python3/awk/sed/findmnt/df/ss`，再检查 PostgreSQL 工具链。如果 `pg_config/pg_ctl/psql` 不存在，`PREPARE_PG=auto` 会自动从 `third_party/postgres` 编译并安装到 `/usr/local/pgsql`。随后脚本会执行：

```bash
./build.sh build falcon
sudo ./build.sh install falcon
ninja -C build falcon_internal_perf
```

如果需要强制重编 PostgreSQL，可以设置 `PREPARE_PG=1`；如果环境中已经准备好 PostgreSQL 且不希望脚本处理，可以设置 `PREPARE_PG=0`。如果需要先清理 Falcon 构建目录再编译，可以设置 `PREPARE_CLEAN=1`。

fio 相关场景会检查 `fio` 是否存在，但不会自动安装系统包。

确认 Python internal 扩展已安装或可从源码目录导入：

```bash
python3 - <<'PY'
import sys
sys.path.insert(0, "/home/hx/code/falconfs/python_interface")
import pyfalconfs
print(pyfalconfs.__file__)
PY
```

确认 benchmark helper 可构建：

```bash
ninja -C build falcon_internal_perf
test -x build/internal_perf/falcon_internal_perf
```

## 4. 一键执行

默认一键执行 `all` 会跑 Python internal 场景和 fio 本地基准；只想快速验证 Python internal 场景时使用 `python`。另一台服务器如果 NVMe 盘挂载在 `/data4/hxing`，建议显式设置 `BENCHMARK_ROOT=/data4/hxing`。默认情况下，metadata workspace 也会跟随 `BENCHMARK_ROOT`，即使用 `/data4/hxing/metadata`，不需要手动修改 `deploy/meta/falcon_meta_config.sh`：

```bash
cd ~/code/falconfs

BENCHMARK_ROOT=/data4/hxing \
CLIENTS=4 \
FILES=6000 \
UNLINK_FILES=6000 \
P5_UNLINK_FILES=200000 \
MAX_LOCAL_DISK_SIZE=16 \
FALCON_LOG_LEVEL=INFO \
EVICT_THRESHOLD=0.90 \
FILE_SIZE=2097152 \
WAIT_SEC=45 \
bash tools/run_pyfalcon_client_benchmark.sh all
```

`all/python/fio` 会按 case 独立执行。某个 case 失败时，脚本会记录失败状态并继续执行后续 case，最后生成 summary，并在 `run.log` 末尾列出失败项。`all` 是完整口径，会额外跑 P-DIO、fio B-1..B-6、fio matrix 和 B-DIO。

只跑核心 Python internal 场景或单个场景：

```bash
bash tools/run_pyfalcon_client_benchmark.sh python
bash tools/run_pyfalcon_client_benchmark.sh P-LOCK
bash tools/run_pyfalcon_client_benchmark.sh P-1
bash tools/run_pyfalcon_client_benchmark.sh P-2
bash tools/run_pyfalcon_client_benchmark.sh P-3
bash tools/run_pyfalcon_client_benchmark.sh P-5
bash tools/run_pyfalcon_client_benchmark.sh P-R1
bash tools/run_pyfalcon_client_benchmark.sh P-RW
bash tools/run_pyfalcon_client_benchmark.sh P-RWE

# 需要时单独跑 direct I/O 探测或完整基准
bash tools/run_pyfalcon_client_benchmark.sh P-DIO
bash tools/run_pyfalcon_client_benchmark.sh all

# 只跑 fio 基准
bash tools/run_pyfalcon_client_benchmark.sh fio
bash tools/run_pyfalcon_client_benchmark.sh B-1
bash tools/run_pyfalcon_client_benchmark.sh B-5
bash tools/run_pyfalcon_client_benchmark.sh B-MATRIX
bash tools/run_pyfalcon_client_benchmark.sh B-DIO
```

## 5. 参数说明

| 参数 | 默认值 | 说明 |
| --- | ---: | --- |
| `DEFAULT_BENCHMARK_ROOT` | `/data4/hxing` | 未显式设置 `BENCHMARK_ROOT` 时，如果该目录存在且可写，会自动作为测试根目录 |
| `BENCHMARK_ROOT` | 空 | 测试根目录；设置为 `/data4/hxing` 后，默认 `OUT_DIR`、`CACHE_ROOT`、`FIO_DIR` 都在 `/data4/hxing` 下 |
| `OUT_DIR` | `/tmp/pyfalcon_client_benchmark_<timestamp>` 或 `$BENCHMARK_ROOT/pyfalcon_client_benchmark_<timestamp>` | 输出目录，保留最终结果和日志 |
| `CLIENTS` | 4 | 每组 Python client 进程数 |
| `FILES` | 6000 | 写入文件数 |
| `READ_FILES` | `$FILES` | P-R1 的读数据集文件数 |
| `PINNED_READ_FILES` | `256` | P-RW/P-RWE 每个 reader 预打开并 pin 住的读热集文件数 |
| `MIXED_DURATION_SEC` | `120` | P-5/P-RW/P-RWE 固定时长混合压力窗口 |
| `HOT_READ_WINDOW` | `1024` | 保留给历史 hot-window 风险验证；正式 P-RW/P-RWE 不使用 |
| `HOT_READ_LAG` | `128` | 保留给历史 hot-window 风险验证；正式 P-RW/P-RWE 不使用 |
| `HOT_READ_MIN_FILES` | `$HOT_READ_WINDOW` | 保留给历史 hot-window 风险验证；正式 P-RW/P-RWE 不使用 |
| `UNLINK_FILES` | 6000 | P-2 和 fio B-5 使用的删除文件数 |
| `P5_UNLINK_FILES` | 200000 | P-5 固定时长边写边删场景请求预创建的待删文件数；脚本会按当前可用空间自动下调实际预置数量 |
| `P5_WRITE_FILES` | `$P5_UNLINK_FILES` | P-5 writer 在固定时长窗口内最多创建的文件数，默认与删除集规模一致，避免 writer 提前跑完 |
| `P5_PREPARE_MAX_AVAIL_RATIO` | 0.45 | P-5 预置待删数据集最多使用当前真实可用空间的比例，避免 prepare 写满盘或长时间卡住 |
| `RESULT_TIMEOUT_SEC` | 120 | worker 结束后等待结果上报的基础超时时间 |
| `PREPARE_TIMEOUT_SEC` | 0 | prepare 阶段结果等待时间；0 表示按预置数据量和 `PREPARE_MIN_MIB_PER_SEC` 自动估算 |
| `PREPARE_MIN_MIB_PER_SEC` | 128 | 自动估算 prepare 超时时使用的最低聚合写入吞吐，单位 MiB/s |
| `FILE_SIZE` | 2097152 | 单文件大小，默认 2MiB |
| `WAIT_SEC` | 45 | P-3 写完后等待 evict 的时间 |
| `FIO_SIZE` | 2G | B-1/B-2 单文件 fio 基准每个 job 的数据量 |
| `FIO_NUMJOBS_LIST` | `1 4 8 16` | B-MATRIX 使用的 fio 并发矩阵 |
| `FIO_MATRIX_SIZE` | `$FIO_SIZE` | B-MATRIX 每个 job 的数据量 |
| `FIO_UNALIGNED_SIZE` | 64M | B-DIO 未对齐 direct I/O 探测的数据量 |
| `FIO_DIR` | `/tmp/pyfalcon_fio_baseline` 或 `$BENCHMARK_ROOT/pyfalcon_fio_baseline` | fio 临时工作目录，结束后清理 |
| `CACHE_ROOT` | `/tmp/falcon_cache` 或 `$BENCHMARK_ROOT/falcon_cache` | Falcon DiskCache 本地 cache 目录，结束后清理 |
| `REQUIRE_NVME` | 1 | 是否要求 `OUT_DIR/CACHE_ROOT/FIO_DIR/metadata workspace` 都在 NVMe 设备上；临时非 NVMe 测试可设为 0 |
| `WRITE_THRESHOLD` | 1 | P-1/P-2/P-5 使用的 `STORAGE_THRESHOLD` |
| `EVICT_THRESHOLD` | 0.72 | P-3/P-RWE 使用的 `STORAGE_THRESHOLD`；正式 NVMe benchmark 推荐显式设置为 0.90 |
| `MAX_LOCAL_DISK_SIZE` | 16 | 写入每个 Python client / FUSE data node case config 的 `max_local_disk_size`，单位 GiB；本机快速验证可设为 1 |
| `FALCON_LOG_LEVEL` | 空 | 可选：写入本轮临时 client config 的 `falcon_log_level`；正式 benchmark 推荐设为 INFO，便于 summary 采集 `DiskCache::Cleanup()` 和 `CleanupForEvict()` 成功日志 |
| `DISKCACHE_EVICTION_RATIO` | 0.1 | DiskCache 后台 evict 每轮候选比例，默认 0.1；正式 benchmark 不需要配置，仅在强制验证前台路径时可临时设为 0 |
| `AUTO_EVICT_CONFIG` | 1 | 是否自动按 `MAX_LOCAL_DISK_SIZE` 逻辑容量和当前真实可用空间计算 P-3/P-RWE 需要写入的文件数 |
| `AUTO_EVICT_WRITE_RATIO` | 0.01 | 自动模式下目标 P-3 写入量至少覆盖文件系统总容量的比例 |
| `AUTO_EVICT_MIN_WRITE_BYTES` | 12884901888 | 自动模式下 P-3 最小目标写入量，默认 12GiB |
| `AUTO_EVICT_MAX_WRITE_BYTES` | 0 | 自动模式下 P-3 写入量硬上限；0 表示不设置绝对上限，保证能按磁盘空间触发 evict |
| `AUTO_EVICT_MAX_AVAIL_RATIO` | 0.60 | 自动模式最多使用当前可用空间的比例来增加 P-3/P-RW/P-RWE 写入量，避免真实空间足够时被过保守限制拦截 |
| `AUTO_EVICT_TRIGGER_RATIO` | 0.90 | 自动模式按“有效进入 cache 的写入占比”反推 P-3/P-RWE 写入量；值越小，计划写入越多，默认保留足够余量确保实际 cache 峰值跨过 evict 水位 |
| `AUTO_EVICT_INIT_MARGIN_BYTES` | 1073741824 | 自动模式给 Falcon metadata 初始化预留的空间余量 |
| `AUTO_EVICT_START_MARGIN_RATIO` | 0.12 | 自动模式为满足 Falcon 启动空闲空间检查，在当前已用比例上额外预留的比例 |
| `AUTO_EVICT_MAX_THRESHOLD` | 0.98 | 自动模式计算出的水位线上限 |
| `CONFIG_FILE_PATH` | `/usr/local/falconfs/falcon_client/config/config.json` | benchmark 读取的基础配置文件；每个 P case 会在 `$OUT_DIR/python` 下生成覆盖后的运行配置 |
| `BENCHMARK_CLUSTER_VIEW` | `127.0.0.1:56039` | benchmark 专用 store 节点列表，默认指向本机 FUSE brpc data node 的 RemoteIOServer |
| `BRPC_DATA_ENDPOINT` | `127.0.0.1:56039` | Python internal client 使用的 brpc data endpoint；脚本仍兼容旧 `DISKCACHE_MANAGER_ENDPOINT` 环境变量别名 |
| `BRPC_DATA_LISTEN_ENDPOINT` | `0.0.0.0:56039` | FUSE brpc data node 监听地址，默认不更换端口；脚本仍兼容旧 `DISKCACHE_MANAGER_LISTEN_ENDPOINT` 环境变量别名 |
| `PYTHON_INTERFACE` | `$ROOT_DIR/python_interface` | `pyfalconfs` Python 包路径 |
| `PREPARE_ENV` | 1 | 是否在执行 case 前自动准备前置条件；设为 0 时跳过自动准备 |
| `PREPARE_PG` | `auto` | PostgreSQL 准备模式；`auto` 表示缺少 `pg_config/pg_ctl/psql` 时自动编译安装，`1` 表示强制编译安装，`0` 表示只检查不编译 |
| `PREPARE_FALCON` | 1 | 是否自动执行 `./build.sh build falcon` |
| `PREPARE_INSTALL` | 1 | 是否自动执行 `sudo ./build.sh install falcon` |
| `PREPARE_INTERNAL_BENCH` | 1 | 是否自动构建 `build/internal_perf/falcon_internal_perf` |
| `PREPARE_CLEAN` | 0 | 是否在构建 Falcon 前执行 `./build.sh clean falcon` |
| `PREPARE_FIX_BUILD_PERMS` | 1 | build 目录由 root/其他用户遗留导致不可写时，是否自动 `sudo chown -R` 修复仓库内 `BUILD_DIR` 权限 |
| `CASE_SPACE_MARGIN_BYTES` | 1073741824 | P-1/P-2/P-5/P-R1/P-RW 这类非 evict 回收场景的写入空间预检查保留余量，默认 1GiB |
| `BENCHMARK_META_WORKSPACE` | `$BENCHMARK_ROOT` 或 `$HOME` | meta 服务使用的 workspace；脚本会以该 HOME 启停 meta，使 metadata 默认落在测试盘下 |

P-5 的正式阶段总进程数是 `CLIENTS * 2`。例如 `CLIENTS=4` 时，是 4 个 writer client 加 4 个 deleter client。P-5 默认使用固定时长口径：writer 在 `MIXED_DURATION_SEC` 内持续创建并写入新文件，deleter 同时删除预创建的旧文件。报告中的 `writer.files_per_sec` 和 `deleter.files_per_sec` 分别表示同一时间窗口内两个方向各自完成的真实吞吐。

P-5 会先预创建待删数据集，再启动 writer/deleter 固定时长混合压力。脚本会根据当前 `CACHE_ROOT` 可用空间和 `P5_PREPARE_MAX_AVAIL_RATIO` 自动下调实际预置文件数，避免在 128GiB/64GiB 这类节点上按默认 `P5_UNLINK_FILES=200000` 预置约 390GiB 数据导致 prepare 阶段卡住。writer 默认最多写 `P5_WRITE_FILES=$P5_UNLINK_FILES` 个文件，用于避免固定窗口内 writer 过早耗尽任务。如果结果中 `delete_dataset_exhausted=true` 或 deleter 的 `stop_reasons` 包含 `max_files`，说明待删数据集提前耗尽；这轮可以用于观察写入是否受并发 unlink 影响，但删除吞吐不是上限，需要增加可用空间、提高 `P5_PREPARE_MAX_AVAIL_RATIO` 或缩短 `MIXED_DURATION_SEC` 后重跑。

## 6. P-3 自动 evict 配置

默认 `AUTO_EVICT_CONFIG=1`。正式 NVMe benchmark 推荐显式设置 `MAX_LOCAL_DISK_SIZE=16`、`EVICT_THRESHOLD=0.90`，后台 evict 候选比例使用代码默认 `DISKCACHE_EVICTION_RATIO=0.1`，不需要在一键命令里单独配置。P-3/P-RWE 执行前脚本会在清理环境后读取 `CACHE_ROOT` 所在文件系统的空间信息，并结合 `MAX_LOCAL_DISK_SIZE` 逻辑容量规划写入文件数。当前没有 node 级统一 DiskCache manager；这里的 `MAX_LOCAL_DISK_SIZE` 会写入每个 case config，表示每个参与进程自己的 DiskCache 逻辑容量。最终使用值会记录在 `evict_config.txt` 的 `case_P-3_config_max_local_disk_size` / `case_P-RWE_config_max_local_disk_size` 字段中。

```text
total_bytes
used_bytes
avail_bytes
```

然后根据 `FILES * FILE_SIZE`、`MAX_LOCAL_DISK_SIZE` 逻辑容量、inode 使用率和最小写入量计算 P-3/P-RWE 的实际写入文件数。基础目标写入量为：

```text
max(FILES * FILE_SIZE, logical_total_bytes * AUTO_EVICT_WRITE_RATIO, AUTO_EVICT_MIN_WRITE_BYTES)
```

如果 `AUTO_EVICT_MAX_WRITE_BYTES > 0`，脚本会把它当成硬上限；当实际需要写入的数据量超过这个上限时，脚本会失败并提示调大上限或降低启动安全余量。默认 `AUTO_EVICT_MAX_WRITE_BYTES=0`，表示不设置绝对上限。

如果实际需要写入的数据量超过当前可用空间的 `AUTO_EVICT_MAX_AVAIL_RATIO`，脚本会失败并提示需要更多可用空间，避免为了触发 evict 写满盘。当前默认值是 `0.60`；如果 16GiB/0.90 这类配置只差少量空间规划失败，优先调高该比例或降低 `MAX_LOCAL_DISK_SIZE`，不要把失败结果解释成 Falcon evict 运行时失败。

Falcon 启动阶段要求当前 block 可用比例和 inode 可用比例都高于 `freeRatio = 1 - STORAGE_THRESHOLD`。DiskCache 使用 `statfs.f_bavail` 计算 block 可用比例，所以自动模式按 `block_start_used_ratio = 1 - avail_bytes/total_bytes`，以及 `inode_start_used_ratio = 1 - inodes_avail/inodes_total` 计算启动水位线，并保证水位线至少高于 `max(block_start_used_ratio, inode_start_used_ratio) + AUTO_EVICT_START_MARGIN_RATIO`，默认额外预留 12 个百分点。推荐命令显式使用 `EVICT_THRESHOLD=0.90`，自动模式不再动态改写 threshold，而是自动提高文件数，直到计划写入量足够跨过这个固定水位并触发 DiskCache evict。以 `MAX_LOCAL_DISK_SIZE=16`、`EVICT_THRESHOLD=0.90`、`AUTO_EVICT_TRIGGER_RATIO=0.90` 为例，脚本会按“至少约 14.4GiB / 0.90 有效写入”规划单进程写入量，用来覆盖多进程共享 cache root、实际 cache 保留比例低于计划写入量等情况。实际使用的参数会写入：

```text
$OUT_DIR/evict_config.txt
```

如果只想固定使用手动水位线，可以关闭自动模式：

```bash
AUTO_EVICT_CONFIG=0 EVICT_THRESHOLD=0.90 MAX_LOCAL_DISK_SIZE=16 FALCON_LOG_LEVEL=INFO bash tools/run_pyfalcon_client_benchmark.sh P-3
```

## 7. 输出文件

完整执行后会生成：

```text
$OUT_DIR/run.log
$OUT_DIR/benchmark_summary.md
$OUT_DIR/benchmark_summary.log
$OUT_DIR/storage_info.txt
$OUT_DIR/evict_config.txt
$OUT_DIR/python/P-1.json
$OUT_DIR/python/P-2.json
$OUT_DIR/python/P-3.json
$OUT_DIR/python/P-5.json
$OUT_DIR/python/P-1-brpc-data-node.log
$OUT_DIR/python/P-2-brpc-data-node.log
$OUT_DIR/python/P-3-brpc-data-node.log
$OUT_DIR/python/P-5-brpc-data-node.log
$OUT_DIR/python/P-3-monitor.log
$OUT_DIR/python/P-RWE-monitor.log
$OUT_DIR/python/P-3-cache_state.txt
$OUT_DIR/python/P-RWE-cache_state.txt
$OUT_DIR/fio/B-1.json
$OUT_DIR/fio/B-2.json
$OUT_DIR/fio/B-2-prepare.json
$OUT_DIR/fio/B-3.json
$OUT_DIR/fio/B-4.json
$OUT_DIR/fio/B-5-create.json
$OUT_DIR/fio/B-5.json
```

其中：

| 文件 | 说明 |
| --- | --- |
| `run.log` | 一键脚本执行过程日志，包含每个场景启动、清理和汇总路径 |
| `benchmark_summary.md` | 类似性能测试结果文档的最终汇总表；`all` 包含 Python internal 和 fio 基准，`python` 只包含 Python internal 场景 |
| `benchmark_summary.log` | 与 `benchmark_summary.md` 内容一致，便于直接归档或 `cat` 查看 |
| `storage_info.txt` | 记录 `OUT_DIR/CACHE_ROOT/FIO_DIR/metadata workspace` 对应的挂载点、设备和文件系统类型 |
| `evict_config.txt` | 记录 P-3 自动计算出的 `threshold`、实际 `files`、磁盘总量/已用量/可用量、inode 总量/已用量/可用量、计划写入量；失败后 cache root 被清理时也以这里为准 |
| `python/P-*-config.json` | 每个 Python internal case 实际使用的运行配置，会覆盖 `falcon_cache_root`、`falcon_cluster_view` 和 `falcon_log_dir` |
| `python/P-*.log` | 记录对应 Python internal case 的 stdout/stderr，case 失败时会自动打印尾部到 `run.log` |
| `python/P-*-meta.log` | 记录对应 case 启动 Falcon meta service 的输出，case 失败时会自动打印尾部到 `run.log` |
| `python/P-3-cache_state.txt` / `python/P-RWE-cache_state.txt` | P-3/P-RWE 结束后、清理 cache 前记录的 cache 目录大小和文件数 |
| `python/P-3-monitor.log` / `python/P-RWE-monitor.log` | P-3/P-RWE 执行期间的进程 I/O、cache 目录体积和卡住判断信息 |

重点字段：

| 字段 | 含义 |
| --- | --- |
| `files_per_sec` | 文件吞吐 |
| `mib_per_sec` | 按文件大小折算的数据吞吐 |
| `latency_p50_sec` | 单次操作 p50 延迟 |
| `latency_p95_sec` | 单次操作 p95 延迟 |
| `latency_p99_sec` | 单次操作 p99 延迟 |
| `error_count` | 错误数，正常应为 0 |
| `per_client` | 每个 Python client 的文件数、耗时和错误 |

P-2 的删除结果在：

```text
unlink.files_per_sec
unlink.mib_per_sec
unlink.latency_p99_sec
```

P-5 的写入和删除结果分别在：

```text
writer.files_per_sec
writer.mib_per_sec
deleter.files_per_sec
deleter.mib_per_sec
```

P-RW/P-RWE 会先预打开 `PINNED_READ_FILES * CLIENTS` 个读热集文件；lease 方案下这些打开文件会持有 metadata lease，并在 case 收尾 close 时集中释放。NVMe 正式测试可以保留默认热集规模，本机或非 NVMe 完整性验证如果出现 metadata brpc timeout，可临时设置 `PINNED_READ_FILES=64 RESULT_TIMEOUT_SEC=120` 先验证脚本完整链路。

P-RW/P-RWE 的总览表中，writer 写吞吐使用 active 口径，即 `writer` 实际写入文件数除以 `writer.max_worker_elapsed_sec`。这是因为 writer 可能提前达到 `max_files` 并停止，不能用 120s 混合窗口平均值代表真实写能力。JSON 明细中会同时保留：

```text
writer.active_files_per_sec
writer.active_mib_per_sec
writer.window_files_per_sec
writer.window_mib_per_sec
writer.max_worker_elapsed_sec
```

P-3 的 evict 删除明细需要结合 Falcon 日志中的 `DiskCache::Cleanup()` 聚合。

fio 结果字段：

| 文件 | 说明 |
| --- | --- |
| `B-1.json` | 单文件 direct 写基准 |
| `B-2.json` | 单文件 direct 读基准 |
| `B-3.json` | 多文件 direct 写基准 |
| `B-4.json` | 多文件 direct 读基准 |
| `B-5.json` | 本地多文件删除基准，字段包含 `delete_files_per_sec` 和 `delete_mib_per_sec` |

## 8. 环境清理

脚本每个 Python internal case 前都会清理：

```text
$CACHE_ROOT
$MOUNT_DIR
<falcon_meta_config.sh 中 workspace>/metadata
55500/55510/55520/55530/56039 默认端口
```

如果 `falcon_meta_config.sh` 中 `workspace=/data4/hxing`，实际清理的是 `/data4/hxing/metadata`，不是 `$HOME/metadata`。

脚本结束后一定会清理：

```text
$FIO_DIR
$OUT_DIR/work_*
```

如果执行的是 `python`、`P-*` 或 `all` 这类会启动 Falcon 的场景，脚本结束后还会清理：

```text
$CACHE_ROOT
<falcon_meta_config.sh 中 workspace>/metadata
```

如果只执行 `fio` 或 `B-*`，脚本不会清理 Falcon metadata/cache，只会清理 fio 临时工作目录。最终只保留 `$OUT_DIR` 下的结果文件、JSON、`run.log`、`benchmark_summary.md`、`benchmark_summary.log` 和 `storage_info.txt`。

清理函数会拒绝删除 `/`、`/tmp`、`$HOME` 和 `BENCHMARK_ROOT` 本身，只删除这些目录下的专用子目录。手工检查：

```bash
ss -ltnp | grep -E ':(55500|55510|55520|55530|56039)([[:space:]]|$)' || true
```

## 9. NVMe 检查

默认 `REQUIRE_NVME=1`。如果没有显式传 `BENCHMARK_ROOT`，脚本会先尝试使用可写的 `/data4/hxing`；如果这个目录不存在或不可写，才会回落到 `/tmp`，此时 NVMe 检查会失败并提示设置 `BENCHMARK_ROOT=/data4/hxing`。脚本启动后会用 `findmnt -T` 和 `lsblk` 检查以下路径所在设备：

```text
OUT_DIR
CACHE_ROOT
FIO_DIR
falcon_meta_config.sh 中 workspace 所在目录
```

如果任一目录不在 NVMe 设备上，脚本会直接退出，避免把结果误测到系统盘或非目标盘。确认在 `/data4/hxing` 上测试时，推荐命令：

```bash
BENCHMARK_ROOT=/data4/hxing bash tools/run_pyfalcon_client_benchmark.sh python
```

仅做功能 smoke、不关心磁盘类型时，可以临时关闭检查：

```bash
REQUIRE_NVME=0 bash tools/run_pyfalcon_client_benchmark.sh B-5
```

## 10. 常见问题

如果看到：

```text
Not connected to 127.0.0.1:56039
```

说明 FUSE brpc data node 没有启动成功，或 56039 被其他进程占用。检查：

```bash
ss -ltnp | grep -E ':(56039)([[:space:]]|$)' || true
cat "$OUT_DIR"/python/*-brpc-data-node.log
```

如果 P-3 在 `WAIT_SEC=45` 内没有清完所有 cache，这是当前 evict 后台调度和水位推进的表现，不代表 benchmark 失败。以 JSON 的写入结果、Falcon `DiskCache::Cleanup()` 日志，以及 evict 前置 `UNLINK_IF_INODE_MATCH(path, expected_inode)` 条件删除统计共同判断。

当前 evict metadata 删除条件是 `path + expected_inode + no active lease`。多个 Python client 或 FUSE 进程打开同一个 inode 时，metadata `lease_count` 会保护该 inode；evict listener 收到 `LEASE_CONFLICT` 后不会删除本地 cache，DiskCache 会恢复该条目并等待后续 cleanup 重试。因此报告中 metadata unlink failed 如果集中为 `LEASE_CONFLICT`，通常表示读写 lease 仍活跃，而不是本地 remove 失败。

## 11. 新增混合读写 evict 与 direct I/O 验证

为了在非 NVMe 或本地功能验证时缩短时间，可以先用小规模参数跑核心 Python internal 流程：

```bash
REQUIRE_NVME=0 BENCHMARK_ROOT=/tmp/falconfs_local_benchmark CLIENTS=2 FILES=64 READ_FILES=64 UNLINK_FILES=64 P5_UNLINK_FILES=512 WAIT_SEC=5 MIXED_DURATION_SEC=30 bash tools/run_pyfalcon_client_benchmark.sh python
```

在 NVMe 服务器正式测试核心 Python internal 场景时，建议恢复 4 client，并给 P-5 准备足够大的删除集：

```bash
BENCHMARK_ROOT=/data4/hxing CLIENTS=4 FILES=6000 READ_FILES=6000 UNLINK_FILES=6000 P5_UNLINK_FILES=200000 WAIT_SEC=45 MIXED_DURATION_SEC=120 bash tools/run_pyfalcon_client_benchmark.sh python
```

混合场景判断顺序：先看 P-R1/P-1 的 Falcon 纯读纯写，再看固定时长 P-5/P-RW 的写删、读写基线，最后用固定时长 P-RWE 对比 P-RW，评估 evict 对已打开读热集和持续写入的额外影响。P-RW/P-RWE 的 JSON 中必须确认 `timed=true`、`read_pattern=pinned_read_set`；总览写吞吐看 active 口径，窗口平均值只作为固定时长占用程度参考；P-5 如果 `delete_dataset_exhausted=false`，删除吞吐可作为完整窗口结果，如果为 `true`，只能说明有限待删数据集被删完，不能当作删除上限。需要重新采 fio 基准时可单独执行 `fio`，或执行完整 `all`。

需要验证 direct I/O 未对齐行为时单独运行 `P-DIO`、`B-DIO`，或执行完整 `all`；未对齐写失败不会被当成脚本失败。

## 12. 最终整合日志

一键脚本执行结束后会自动生成：

```text
$OUT_DIR/final_report.log
```

这个文件用于跨服务器传递结果，内容已经整合以下信息：

| 内容 | 来源 |
| --- | --- |
| 总览结果 | `$OUT_DIR/benchmark_summary.md` |
| 存储设备信息 | `$OUT_DIR/storage_info.txt` |
| evict 自动水位配置 | `$OUT_DIR/evict_config.txt` |
| 执行过程 | `$OUT_DIR/run*.log` 的尾部 |
| P-3/P-RWE 运行监控 | `$OUT_DIR/python/P-3-monitor.log`、`$OUT_DIR/python/P-RWE-monitor.log` |
| P-3/P-RWE cache 状态 | `$OUT_DIR/python/P-3-cache_state.txt`、`$OUT_DIR/python/P-RWE-cache_state.txt` |
| evict 原始日志摘录 | 从 `$OUT_DIR/work_P-*`、`$OUT_DIR/python/P-*-falcon-log` 和 `$OUT_DIR/falcon_logs/P-*` 中摘取 `DiskCache::Cleanup()` / `CleanupForEvict()` / `FalconEvictUnlinkListener stopped` 相关行 |
| 关键 Python JSON | `$OUT_DIR/python/P-3.json`、`$OUT_DIR/python/P-5.json`、`$OUT_DIR/python/P-RWE.json` |
| direct I/O 探测 | 执行 `P-DIO`、`B-DIO` 或完整 `all` 时生成对应 JSON/stderr/stdout |
| 文件清单 | `python` 主要是 `$OUT_DIR/python/*.json`；执行 `fio/all` 时还会包含 `$OUT_DIR/fio/*.json` |

因此在另一台服务器完成 NVMe 测试后，只需要把 `$OUT_DIR/final_report.log` 发回来即可，不需要分别复制 summary、storage、evict config 和各个 JSON 文件。

脚本结束时会打印：

```text
final report: <OUT_DIR>/final_report.log
```

如果需要手动重新生成 summary 后再重新生成 final report，直接重新跑对应场景或重新执行一键脚本即可；`final_report.log` 会被覆盖为最新内容。
