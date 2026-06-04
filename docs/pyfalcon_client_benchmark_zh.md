# Python internal client 并发 benchmark 使用说明

本文说明如何在服务器上通过 Python internal API 调用 Falcon 内部接口做并发性能测试。该 benchmark 不通过 FUSE 挂载点发起业务操作。

## 1. 测试目标

benchmark 覆盖 Python internal API 和 fio 本地基准两类场景。Python internal API 覆盖 4 个场景：

| 编号 | 场景 | 并发模型 |
| --- | --- | --- |
| P-1 | 纯写 | 4 个 Python 进程，每个进程 1 个 `pyfalconfs.Client` |
| P-2 | 纯删除 | 先 4 client 预创建文件，再 4 client 并发 `FalconUnlink` |
| P-3 | 写触发 evict | 4 writer client 并发写，DiskCache 后台 cleanup 删除 cache |
| P-5 | 边写边删 | 4 writer client + 4 deleter client 同时运行 |

fio 本地基准覆盖 5 个场景：

| 编号 | 场景 | 说明 |
| --- | --- | --- |
| B-1 | 单文件 direct 顺序写 | `fio --rw=write`，`numjobs=CLIENTS` |
| B-2 | 单文件 direct 顺序读 | 先预写，再 `fio --rw=read` |
| B-3 | 多文件 direct 顺序写 | 按 `FILES/CLIENTS` 拆分多文件 |
| B-4 | 多文件 direct 顺序读 | 读取 B-3 创建的多文件 |
| B-5 | 本地多文件删除 | 按 `UNLINK_FILES/CLIENTS` 先用 fio 创建待删文件，再用并发 `rm -f` 删除 |

Python client 调用链：

```text
pyfalconfs.Client
-> _pyfalconfs_internal.cpp
-> FalconCreate / FalconWrite / FalconClose / FalconUnlink
```

## 2. 为什么需要 idle_server

`_pyfalconfs_internal.cpp` 只执行 `GetInit().Init()` 和 `FalconInit()`，不会启动 `RemoteIOServer`。写入路径需要连接配置中的 `falcon_cluster_view`，默认是：

```json
"falcon_cluster_view": ["127.0.0.1:56039", "0.0.0.0:56039"]
```

因此 benchmark 会额外启动：

```bash
build/internal_perf/falcon_internal_perf --mode idle_server
```

这个进程只负责提供稳定的 `56039` RemoteIOServer。业务操作仍然由 Python 进程通过 internal API 发起，不经过 `/tmp/falcon_mnt`。

## 3. 前置条件

先完成 FalconFS 编译和安装。示例：

```bash
cd ~/code/falconfs

cd third_party/postgres/
make distclean || true
./configure --prefix=/usr/local/pgsql --without-icu --enable-debug
make -j"$(nproc)"
sudo make install

cd ~/code/falconfs
./build.sh clean pg
./build.sh build pg
sudo ./build.sh install pg
sudo ./build.sh clean falcon
./build.sh build falcon
sudo ./build.sh install falcon
```

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

完整执行 P-1/P-2/P-3/P-5 和 fio B-1..B-5。另一台服务器如果 NVMe 盘挂载在 `/data4/hxing`，建议显式设置 `BENCHMARK_ROOT=/data4/hxing`：

```bash
cd ~/code/falconfs

BENCHMARK_ROOT=/data4/hxing \
CLIENTS=4 \
FILES=6000 \
UNLINK_FILES=6000 \
FILE_SIZE=2097152 \
WAIT_SEC=45 \
FIO_SIZE=2G \
bash tools/run_pyfalcon_client_benchmark.sh all
```

`all/python/fio` 会按 case 独立执行。某个 case 失败时，脚本会记录失败状态并继续执行后续 case，最后生成 summary，并在 `run.log` 末尾列出失败项。

只跑 Python internal 场景或单个场景：

```bash
bash tools/run_pyfalcon_client_benchmark.sh python
bash tools/run_pyfalcon_client_benchmark.sh P-1
bash tools/run_pyfalcon_client_benchmark.sh P-2
bash tools/run_pyfalcon_client_benchmark.sh P-3
bash tools/run_pyfalcon_client_benchmark.sh P-5

# 只跑 fio 基准
bash tools/run_pyfalcon_client_benchmark.sh fio
bash tools/run_pyfalcon_client_benchmark.sh B-1
bash tools/run_pyfalcon_client_benchmark.sh B-5
```

## 5. 参数说明

| 参数 | 默认值 | 说明 |
| --- | ---: | --- |
| `DEFAULT_BENCHMARK_ROOT` | `/data4/hxing` | 未显式设置 `BENCHMARK_ROOT` 时，如果该目录存在且可写，会自动作为测试根目录 |
| `BENCHMARK_ROOT` | 空 | 测试根目录；设置为 `/data4/hxing` 后，默认 `OUT_DIR`、`CACHE_ROOT`、`FIO_DIR` 都在 `/data4/hxing` 下 |
| `OUT_DIR` | `/tmp/pyfalcon_client_benchmark_<timestamp>` 或 `$BENCHMARK_ROOT/pyfalcon_client_benchmark_<timestamp>` | 输出目录，保留最终结果和日志 |
| `CLIENTS` | 4 | 每组 Python client 进程数 |
| `FILES` | 6000 | 写入文件数 |
| `UNLINK_FILES` | 6000 | 删除文件数 |
| `FILE_SIZE` | 2097152 | 单文件大小，默认 2MiB |
| `WAIT_SEC` | 45 | P-3 写完后等待 evict 的时间 |
| `FIO_SIZE` | 2G | B-1/B-2 单文件 fio 基准每个 job 的数据量 |
| `FIO_DIR` | `/tmp/pyfalcon_fio_baseline` 或 `$BENCHMARK_ROOT/pyfalcon_fio_baseline` | fio 临时工作目录，结束后清理 |
| `CACHE_ROOT` | `/tmp/falcon_cache` 或 `$BENCHMARK_ROOT/falcon_cache` | Falcon DiskCache 本地 cache 目录，结束后清理 |
| `REQUIRE_NVME` | 1 | 是否要求 `OUT_DIR/CACHE_ROOT/FIO_DIR/metadata workspace` 都在 NVMe 设备上；临时非 NVMe 测试可设为 0 |
| `WRITE_THRESHOLD` | 1 | P-1/P-2/P-5 使用的 `STORAGE_THRESHOLD` |
| `EVICT_THRESHOLD` | 0.72 | `AUTO_EVICT_CONFIG=0` 时 P-3 使用的固定 `STORAGE_THRESHOLD` |
| `AUTO_EVICT_CONFIG` | 1 | 是否自动按 `CACHE_ROOT` 所在文件系统容量和当前已用空间计算 P-3 的水位线和文件数 |
| `AUTO_EVICT_WRITE_RATIO` | 0.01 | 自动模式下目标 P-3 写入量至少覆盖文件系统总容量的比例 |
| `AUTO_EVICT_MIN_WRITE_BYTES` | 12884901888 | 自动模式下 P-3 最小目标写入量，默认 12GiB |
| `AUTO_EVICT_MAX_WRITE_BYTES` | 0 | 自动模式下 P-3 写入量硬上限；0 表示不设置绝对上限，保证能按磁盘空间触发 evict |
| `AUTO_EVICT_MAX_AVAIL_RATIO` | 0.60 | 自动模式最多使用当前可用空间的比例来增加 P-3 写入量 |
| `AUTO_EVICT_TRIGGER_RATIO` | 0.90 | 自动模式把水位线放在计划写入量靠后的阶段，减少为触发 evict 需要额外写入的数据量 |
| `AUTO_EVICT_INIT_MARGIN_BYTES` | 1073741824 | 自动模式给 Falcon metadata 初始化预留的空间余量 |
| `AUTO_EVICT_START_MARGIN_RATIO` | 0.12 | 自动模式为满足 Falcon 启动空闲空间检查，在当前已用比例上额外预留的比例 |
| `AUTO_EVICT_MAX_THRESHOLD` | 0.98 | 自动模式计算出的水位线上限 |
| `CONFIG_FILE_PATH` | `/usr/local/falconfs/falcon_client/config/config.json` | Python client 使用的配置文件 |
| `PYTHON_INTERFACE` | `$ROOT_DIR/python_interface` | `pyfalconfs` Python 包路径 |

P-5 的正式阶段总进程数是 `CLIENTS * 2`。例如 `CLIENTS=4` 时，是 4 个 writer client 加 4 个 deleter client。

## 6. P-3 自动 evict 配置

默认 `AUTO_EVICT_CONFIG=1`。P-3 执行前脚本会在清理环境后读取 `CACHE_ROOT` 所在文件系统的空间信息：

```text
total_bytes
used_bytes
avail_bytes
```

然后根据 `FILES * FILE_SIZE`、磁盘总容量比例和最小写入量计算 P-3 的实际写入文件数。基础目标写入量为：

```text
max(FILES * FILE_SIZE, total_bytes * AUTO_EVICT_WRITE_RATIO, AUTO_EVICT_MIN_WRITE_BYTES)
```

如果 `AUTO_EVICT_MAX_WRITE_BYTES > 0`，脚本会把它当成硬上限；当实际需要写入的数据量超过这个上限时，脚本会失败并提示调大上限或降低启动安全余量。默认 `AUTO_EVICT_MAX_WRITE_BYTES=0`，表示不设置绝对上限。

如果实际需要写入的数据量超过当前可用空间的 `AUTO_EVICT_MAX_AVAIL_RATIO`，脚本会失败并提示需要更多可用空间，避免为了触发 evict 写满盘。

Falcon 启动阶段还要求当前空闲比例高于 `bgFreeRatio = 1.1 - STORAGE_THRESHOLD`。因此自动模式会保证水位线至少高于当前已用比例 `AUTO_EVICT_START_MARGIN_RATIO`，默认是 12 个百分点。水位线会放在计划写入量靠后的阶段：

```text
threshold >= used_ratio + AUTO_EVICT_START_MARGIN_RATIO
threshold ~= (used_bytes + planned_write_bytes * AUTO_EVICT_TRIGGER_RATIO) / total_bytes
```

这样在不同容量、不同已用空间的 NVMe 盘上，P-3 会自动提高文件数，直到计划写入量足够跨过启动安全水位并触发 DiskCache evict。实际使用的参数会写入：

```text
$OUT_DIR/evict_config.txt
```

如果只想固定使用手动水位线，可以关闭自动模式：

```bash
AUTO_EVICT_CONFIG=0 EVICT_THRESHOLD=0.72 bash tools/run_pyfalcon_client_benchmark.sh P-3
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
$OUT_DIR/python/P-1-idle.log
$OUT_DIR/python/P-2-idle.log
$OUT_DIR/python/P-3-idle.log
$OUT_DIR/python/P-5-idle.log
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
| `benchmark_summary.md` | 类似性能测试结果文档的最终汇总表，包含 Python internal 和 fio 基准 |
| `benchmark_summary.log` | 与 `benchmark_summary.md` 内容一致，便于直接归档或 `cat` 查看 |
| `storage_info.txt` | 记录 `OUT_DIR/CACHE_ROOT/FIO_DIR/metadata workspace` 对应的挂载点、设备和文件系统类型 |
| `evict_config.txt` | 记录 P-3 自动计算出的 `threshold`、实际 `files`、磁盘总量/已用量/计划写入量 |
| `python/P-*.log` | 记录对应 Python internal case 的 stdout/stderr，case 失败时会自动打印尾部到 `run.log` |
| `python/P-*-meta.log` | 记录对应 case 启动 Falcon meta service 的输出，case 失败时会自动打印尾部到 `run.log` |

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
BENCHMARK_ROOT=/data4/hxing bash tools/run_pyfalcon_client_benchmark.sh all
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

说明 `idle_server` 没有启动成功，或 56039 被其他进程占用。检查：

```bash
ss -ltnp | grep -E ':(56039)([[:space:]]|$)' || true
cat "$OUT_DIR"/python/*-idle.log
```

如果 P-3 在 `WAIT_SEC=45` 内没有清完所有 cache，这是当前 evict 后台调度和水位推进的表现，不代表 benchmark 失败。以 JSON 的写入结果和 Falcon `DiskCache::Cleanup()` 日志共同判断。
