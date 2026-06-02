# DiskCache Evict / Unlink 性能测试步骤

本文只记录测试步骤、执行口径和场景编号。测试结果见 [diskcache_evict_perf_test_zh.md](diskcache_evict_perf_test_zh.md)。两个文档使用相同场景编号，便于一一对应。

## 1. 场景编号

| 编号 | 类型 | 场景 | 入口 | 说明 |
| --- | --- | --- | --- | --- |
| I-1 | 内部接口 | 纯写 | `FalconCreate/FalconWrite/FalconClose` | 不触发 evict |
| I-2 | 内部接口 | 纯删除 | `FalconUnlink` | 先创建文件，再集中删除 |
| I-3 | 内部接口 | create+evict | 写入 + DiskCache 后台 cleanup | 前台单线程写入，后台 evict 线程异步执行 |
| I-4 | 内部接口 | create+unlink | 写入 + `FalconUnlink` | 保留 500 文件窗口，边写边删 |
| F-1 | FUSE | 纯写 | `/tmp/falcon_mnt` + `open/write/fsync/close` | 真实挂载路径 |
| F-2 | FUSE | 纯删除 | `/tmp/falcon_mnt` + `os.unlink` | 先创建文件，再集中删除 |
| F-3 | FUSE | create+evict | FUSE 写入 + DiskCache 后台 cleanup | 前台单线程 FUSE 写入，后台 evict 线程异步执行 |
| F-4 | FUSE | create+unlink | FUSE 写入 + `os.unlink` | 保留 500 文件窗口，边写边删 |
| B-1 | fio | 单文件顺序写 | `/tmp` direct I/O | 不经过 Falcon/FUSE |
| B-2 | fio | 单文件顺序读 | `/tmp` direct I/O | 不经过 Falcon/FUSE |
| B-3 | fio | 多文件顺序写 | `/tmp` direct I/O | 6000 个 2MiB 文件，不经过 Falcon/FUSE |
| B-4 | fio | 多文件顺序读 | `/tmp` direct I/O | 6000 个 2MiB 文件，不经过 Falcon/FUSE |
| B-5 | fio + 本地 unlink | 多文件删除 | `/tmp` 本地文件系统 | fio 创建 6000 个 2MiB 文件后，单进程顺序 unlink，不经过 Falcon/FUSE |

## 2. 公共环境

- 仓库路径：`/home/hx/code/falconfs`
- 测试日期：`2026-06-01`、`2026-06-02`
- 测试提交：`333969a`
- Falcon 默认端口：`55500/55510/55520/55530`
- client/store brpc 端口：`56039`
- 单文件大小：`2MiB`
- 纯写 / create+evict / create+unlink 文件数：`6000`
- 纯删除文件数：`2000`
- create+unlink 保留窗口：`500`
- 执行模型：测试脚本/驱动为单进程单线程顺序执行；create+evict 场景额外包含 DiskCache 后台 cleanup 线程。

## 3. 内部接口测试步骤

对应结果：`I-1` 到 `I-4`。

### 3.1 内部接口环境

内部接口测试不经过 FUSE，不使用系统 `open/write/unlink`。测试驱动直接调用：

```text
FalconMkdir
FalconCreate
FalconWrite
FalconClose(path, fd, true, -1)
FalconClose(path, fd, false, -1)
FalconUnlink
```

测试驱动：`tools/falcon_internal_perf.cpp`
测试二进制：`build/internal_perf/falcon_internal_perf`
结果目录：`/tmp/falcon_internal_perf_results_internal_final`

每个 case 使用独立 cache root，并预创建 DiskCache 分片目录：

```bash
mkdir -p /tmp/falcon_internal_perf_cache_<case>
for i in $(seq 0 100); do
  mkdir -p /tmp/falcon_internal_perf_cache_<case>/$i
done
```

临时 config 调整：

```text
falcon_cluster_view = ["127.0.0.1:56039"]
falcon_cache_root   = /tmp/falcon_internal_perf_cache_<case>
```

### 3.2 内部接口清理和启动

每个 case 前清理默认端口和残留 pid/socket，不更换端口：

```bash
sudo ./deploy/falcon_stop.sh || true
# 如 stop 后仍有默认端口残留，通过 ss 找到 55500/55510/55520/55530/56039 对应 PID 后 kill。
sudo rm -f /tmp/.s.PGSQL.55500* /tmp/.s.PGSQL.55520* \
           /tmp/.s.PGSQL.55510* /tmp/.s.PGSQL.55530*
sudo rm -f /home/hx/metadata/coordinator0/postmaster.pid \
           /home/hx/metadata/worker0/postmaster.pid
source ./deploy/falcon_env.sh
./deploy/meta/falcon_meta_start.sh
# 等待 127.0.0.1:55510 和 127.0.0.1:55530 LISTEN。
```

### 3.3 内部接口创建/写入流程

每个文件都走 `CreateWriteClose`：

```text
1. 生成内部路径，例如 /internal_pure_write/f_00000000。
2. FalconCreate(path, fd, O_CREAT | O_RDWR, &st)。
3. FalconWrite(fd, path, payload.data(), payload.size(), 0)。
4. FalconClose(path, fd, true, -1)，执行 sync/flush。
5. FalconClose(path, fd, false, -1)，释放打开实例。
```

### 3.4 内部接口命令模板

```bash
STORAGE_THRESHOLD=<threshold> \
CONFIG_FILE=<case_config.json> \
build/internal_perf/falcon_internal_perf \
  --mode <write_only|unlink_only|create_evict|create_unlink> \
  --dir /internal_<case> \
  --output <result.json> \
  --files <count> \
  --file-size 2097152 \
  --window 500 \
  --wait-sec <seconds>
```

内部接口场景参数：

| 编号 | mode | threshold | files | wait-sec | 说明 |
| --- | --- | ---: | ---: | ---: | --- |
| I-1 | `write_only` | `1` | `6000` | `0` | 纯写 |
| I-2 | `unlink_only` | `1` | `2000` | `0` | 先创建后集中 `FalconUnlink` |
| I-3 | `create_evict` | `0.72` | `6000` | `45` | 写入触发 DiskCache 后台 evict |
| I-4 | `create_unlink` | `1` | `6000` | `0` | 保留 500 文件窗口，边写边删 |

## 4. FUSE 端到端测试步骤

对应结果：`F-1` 到 `F-4`。

### 4.1 FUSE 环境

FUSE 测试通过真实挂载点 `/tmp/falcon_mnt`：

```text
os.open / os.write / os.fsync / os.close / os.unlink
-> FUSE
-> falcon_client DoCreate/DoWrite/DoFlush/DoRelease/DoUnlink
-> FalconCreate/FalconWrite/FalconClose/FalconUnlink
```

FUSE 测试脚本：`/tmp/ffuse_perf.py`
结果目录：`/tmp/ffuse_perf_results_current_20260601_153026`

测试前确认安装产物为当前分支：

```bash
sudo ./deploy/falcon_stop.sh
./build.sh build falcon
sudo ./build.sh install falcon
```

### 4.2 FUSE 清理和启动

每个 case 前清理环境并重新拉起默认端口服务：

```bash
sudo ./deploy/falcon_stop.sh || true
sudo umount -l /tmp/falcon_mnt 2>/dev/null || true
sudo rm -rf /tmp/falcon_cache /tmp/falcon_mnt /home/hx/metadata
sudo rm -f /tmp/.s.PGSQL.55500* /tmp/.s.PGSQL.55520* \
           /tmp/.s.PGSQL.55510* /tmp/.s.PGSQL.55530*
mkdir -p /tmp/falcon_mnt
STORAGE_THRESHOLD=<threshold> ./deploy/falcon_start.sh
# 等待 /tmp/falcon_mnt 成为 mountpoint，且 55510/55530/56039 ready。
```

### 4.3 FUSE 创建/写入流程

单个文件流程：

```text
1. 路径示例：/tmp/falcon_mnt/fuse_pure_write/f_00000000。
2. os.open(path, O_CREAT | O_RDWR | O_TRUNC, 0644)。
3. os.write(fd, payload)，payload 为 2MiB。
4. os.fsync(fd)。
5. os.close(fd)。
```

主动删除流程：

```text
os.unlink(path)
```

### 4.4 FUSE 命令模板

```bash
python3 /tmp/ffuse_perf.py \
  --mode <write_only|unlink_only|create_evict|create_unlink> \
  --dir /tmp/falcon_mnt/fuse_<case> \
  --output <result.json> \
  --files <count> \
  --file-size 2097152 \
  --window 500 \
  --wait-sec <seconds>
```

FUSE 场景参数：

| 编号 | mode | threshold | files | wait-sec | 说明 |
| --- | --- | ---: | ---: | ---: | --- |
| F-1 | `write_only` | `1` | `6000` | `0` | 纯写 |
| F-2 | `unlink_only` | `1` | `2000` | `0` | 先创建后集中 `os.unlink` |
| F-3 | `create_evict` | `0.53` | `6000` | `45` | 写入触发 DiskCache 后台 evict |
| F-4 | `create_unlink` | `1` | `6000` | `0` | 保留 500 文件窗口，边写边删 |

## 5. fio 基线测试步骤

对应结果：`B-1` 到 `B-5`。fio 不经过 Falcon，也不经过 FUSE，只用于说明当前 `/tmp` 所在文件系统的本地基线。

### 5.1 单文件 direct 顺序读写

```bash
fio --output=/tmp/ffio_seq_write.json --output-format=json \
  --name=fio_seq_write \
  --filename=/tmp/ffio_baseline.dat \
  --rw=write --size=8G --bs=2M \
  --iodepth=1 --numjobs=1 --direct=1 --group_reporting=1

fio --output=/tmp/ffio_seq_read.json --output-format=json \
  --name=fio_seq_read \
  --filename=/tmp/ffio_baseline.dat \
  --rw=read --size=8G --bs=2M \
  --iodepth=1 --numjobs=1 --direct=1 --group_reporting=1

rm -f /tmp/ffio_baseline.dat
```

### 5.2 多文件 direct 顺序读写

```bash
rm -rf /tmp/ffio_multi
mkdir -p /tmp/ffio_multi

fio --output=/tmp/ffio_multi_write.json --output-format=json \
  --name=ffio_multi \
  --directory=/tmp/ffio_multi \
  --nrfiles=6000 --filesize=2M \
  --rw=write --bs=2M \
  --iodepth=1 --numjobs=1 --direct=1 \
  --openfiles=1 --file_service_type=sequential \
  --group_reporting=1 --unlink=0

fio --output=/tmp/ffio_multi_read.json --output-format=json \
  --name=ffio_multi \
  --directory=/tmp/ffio_multi \
  --nrfiles=6000 --filesize=2M \
  --rw=read --bs=2M \
  --iodepth=1 --numjobs=1 --direct=1 \
  --openfiles=1 --file_service_type=sequential \
  --group_reporting=1 --unlink=0

rm -rf /tmp/ffio_multi
```

### 5.3 多文件本地删除

说明：fio 本身用于创建 6000 个 2MiB 文件；删除耗时用本地文件系统上的单进程顺序 unlink 统计，不经过 Falcon/FUSE。

```bash
rm -rf /tmp/ffio_delete
mkdir -p /tmp/ffio_delete

fio --output=/tmp/ffio_delete_create.json --output-format=json \
  --name=ffio_delete \
  --directory=/tmp/ffio_delete \
  --nrfiles=6000 --filesize=2M \
  --rw=write --bs=2M \
  --iodepth=1 --numjobs=1 --direct=1 \
  --openfiles=1 --file_service_type=sequential \
  --group_reporting=1 --unlink=0

count_before=$(find /tmp/ffio_delete -type f | wc -l)
start_ns=$(date +%s%N)
find /tmp/ffio_delete -type f -delete
end_ns=$(date +%s%N)
count_after=$(find /tmp/ffio_delete -type f | wc -l)
rm -rf /tmp/ffio_delete
```

## 6. 耗时口径

### 6.1 内部接口

```text
write_latency_*:
  单文件 FalconCreate -> FalconWrite -> FalconClose(sync) -> FalconClose(release)。

elapsed_sec (write_only/create_evict):
  第一个文件 CreateWriteClose 开始，到最后一个文件 CreateWriteClose 结束。
  create_evict 的 wait-sec 不计入 elapsed_sec。

unlink_latency_*:
  单文件 FalconUnlink(path) 调用耗时。

unlink_elapsed_sec:
  连续 FalconUnlink loop 总耗时。

mixed_elapsed_sec:
  写入和 FalconUnlink 混合执行的总窗口。

cleanup_elapsed_us_total:
  DiskCache::Cleanup 每轮 cleanup elapsed us 累加。

remove_elapsed_us_total:
  DiskCache::Cleanup 内部 remove(fileName.c_str()) 调用耗时累加。

notify_elapsed_us_total:
  DiskCache::NotifyEvicted(evictedItems) 耗时累加。

cleanup_log_window_sec:
  第一条 Evicted 日志到最后一条 Evicted 日志的时间窗口，包含后台周期等待。
```

### 6.2 FUSE

```text
write_latency_*:
  单文件 os.open -> os.write 2MiB -> os.fsync -> os.close。
  包含 FUSE 调度和 falcon_client 回调进入 Falcon 内部接口的耗时。

elapsed_sec (write_only/create_evict):
  第一个文件 open 前，到最后一个文件 close 后。
  create_evict 的 wait-sec 不计入 elapsed_sec。

unlink_latency_*:
  单文件 os.unlink(path) 调用耗时，包含 VFS/FUSE 和 Falcon 内部删除路径。

unlink_elapsed_sec:
  连续 os.unlink loop 总耗时。

mixed_elapsed_sec:
  写入和 os.unlink 混合执行的总窗口。

cleanup/remove/notify/log_window:
  从 falcon_client 的 DiskCache::Cleanup 日志汇总，口径与内部接口 evict 相同。
```
