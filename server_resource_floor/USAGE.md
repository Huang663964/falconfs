# server-resource-floor.sh 使用说明

这个脚本用于启动和停止服务器资源占用。默认目标是 `25%`，并会在目标附近轻微起伏，默认波动范围是 `±5%`，也就是大致在 `20-30%`，CPU 和内存会持续波动。

依赖：`bash`、`awk`、`python3`、`lscpu`（用于按 NUMA node 发现 CPU，找不到时自动回退到所有在线 CPU）、`taskset`（用于绑定 CPU，未安装时仅记录负载不做绑定）。

## 启动

```bash
bash server-resource-floor.sh start
```

指定目标和波动幅度：

```bash
bash server-resource-floor.sh start --target 25 --swing 6
```

绑定 CPU 到 node1（默认）：

```bash
bash server-resource-floor.sh start --node 1
```

调整波动周期，单位是秒：

```bash
bash server-resource-floor.sh start --target 25 --swing 5 --step-sec 15
```

## 停止

```bash
bash server-resource-floor.sh stop
```

## 查看状态

```bash
bash server-resource-floor.sh status
```

## 只查看当前使用率

```bash
bash server-resource-floor.sh once
```

脚本会在当前目录生成两个运行文件：

```text
server-resource-floor.pid
server-resource-floor.log
```

停止后 `pid` 文件会自动删除，日志文件会保留。
