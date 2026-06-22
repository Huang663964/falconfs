# DiskCache Evict 行为与 Metadb 删除方案

## 1. 使用背景

本文基于当前新的使用背景：后续不再使用 OBS/object storage 作为 evict 后的数据恢复来源。也就是说，本地 SSD 上的 cache 文件被 evict 删除后，系统不会再从 OBS 把数据读回来。

在这个背景下，DiskCache 中的本地文件不再只是“可丢弃副本”，而更接近当前可读写的数据文件本身。因此如果 evict 删除了 SSD 上的数据文件，而 metadb 仍然保留该文件的 inode/path 元数据，就会产生不一致：

```text
metadb 认为文件存在
SSD 上的数据文件已经被 remove
后续 open/read 查到元数据，但读不到真实数据
```

所以，如果确定没有 OBS 或其他恢复来源，evict 后联动删除 metadb 元数据是合理需求。但这个删除不能简单地在 `DiskCache` 里按 inode 直接删一行，需要补齐 path/inode 到 metadb 删除语义之间的链路。

## 2. 当前 DiskCache Evict 做了什么

当前 evict 的目标是释放本地 SSD cache 目录占用的空间。它只操作本地文件和 DiskCache 进程内索引。

本地 cache 文件路径由 inode 生成：

```text
{falcon_cache_root}/{inode % falcon_dir_num}/{inode}-large
```

例如：

```text
/tmp/falcon_cache/17/12345-large
```

前台 evict 的核心流程：

```text
PreAllocSpace(size) 发现空间不足
  -> Evict(size)
  -> GetCurFreeRatio()
  -> CleanupForEvict(size)
  -> 从 cacheItems 链表中找 refs == 0 的文件
  -> remove("/tmp/falcon_cache/.../{inode}-large")
  -> cacheItems.erase(...)
  -> inodeToCacheIter.erase(inode)
  -> usedCap -= size
  -> freeCap += size
```

后台清理流程：

```text
CheckFreeSpace()
  -> 每 10 秒 statfs(rootDir)
  -> block 或 inode 空闲比例低于 bgFreeRatio
  -> Cleanup()
  -> 删除未被 pin 的本地 cache 文件
```

`refs > 0` 的文件会被跳过，表示该文件当前被 DiskCache 认为正在使用。

## 3. 当前 Evict 删除了什么

当前 evict 删除两类内容。

第一类是 SSD 本地文件系统里的数据文件：

```cpp
remove(fileName.c_str());
```

这里删除的是普通本地文件，例如：

```text
/tmp/falcon_cache/17/12345-large
```

这个 `remove()` 会删除本地文件系统中的目录项，并在没有打开 fd 继续引用该文件时释放本地文件系统 inode 和数据 block。

第二类是 DiskCache 的内存索引：

```cpp
it = cacheItems.erase(it);
inodeToCacheIter.erase(key);
usedCap -= size;
freeCap += size;
```

也就是删除：

```text
CacheItem
inode -> CacheItem iterator 映射
DiskCache 内部容量统计
```

## 4. 当前 Evict 没有删除什么

当前 evict 不会删除：

```text
FalconFS metadb 中的 inode 元数据
FalconFS metadb 中的 path/name/parent 关系
FalconFS 目录项元数据
FalconFS 文件 size/mode/nlink/node_id 等元数据
```

也就是说，当前实现下 evict 后，metadb 仍然认为该文件存在。

在无 OBS 背景下，这会带来：

```text
metadata hit + data miss
```

即元数据能查到，但 SSD 数据文件已经不存在。

## 5. 现有 Metadb 删除路径

当前真正删除文件元数据的入口是用户 unlink 路径。

客户端流程：

```text
FalconUnlink(path)
  -> router->GetWorkerConnByPath(path)
  -> Connection::Unlink(path)
  -> meta service UNLINK
  -> FalconUnlinkHandle()
  -> 返回 inodeId / size / nodeId
  -> InnerFalconUnlink(inodeId, nodeId, path)
  -> FalconStore::DeleteFiles(inodeId, nodeId, path)
```

metadb 内部删除逻辑在 `FalconUnlinkHandle()` 中完成。它不是直接按 inode id 删除，而是按 path 解析出：

```text
parentId
name
parentId_partId
shardId
workerId
```

然后调用：

```cpp
SearchAndUpdateInodeTableInfo(..., nlinkChangeNum = -1, ...)
```

当 `nlink + nlinkChangeNum == 0` 时，才执行：

```cpp
CatalogTupleDelete(workerInodeRel, &heapTuple->t_self);
```

因此，现有 metadb 删除语义是：

```text
按 path 定位文件
校验目标是文件
按 parentId_partId/name 定位 inode table 记录
更新 nlink
nlink 归零时删除 inode row
再返回 inodeId/nodeId 供 store 删除数据文件
```

## 6. 是否有通过 Path 直接删除 Metadb 的方式

有。现有方式就是：

```cpp
FalconUnlink(path)
```

这是目前最完整、最安全的删除入口，因为它复用了已有 unlink 语义：

```text
path 校验
目录解析
shard/worker 路由
nlink 处理
返回数据删除所需的 inodeId/nodeId/path
```

如果上层在 evict 时能拿到 FalconFS 逻辑 path，推荐不要让 `DiskCache` 自己删 metadb，而是把 evict 事件上抛到能拿到 path 的层，由上层调用：

```cpp
FalconUnlink(path)
```

或者新增一个更明确的上层接口：

```cpp
EvictAndUnlink(path)
```

该接口内部统一处理 metadb unlink、store 数据删除、失败补偿和日志审计。

## 7. 是否有通过 Inode ID 直接删除 Metadb 的方式

从当前代码看，没有现成的、完整安全的“只凭 inode id 删除文件元数据”的公开路径。

原因有两个。

第一，现有 inode 表虽然有 `st_ino` 字段，但 shard 内唯一索引是：

```text
(parentid_partid, name)
```

也就是说，现有 metadb 的主定位方式是：

```text
parentId + name
```

而不是：

```text
inode id
```

第二，删除文件不仅是删除 inode row，还涉及目录项/path 关系和 nlink 语义。只凭 inode id 删除容易绕过：

```text
path 校验
parent/name 目录关系
nlink 更新
shard/worker 路由
并发 unlink/open/read/write 语义
```

如果强行做 `DeleteMetaByInode(inode)`，至少需要新增反向索引或扫描机制：

```text
inode -> parentId_partId + name + shardId
```

然后才能复用或模拟 `FalconUnlinkHandle()` 的删除逻辑。

不建议直接在 inode table 上按 `st_ino` 扫描后 `CatalogTupleDelete()`，因为这很容易留下目录表、nlink 或并发状态不一致。

## 8. Store 是否能反向查找 Metadb 元数据

从当前代码结构看，store 主要承担数据面职责，掌握的是：

```text
inodeId
nodeId
本地 cache 文件路径
部分 RPC 请求里携带的 path
```

store 能通过 inode 算本地数据路径：

```cpp
GetFilePath(inodeId)
```

但这只是：

```text
inode -> /tmp/falcon_cache/.../{inode}-large
```

不是：

```text
inode -> FalconFS 逻辑 path
```

现有 store 侧没有看到“根据 inode 反查 metadb path”的接口。现有 client/meta 侧则是 path 驱动：

```text
path -> router -> meta worker -> inodeId/nodeId
```

也就是说，目前更像是：

```text
client/meta 知道 path 到 inode/node 的映射
store 知道 inode 到本地数据文件的映射
```

store 和 metadb 的关联主要通过 client/open/unlink 等流程传下来的 `inodeId/nodeId/path` 完成。store 自己不能可靠地只凭本地 cache 文件反查完整 metadb 元数据。

## 9. 方案 A：DiskCache 记录 Path，Evict 后上抛删除请求

### 9.1 方案目标

在 DiskCache 的 cache item 中保存 FalconFS 逻辑 path。evict 成功删除本地 SSD 文件后，把 inode/path/size 上报给上层，由上层调用现有 `FalconUnlink(path)` 或专门的 `EvictAndUnlink(path)`。

这个方案的核心思想是：

```text
DiskCache 只负责发现和删除本地文件
真正的 metadb 删除仍然走 path unlink 语义
```

### 9.2 需要改动的模块

需要改造 `CacheItem`：

```cpp
struct CacheItem {
    uint64_t inode;
    uint64_t size;
    uint64_t atime;
    uint32_t refs;
    std::string path;
};
```

需要改造 DiskCache 写入索引的接口：

```cpp
InsertAndUpdate(uint64_t key, uint64_t size, bool needPin, std::string path)
Update(uint64_t key, uint64_t size)
Add(uint64_t key, uint64_t size)
```

其中 `InsertAndUpdate()` 最适合携带 path，因为 cache item 第一次创建时通常能拿到 `openInstance->path`。

需要梳理所有创建或回填 DiskCache 条目的位置，例如：

```text
OpenFile() 创建新本地文件
DownLoadFromStorage() 下载完成后 InsertAndUpdate
WriteToFileAsync() 异步写小文件后 InsertAndUpdate
CloseTmpFiles() flush 时 InsertAndUpdate
brpc ReadSmallFilesForBrpc / DownLoadFromStorageForBrpc
测试中直接调用 InsertAndUpdate 的位置
```

### 9.3 建议的数据流

不要在 `DiskCache::CleanupForEvict()` 里直接调用 `FalconUnlink(path)`。建议引入回调或事件队列：

```cpp
struct EvictedItem {
    uint64_t inode;
    uint64_t size;
    std::string path;
};

class DiskCacheEvictListener {
public:
    virtual void OnEvicted(const EvictedItem &item) = 0;
};
```

evict 数据流：

```text
DiskCache::CleanupForEvict()
  -> remove(local cache file) 成功
  -> 删除 cacheItems / inodeToCacheIter
  -> 生成 EvictedItem(inode, path, size)
  -> 投递到上层事件队列
  -> 上层异步调用 FalconUnlink(path)
  -> metadb 删除成功后记录审计日志
```

更稳妥的顺序也可以反过来：

```text
先调用 FalconUnlink(path)
  -> metadb 删除成功
  -> DeleteFiles 删除本地文件
```

但这就不应该叫 DiskCache evict，而更像是“容量驱动的文件删除 GC”。如果业务语义是自动删除文件，建议把它从 DiskCache evict 中拆出去，做成单独的 GC 组件。

### 9.4 失败补偿

如果采用“先 remove 本地文件，再 unlink metadb”：

```text
remove 成功
metadb unlink 失败
```

会出现：

```text
数据已删除
元数据仍存在
```

这正是当前最需要避免的状态。必须有补偿机制，例如：

```text
把 EvictedItem 持久化到本地 pending-delete 日志
后台持续重试 FalconUnlink(path)
重试成功后删除 pending 记录
启动时加载 pending-delete 日志继续补偿
```

如果采用“先 unlink metadb，再 remove 本地文件”：

```text
metadb unlink 成功
remove 失败
```

会出现：

```text
元数据不存在
SSD 文件残留
```

这个状态相对可控，因为文件已不可见，但会泄漏 SSD 空间。可以通过后台 orphan cache cleaner 清理。

所以在无 OBS 背景下，更推荐：

```text
先删 metadb，再删本地文件
```

但这需要把流程从 DiskCache 内部 evict 改成上层 GC，而不是在 DiskCache 的空间不足路径里直接删 metadb。

### 9.5 风险

path 过期风险：

```text
文件 rename 后，DiskCache 中保存的 path 可能已经不是当前 path
后续用旧 path 调 FalconUnlink 可能失败，或者误删旧路径上的新对象
```

需要在 rename 时更新 DiskCache 中的 path，或者不要把 path 长期缓存在 DiskCache，而是通过 metadb 反查。

并发风险：

```text
evict 选中 refs == 0 的文件
另一个 client 同时准备 open/read
metadb unlink 和 open/read 竞态
```

`refs == 0` 只代表当前 store 进程内没有 pin，不代表全局没有 client 正在使用这个文件。

递归删除风险：

```text
evict -> FalconUnlink(path) -> InnerFalconUnlink -> DeleteFiles -> DiskCache::Delete
```

如果 evict 已经删除了本地文件，再走 DeleteFiles 可能遇到 ENOENT，需要明确这个错误在 evict unlink 场景下是否可忽略。

锁顺序风险：

```text
DiskCache mutex 内调用 metadb/client/store 逻辑
metadb unlink 回调 DeleteFiles
DeleteFiles 再进入 DiskCache
```

这可能造成死锁。因此不能在持有 DiskCache mutex 时调用外部复杂逻辑。

语义风险：

```text
空间回收变成文件删除
用户可能没有显式执行 unlink，文件却消失
```

需要产品层明确这是允许的行为，并最好有日志、指标、告警或删除策略。

## 10. 方案 B：新增 Inode -> Path 反向元数据

### 10.1 方案目标

新增 metadb 能力，让系统可以根据 inodeId 找到对应的 path 或 parent/name，然后复用 unlink 语义删除元数据。

这个方案适合：

```text
evict 时 store 只有 inodeId
不希望 DiskCache 长期保存 path
希望 rename 后以 metadb 当前状态为准
```

### 10.2 可能的数据结构

可以新增一张反向索引表：

```text
falcon_inode_reverse_table
  inode_id bigint
  parent_id_part_id bigint
  name text
  shard_id int
  node_id int
  version bigint
```

如果需要支持 hard link 或多个 path 指向同一 inode，则必须允许：

```text
inode_id -> 多条 parent/name
```

如果系统语义保证无 hard link，则可以约束 inode_id 唯一。

### 10.3 写入和维护点

create 时：

```text
创建 inode table row
写入 inode_reverse_table(inode, parentId_partId, name, shardId)
```

rename 时：

```text
更新 inode_reverse_table 中 parentId_partId/name
或者删除旧记录并插入新记录
```

unlink 时：

```text
删除 inode_reverse_table 中对应记录
nlink 归零时删除 inode table row
```

rmdir/目录递归删除时：

```text
需要批量删除子文件反向索引
```

启动或恢复时：

```text
需要能从 inode table/directory table 重建或校验反向索引
```

### 10.4 新增 API 设计

可以新增 metadb RPC：

```text
ResolvePathByInode(inodeId) -> parentId_partId/name/path/nodeId/version
UnlinkByInode(inodeId, expectedVersion)
```

更推荐提供：

```text
UnlinkByInode(inodeId)
```

但内部仍然要先解析到 parent/name，再调用现有 unlink 逻辑，而不是直接删 inode row。

建议内部流程：

```text
store 上报 inodeId
  -> metadb 根据 inodeId 查 reverse table
  -> 得到 parentId_partId/name/shardId
  -> 校验该 inode 仍匹配 parent/name
  -> 走 SearchAndUpdateInodeTableInfo(... nlinkChangeNum = -1 ...)
  -> 删除 reverse table 记录
  -> 返回 inodeId/nodeId
```

### 10.5 失败补偿

需要考虑：

```text
反向索引写入成功，inode row 写入失败
inode row 写入成功，反向索引写入失败
rename 更新 inode table 成功，reverse table 更新失败
unlink 删除 inode row 成功，reverse table 删除失败
```

这些都需要事务包裹，或者提供后台一致性校验任务：

```text
scan inode table
scan reverse table
发现孤儿 reverse record
发现缺失 reverse record
修复或告警
```

### 10.6 风险

一致性风险：

```text
新增一份 inode->path 映射后，所有 create/rename/unlink 都必须维护它
任何遗漏都会导致 evict 删除错误 path 或找不到 path
```

多路径风险：

```text
如果存在 hard link，同一个 inode 可能对应多个 name
evict 时按 inode 删除哪个 path 需要明确定义
```

并发风险：

```text
evict 根据 inode 查到 path
同时 rename 改变 path
evict 继续删除旧 path
```

需要版本号或锁来保证查到的映射仍然有效。

复杂度风险：

```text
新增 metadb 表、RPC、事务、恢复逻辑
会明显扩大改造范围
```

## 11. 方案 C：新增 DeleteMetaByInode(inodeId)

### 11.1 方案目标

在 metadb 中新增一个按 inodeId 直接删除元数据的接口。store evict 时只上报 inodeId，不需要 path。

表面流程：

```text
DiskCache evict inode
  -> store 调 DeleteMetaByInode(inode)
  -> metadb 找到 inode row
  -> 删除 inode row
```

### 11.2 为什么不推荐

当前 metadb 的删除模型不是 inode 主键驱动，而是 path/parent/name 驱动。直接按 inode 删除会绕过现有 unlink 的关键语义。

主要问题：

```text
inode table 删除了，但 directory table 或 dir path cache 仍可能保留目录关系
如果存在多个 name 指向同一 inode，直接删除 inode 会破坏其他 path
nlink 没有按语义递减
rename 并发时可能删错对象
open/read 并发时语义不清晰
shard 路由需要先知道 inode 属于哪个 shard
```

### 11.3 如果必须实现，需要补齐什么

即使要实现，也不应直接 `CatalogTupleDelete()`。至少需要：

```text
按 inodeId 建索引或全局映射，定位 shard
根据 inodeId 找到 parentId_partId/name
校验 nlink 和 mode
删除或更新目录关系
处理 hard link
处理 rename/open/unlink 并发
提供删除事务或补偿日志
提供审计和告警
```

最终它会逐渐演化成方案 B 的反向索引 + unlink 语义。因此不建议从“直接按 inode 删 row”开始。

### 11.4 风险

元数据残留风险：

```text
目录项仍然存在，但 inode row 已经没了
readdir 能看到文件，open/stat 失败
```

误删风险：

```text
inode 复用或映射过期时，可能删除错误文件的元数据
```

数据泄漏风险：

```text
metadb 删除了，但 SSD 文件或远端副本没有删除
```

不可恢复风险：

```text
没有 OBS 背景下，一旦元数据和数据删除顺序不当，可能无法自动恢复
```

## 12. 方案 D：独立容量 GC，不复用 DiskCache Evict

### 12.1 方案目标

如果业务语义明确允许“SSD 空间不足时自动删除文件”，建议不要把这个行为塞进 `DiskCache::Evict()`，而是设计一个独立 GC：

```text
CapacityGC / FileEvictionGC
```

它负责从系统层面选择要删除的文件，并按完整删除流程执行。

### 12.2 推荐流程

```text
GC 监控 SSD 空间
  -> 根据策略选择候选文件
  -> 确认文件未打开/未 pin/未被锁定
  -> 调 FalconUnlink(path) 删除 metadb
  -> FalconUnlink 内部调用 DeleteFiles 删除 SSD 数据
  -> 记录审计日志
```

候选策略可以是：

```text
LRU
TTL
低优先级目录
超过容量水位后的批量删除
用户配置的可回收 namespace
```

### 12.3 相比改 DiskCache 的优点

```text
删除语义清晰：这是文件 GC，不是 cache evict
可以先删 metadb，再删数据，避免 metadata hit + data miss
可以做用户可见的策略、日志、指标和告警
可以统一处理 path、rename、并发和补偿
```

### 12.4 风险

```text
需要新增 GC 模块和调度逻辑
需要定义用户文件被自动删除的产品语义
需要维护候选文件列表或从 metadb 扫描
需要防止 GC 与用户 unlink/rename/open 并发冲突
```

## 13. 关键风险清单

### 13.1 语义风险

无 OBS 背景下，evict 联动 metadb 删除意味着：

```text
磁盘空间回收 == 文件删除
```

这和传统 cache eviction 语义不同。必须明确：

```text
SSD 空间不足时，系统是否允许自动删除用户文件
删除哪些文件
是否需要用户可感知
是否需要白名单/黑名单/目录级策略
```

### 13.2 一致性风险

最危险的不一致是：

```text
SSD 数据文件已删
metadb 元数据仍存在
```

这会导致后续访问出现 metadata hit + data miss。

另一个不一致是：

```text
metadb 元数据已删
SSD 文件仍存在
```

这个会造成空间泄漏，但相对可通过后台清理修复。

因此推荐删除顺序更偏向：

```text
先 metadb unlink
再删除 SSD 文件
```

而不是：

```text
先 remove SSD 文件
再尝试删 metadb
```

### 13.3 Path 过期风险

如果 DiskCache 记录 path：

```text
create /a
DiskCache 记录 /a
rename /a -> /b
DiskCache 仍记录 /a
evict 时调用 FalconUnlink(/a)
```

可能失败，也可能在极端情况下误删后来新建的 `/a`。需要：

```text
rename 时更新 DiskCache path
或者 metadb 删除时携带 inode/version 校验
或者不长期缓存 path，改用 inode->path 反查
```

### 13.4 并发风险

`refs == 0` 只说明当前 DiskCache 认为没人 pin 该本地文件，不等价于全局没人使用。

需要考虑：

```text
client 正在 open
client 正在 read/write
另一个线程正在 rename
用户正在 unlink
远端 node 正在访问
```

如果没有统一锁或版本检查，evict 删除 metadb 可能和这些操作冲突。

### 13.5 锁与递归风险

不能在持有 DiskCache mutex 时调用：

```text
FalconUnlink
Connection::Unlink
FalconStore::DeleteFiles
DiskCache::Delete
```

因为这些路径可能重新进入 DiskCache 或等待其他锁，造成死锁。

### 13.6 失败补偿风险

必须设计 pending 状态：

```text
PENDING_META_DELETE
PENDING_DATA_DELETE
DELETE_DONE
DELETE_FAILED
```

否则进程崩溃或网络失败时容易留下不可恢复的不一致。

### 13.7 启动恢复风险

如果 evict 删除 metadb 是异步的，启动时需要恢复未完成任务：

```text
加载 pending-delete 日志
检查 SSD 文件是否还存在
检查 metadb 是否还存在
继续删除或修复
```

否则重启会丢失补偿任务。

## 14. 推荐结论

在无 OBS 背景下，不能继续把 DiskCache evict 简单理解为“删除可恢复副本”。如果 evict 删除的是唯一数据文件，那么必须解决 metadb 一致性。

推荐优先级：

```text
首选：
  设计独立 CapacityGC。
  GC 在拥有 path 和策略判断的层面调用 FalconUnlink(path)。
  先删除 metadb，再删除 SSD 数据文件。

次选：
  DiskCache 保存 path，evict 成功后上抛事件。
  上层异步调用 FalconUnlink(path)。
  必须有 pending-delete 日志和重试补偿。

中长期：
  新增 inode->path 反向元数据。
  支持 store 根据 inode 请求 metadb 解析 path 后走 unlink 语义。

不推荐：
  在 DiskCache 内直接按 inode 删除 metadb row。
```

当前代码中最可靠的 metadb 删除方式仍然是：

```cpp
FalconUnlink(path)
```

而不是：

```cpp
DeleteMetaByInode(inodeId)
```

原因是当前 metadb 的删除模型是 path/parent/name 驱动的，store 侧也没有现成的 inode 反查 path 能力。
