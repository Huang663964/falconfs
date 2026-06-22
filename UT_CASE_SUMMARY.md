# FalconFS UT 用例说明

## 1. 文档目的

本文档用于说明当前 FalconFS 仓库内已有 UT 以及 PR #111 中新增/补充的 UT 分别覆盖了哪些功能、流程和异常分支。文档重点说明“这些用例在测试什么”，便于评审时判断新增覆盖是否合理、是否依赖外部服务、是否会影响正常构建。

当前 UT 覆盖范围主要包括：

- `common`：配置加载、初始化、日志、统计、buffer、ZooKeeper facade、线程池等基础能力。
- `falcon`：PostgreSQL extension 侧的 connection pool、metadb、transaction、control、remote comm、distributed backend、plugin、perf counter、dir path hash 等模块。
- `falcon_client`：客户端 metadata API、router、BRPC connection、FUSE wrapper、服务模式下 metadata/io wrapper。
- `falcon_store`：本地/远端文件 open/read/write/flush/truncate/delete/statfs、disk cache、read/write stream、node、file lock、storage-backed 分支。
- `falcon_plugin`：inline/background 插件加载、插件执行、插件失败路径、background worker 退出路径。
- `private-directory-test`：本地 metadata、KV、slice、POSIX-like workload 流程。
- `python_interface`：Python C extension 参数校验、同步/异步 API、coverage-only async internals、throw hook。

## 2. PR #111 本轮新增和加强内容

PR #111 的目标是补充低风险覆盖率用例，尽量不改业务逻辑。新增内容主要分为以下几类。

### 2.1 FalconStore storage-backed 分支

本轮为 `FalconStore` 增加了 coverage-only 测试 hook：

- 通过 `FALCON_STORE_COVERAGE_TEST` 编译宏保护。
- 只在 coverage build 下暴露，不影响正常 build。
- 允许测试注入 mock storage，并访问原本难以从 public API 稳定触达的私有 storage helper。

新增 `MockStorage`，用于稳定覆盖真实 OBS/storage 相关分支：

- `Success`：模拟 storage 正常读写、statfs、copy、delete。
- `Error`：模拟 storage 返回 `-EIO` 等错误。
- `SlowSuccess`：模拟慢速 storage，用于制造 async duplicate-download lock contention。

这些用例避免依赖真实 OBS 服务，也避免通过不稳定的外部状态触发分支。

### 2.2 Python interface coverage-only internals

本轮为 Python C extension 增加了 coverage-only helper：

- 通过 `FALCON_PYTHON_COVERAGE_TEST` 编译宏保护。
- `CoverageExerciseInternals` 用于覆盖 Python 层难以稳定触发的 async worker 和 exception-result conversion。
- Python 测试中使用 `hasattr` 判断该 helper 是否存在；普通非 coverage build 中不存在时会显式 skip。

另外，`throw_hook.cpp` 增加 `coverage_print_stacktrace`，测试可以直接调用 stacktrace helper，而不依赖真实 C++ throw interpose。

### 2.3 falcon/falcon_client/common 等低风险分支

PR #111 还补充了多处边界覆盖：

- `falcon_client`：router、BRPC connection、metadata error、FUSE wrapper、服务可用时 metadata/io wrapper。
- `falcon`：connection pool、transaction cleanup、remote comm、distributed backend、control hook/flag、dir path hash、perf counter、plugin loader。
- `common`：配置错误、初始化边界、ZooKeeper facade failure、统计输出、buffer 生命周期。
- `falcon_store`：DiskCache、FileLock、Node、ReadStream、WriteStream 的失败和边界分支。

## 3. common 模块 UT

### 3.1 `tests/common/test_common_coverage.cpp`

#### Base64

`CommonBase64UT.EncodeDecodePaddingAndBinaryData`

- 测试普通字符串的 base64 编码和解码。
- 测试需要 padding 的输入长度，例如长度不能被 3 整除的情况。
- 测试包含 `\0` 或非文本字节的二进制数据。
- 验证 encode 后再 decode 能恢复原始数据。

`CommonBase64UT.DecodeRejectsMalformedInput`

- 测试非法 base64 字符。
- 测试 padding 位置不合法。
- 测试长度或格式不完整。
- 验证 malformed input 不会被错误解析为正常数据。

#### FalconConfig

`CommonFalconConfigUT.LoadsTypedPropertiesAndArrays`

- 加载测试配置文件。
- 验证 string、array、uint32、uint64、double、bool 等类型读取。
- 覆盖配置文件中数组字段和普通字段的读取路径。

`CommonFalconConfigUT.ReportsInvalidFilesAndTypes`

- 测试配置文件不存在。
- 测试配置内容格式错误。
- 测试 key 存在但类型不符合 getter 预期。
- 验证失败时返回错误或默认行为，不造成崩溃。

`CommonFalconConfigUT.FormatUtilConvertsSupportedTypes`

- 测试内部格式化工具对支持类型的转换。
- 覆盖 JSON/value 到 string 的多个分支。

`CommonFalconConfigUT.InvalidGettersAndFormatUtilVariants`

- 使用不存在的 key 触发错误路径。
- 用错误 getter 读取已有 key，例如用 array getter 读数字、用 bool getter 读字符串。
- 验证错误日志和返回值。

`CommonFalconConfigUT.PropertyKeyAccessorsAndUpdater`

- 测试 property key 的名称、默认值、类型访问。
- 覆盖 property key 更新和查询路径。

#### FalconInit

`CommonFalconInitUT.DirectModuleCoversInitBoundaries`

- 测试初始化模块直接调用。
- 覆盖 `CONFIG_FILE` 存在和缺失路径。
- 覆盖重复初始化、singleton 初始化边界。

#### FalconCM / ZooKeeper facade

`CommonFalconCMUT.FetchesClusterMetadataThroughZooKeeperFacade`

- 通过 ZooKeeper facade 拉取集群元数据。
- 覆盖 CN、worker、DN 信息解析。
- 验证节点路径和返回信息能转换为 FalconCM 需要的数据结构。

`CommonFalconCMUT.UploadsReuploadsAndUpdatesStatus`

- 测试节点信息上传。
- 测试重复上传时覆盖或更新已有节点。
- 测试节点状态更新。

`CommonFalconCMUT.HandlesFailureAndControlFileBranches`

- 覆盖 ZooKeeper 操作失败。
- 覆盖控制文件相关分支。
- 验证异常路径不会影响后续状态。

`CommonFalconCMUT.CoversAdditionalZooKeeperFailureBranches`

- 覆盖连接状态异常。
- 覆盖节点不存在、读取失败、状态不匹配等边界。

### 3.2 `tests/common/test_falcon_cm.cpp`

这批是已有 FalconCM 集成类测试，依赖 `zk_endpoint`。没有配置 ZooKeeper endpoint 时会跳过。

- `Start`：测试 FalconCM 启动流程。
- `ShouldHandleConnectingState`：测试连接中状态处理。
- `ExpireBeforeUpLoad`：节点上传前发生过期事件。
- `ExpireAfterUpLoad`：节点上传后发生过期事件。
- `NotConnectedState`：未连接 ZooKeeper 时的状态处理。
- `WrongEventType`：错误事件类型。
- `ShouldHandleUnknownState`：未知连接状态。
- `RetryWithNumAndInterval`：重试次数和重试间隔。

## 4. falcon_store 模块 UT

### 4.1 `tests/falcon_store/test_falcon_store.cpp`

#### 本地 open/create 流程

这些用例验证本地节点上的 open/create 行为：

- `CreateLocalWRonly`
- `OpenLocalWRonlyExist`
- `OpenLocalWRonlyNoneExist`
- `OpenLocalRDonlyExist`
- `OpenLocalRDonlyNoneExist`
- `OpenLocalRDWRExist`
- `OpenLocalRDWRNoneExist`

主要覆盖：

- `O_WRONLY`、`O_RDONLY`、`O_RDWR`。
- 文件存在和不存在两类场景。
- open instance 创建。
- 本地 physical fd 打开。
- 不同 flag 下的成功和失败返回。

#### 远端 open/create 流程

这些用例验证远端 node 的 open/create 行为：

- `CreateRemoteWRonly`
- `OpenRemoteWRonlyExist`
- `OpenRemoteWRonlyNoneExist`
- `OpenRemoteRDonlyExist`
- `OpenRemoteRDonlyNoneExist`
- `OpenRemoteRDWRExist`
- `OpenRemoteRDWRNoneExist`

主要覆盖：

- 通过远端 RPC/BRPC client 创建或打开文件。
- 远端返回 fd 后 open instance 的状态维护。
- 文件不存在时 create/open 的差异。
- 远端错误码转换。

#### Public helper 和边界分支

`OpenStats`

- 验证 open 操作对统计计数的影响。
- 覆盖统计输出相关路径。

`ParentPathHelper`

- 验证 parent path 解析。
- 覆盖根目录、多级目录、文件路径等边界。

`PublicHelpersDoNotRequirePrivateAccess`

- 验证公开 helper 可以稳定触发基础逻辑。
- 避免测试强依赖私有成员。

`BrpcWritePublicBranches`

- 覆盖 BRPC 写本地文件路径。
- 覆盖 direct buffer、普通 buffer、offset 写入、fd 状态等分支。

`CloseTmpFilesPublicBranches`

- 覆盖 close 临时文件时是否 flush、是否释放资源。
- 覆盖已经关闭、未打开、flush 失败等路径。

`SmallFilePublicMissBranches`

- 覆盖小文件 cache miss。
- 覆盖从远端或 storage 读取小文件后填充 read buffer。
- 覆盖 async cache write 调度。

`OpenStatAndTruncatePublicBranches`

- 覆盖 open/stat/truncate 公开入口。
- 覆盖路径不存在、fd 异常、truncate 失败等错误分支。

`ReadPublicBranches`

- 覆盖 `ReadFile`、`ReadFileLR` 等读取路径。
- 覆盖小文件、大文件、超过文件大小、hole 读取。

`PublicBoundaryBranches`

- 覆盖非法参数。
- 覆盖无效 inode、无效 node、无效 offset/size。

`DeleteAndStatPublicBranches`

- 覆盖删除本地/远端数据。
- 覆盖 statfs 成功和失败。

`AsyncWriteRemoteSmallAndSyncFlushPublicBranches`

- 覆盖远端小文件异步写。
- 覆盖同步 flush。

`FalconIOClientNetworkFailureBranches`

- 覆盖 endpoint 不可达。
- 覆盖 channel 初始化失败。
- 覆盖远端请求失败。

`RemoteIOServiceImplPublicErrorBranches`

- 覆盖服务端 RemoteIOServiceImpl 返回错误。
- 验证 response error_code 处理。

`FalconIOClientResponseBranches`

- 覆盖 client 对不同 response 的处理。
- 包括成功、失败、返回长度异常。

`RemoteStorePublicOpenAndSmallReadBranches`

- 覆盖远端 open 后小文件读取。
- 覆盖远端读取失败后 fallback。

#### PR #111 新增 storage-backed coverage-only 用例

`CoverageStorageSuccessBranches`

该用例注入 `MockStorage::Success`，覆盖真实环境中依赖 OBS/storage 的成功路径：

- `StatFS` 通过 storage 返回容量信息。
- `CopyData` 和 `DeleteDataAfterRename` 调用 storage copy/delete。
- `DownLoadFromStorage` 同步下载，并写入 disk cache 或 read buffer。
- `ReadSmallFiles` 在 cache miss 后从 storage 读取小文件。
- `ReadSmallFilesForBrpc` 在 BRPC 场景下从 storage 读取。
- `WriteToFileAsync` 异步写 cache 文件。
- `FlushToStorage` 将本地文件 flush 到 storage。
- `DownLoadFromStorageForBrpc` 覆盖 BRPC 私有 storage helper。
- BRPC local write 覆盖 direct 和普通写路径。
- cache-hit 场景覆盖 early return。
- remote fallback 场景覆盖远端失败后从 storage 读取。
- read-after-write 场景验证读之前会先 flush buffered write。
- `SlowSuccess` 模拟慢 storage，覆盖 async duplicate-download lock contention。
- storage disabled 时覆盖本地 stat/read 失败路径。
- node allocation policy 覆盖本地分配、inference path hash 分配。

`CoverageStorageErrorBranches`

该用例注入 `MockStorage::Error`，覆盖 storage 失败路径：

- `StatFS` 返回 `-EIO`。
- `CopyData`、`DeleteDataAfterRename` 返回错误。
- `DeleteFiles` 触发 storage delete error。
- `FlushToStorage` 失败。
- `DownLoadFromStorage` 失败。
- `ReadSmallFiles` storage read 失败。
- `ReadSmallFilesForBrpc` storage read 失败。
- `DownLoadFromStorageForBrpc` 同步失败和异步失败路径。
- `ReadFileLR` storage fallback 失败。
- 远端 open/read 失败后 fallback 到 storage，但 storage 继续失败。
- 远端 truncate 返回错误。

#### 原有读写矩阵

本地写入：

- `WriteLocalLarge`：本地大文件写入。
- `WriteLocalZero`：写入 0 字节。
- `WriteLocalSeq`：顺序写。
- `WriteLocalRandom`：随机写。
- `WriteLocalSeqToRandom`：先顺序后随机写。
- `WriteLocalStats`：写统计。

远端写入：

- `WriteRemoteLarge`
- `WriteRemoteZero`
- `WriteRemoteSeq`
- `WriteRemoteRandom`
- `WriteRemoteSeqToRandom`
- `WriteRemoteStats`

本地读取：

- `ReadLocalSeqSmall`：本地小文件顺序读。
- `ReadLocalRandomSmall`：本地小文件随机读。
- `ReadLocalSeqLarge`：本地大文件顺序读。
- `ReadLocalRandomLarge`：本地大文件随机读。
- `ReadLocalSeqToRandomLarge`：先顺序后随机读。
- `ReadLocalExceed`：越界读。
- `ReadLocalHole`：hole/sparse 读。
- `ReadLocalStats`：读统计。

远端读取：

- `ReadRemoteSeqSmall`
- `ReadRemoteRandomSmall`
- `ReadRemoteStats`
- `ReadRemoteSeqLarge`
- `ReadRemoteRandomLarge`
- `ReadRemoteSeqToRandomLarge`
- `ReadRemoteExceed`
- `ReadRemoteHole`

#### 读写交错流程

本地交错：

- `PrereadWriteLocal`
- `ReadWriteLocal`
- `PrereadWriteReadLocal`
- `ReadWriteReadLocal`
- `WritePreReadWriteLocal`
- `WriteReadWriteLocal`

远端交错：

- `PrereadWriteRemote`
- `ReadWriteRemote`
- `PrereadWriteReadRemote`
- `ReadWriteReadRemote`
- `WritePreReadWriteRemote`
- `WriteReadWriteRemote`

这些用例验证：

- pre-read 后再写。
- 写后读一致性。
- buffered write flush 时机。
- local/remote 分支行为一致。

#### flush/release/delete/truncate/statfs

- `FlushLocal`、`FlushRemote`：flush 本地/远端文件。
- `ReleaseLocal`、`ReleaseRemote`：release 已打开文件。
- `ReleaseWithoutFlushLocal`、`ReleaseWithoutFlushRemote`：不 flush 直接 release。
- `FlushTwiceLocal`、`FlushTwiceRemote`：重复 flush。
- `DeleteLocal`、`DeleteRemote`：删除存在文件。
- `DeleteLocalNoneExist`、`DeleteRemoteNoneExist`：删除不存在文件。
- `StatFs`：文件系统容量信息。
- `TruncateFileLocal`、`TruncateFileRemote`：truncate 已存在文件。
- `TruncateFileLocalNoneExist`、`TruncateFileRemoteNoneExist`：truncate 不存在文件。
- `TruncateOpenInstanceLocal`、`TruncateOpenInstanceRemote`：truncate open instance。
- `WriteTruncateOpenInstanceLocal`、`WriteTruncateOpenInstanceRemote`：写入后 truncate。

### 4.2 `tests/falcon_store/test_falcon_store_threaded.cpp`

这些用例覆盖 threaded 模式下 write-through/write-back 行为：

- `WriteThroughLocalSame`：本地同节点 write-through。
- `WriteThroughLocalDifferent`：本地不同节点 write-through。
- `WriteThroughRemoteSame`：远端同节点 write-through。
- `WriteThroughRemoteDifferent`：远端不同节点 write-through。
- `WriteBackRemoteSame`：远端同节点 write-back。
- `WriteBackLocalDifferent`：本地不同节点 write-back。
- `ReadLocalSmallSame`、`ReadLocalSmallDifferent`：本地小文件读。
- `ReadLocalLargeSame`、`ReadLocalLargeDifferent`：本地大文件读。
- `ReadRemoteSmallSame`、`ReadRemoteSmallDifferent`：远端小文件读。
- `ReadRemoteLargeSame`、`ReadRemoteLargeDifferent`：远端大文件读。

重点验证不同 nodeId、不同读写策略和不同文件大小下，数据写入和读取路径是否正确。

### 4.3 `tests/falcon_store/test_disk_cache.cpp`

- `Start`：验证 DiskCache 基础初始化。
- `StartWithZeroRatioUsesDirectFileChecks`：cache ratio 为 0 时走直接文件检查。
- `InsertUpdatePinAndDeleteLifecycle`：插入、更新、pin、unpin、删除 cache entry。
- `DeleteOldCacheSkipsPinnedEntry`：pinned entry 不会被老化删除。
- `StartScansExistingCacheFiles`：启动时扫描已有 cache 文件并重建元数据。
- `ZeroRatioStopModeCoversNoopBranches`：zero-ratio stop/noop 分支。
- `UtilityEnvironmentBranches`：环境变量缺失时使用默认值，例如 POD_IP。
- `PublicEvictAndFailureBranches`：覆盖容量不足、inode 不足、evict、删除失败、文件缺失、预分配失败。

### 4.4 `tests/falcon_store/test_file_lock.cpp`

- `TryLock`：参数化测试非阻塞锁获取。
- `WaitLock`：参数化测试等待锁释放。
- `TestLocked`：查询锁状态。
- `ReleaseMissingAndFileLockerBranches`：释放不存在锁、RAII `FileLocker` 构造和析构分支。

### 4.5 `tests/falcon_store/test_node.cpp`

- `CreateIOConnection`：创建 IO connection。
- `SetNodeConfig`：设置 node 配置。
- `GetNodeId`、`GetNodeIdEndpoint`：查询本地 node id 和 endpoint。
- `IsLocalId`、`IsLocalEndpoint`：判断是否本地节点。
- `GetRpcEndPoint`：根据 node id 获取 RPC endpoint。
- `GetNumberofAllNodes`、`GetAllNodeId`：查询集群节点数量和列表。
- `GetRpcConnection`：获取 RPC connection。
- `MissingNodeAndInvalidEndpointBranches`：node 缺失和 endpoint 非法。
- `AllocNode`、`GetNextNode`：node 分配策略。
- `UpdateNodeConfigByValueValid`、`UpdateNodeConfigByValueInvalid`：配置更新成功/失败。
- `DeleteNode`、`Delete`：节点删除和清理。

### 4.6 `tests/falcon_store/test_read_stream.cpp`

- `WaitPush`：测试等待队列 push。
- `WaitPop`：测试等待队列 pop。
- `PipeWaitPopPublicBoundaryBranches`：timeout、空队列、边界条件。
- `ReadStreamInit`：初始化 read stream。
- `ReadStreamReadZero`：读取 0 字节。
- `ReadStreamReadExceed`：读取超过范围。
- `ReadStreamReadHalf`：部分读取。
- `ReadStreamReadFull`：完整读取。

### 4.7 `tests/falcon_store/test_write_stream_coverage.cpp`

- `LocalDirectAndPersistErrorBranches`：direct write、空 buffer persist、fd 未设置。
- `LocalNonDirectPushAndEmptyPersistBranches`：非 direct buffer push 和空 persist。
- `LocalPersistCoversPwriteAndDiskCacheFailures`：pwrite 失败、DiskCache Add 失败。
- `CompletePersistsBufferedLocalData`：complete 时持久化 buffered local data。
- `RemoteBufferedPushAndSetFdBranches`：remote buffered write、重复 set fd。
- `RemoteClientSuccessAndErrorBranches`：mock remote 成功/失败。
- `MemoryAndSliceHelpersCoverInlineBranches`：内存和 slice helper inline 分支。
- `MergeCombinesDisjointAndOverlappingSlices`：coverage-only 测试 slice merge，对不相交、相交、连续区间合并。

## 5. falcon_client 模块 UT

### `tests/falcon_client/test_falcon_client_coverage.cpp`

#### 基础 helper

- `FalconClientErrorCodeUT.MapsKnownFalconErrorsToErrno`：Falcon 错误码到 errno 的映射。
- `FalconClientUtilsUT.ConvertsAndHashesBoundaryInputs`：路径转换、hash、边界输入。
- `BrpcMetaServiceJobCoverageUT.MapsTypesAndRejectsInvalidRequests`：metadata job 类型映射、非法请求拒绝。
- `FalconBrpcServerCoverageUT.RejectsOccupiedPortAndMissingStop`：端口占用、server 未 stop 等分支。

#### Connection

- `FalconClientConnectionUT.UnreachableEndpointCoversRequestBuilders`：不可达 endpoint 下，覆盖 request 构造和发送失败。
- `FalconClientConnectionUT.MockServerCoversSuccessfulResponseHandlers`：mock server 返回成功 response，覆盖 response handler。

#### Metadata API

- `FalconClientMetaUT.MockRouterCoversMetadataSuccessFlow`：mock router 下覆盖 mkdir/create/open/stat/rename 等成功路径。
- `LocalFdCloseBranchesWithoutService`：无服务情况下 close fd 分支。
- `CreateReturnsEmfileWhenFdTableIsFull`：fd table 满时返回 `EMFILE`。
- `RenamePersistRejectsDirectoriesBeforeCopy`：rename persist 前拒绝目录 copy。
- `ReaddirCachedEntriesWithoutService`：无服务下读取 cached readdir entries。
- `RouterRejectsCorruptShardTables`：router 拒绝损坏 shard table。
- `RouterUpdateAndFailureBranches`：router 更新、重复更新、失败分支。
- `MetadataFunctionsReturnServerErrors`：server 返回 metadata error 时客户端转换错误码。
- `ReaddirReturnsServerError`：readdir server error。
- `InvalidFdOperationsReturnErrorsWithoutService`：非法 fd 下 read/write/close/closedir 返回错误。
- `FalconFdLifecycleBranchesWithoutService`：fd 生命周期、重复 fd、fd slot timeout。

#### FUSE wrapper

- `InvalidArgumentsReturnEinval`：FUSE wrapper 对非法参数返回 `EINVAL`。
- `LocalBranchesWithoutService`：无服务下 local wrapper 分支。
- `MainStatsModeRejectsUnknownCommand`：stats mode 下拒绝未知命令。
- `FalconClientFuseWrapperServiceUT.MetadataAndIoWrappers`：服务可用时覆盖真实 metadata/io wrapper。

#### 服务依赖 metadata flow

- `FalconClientMetaServiceUT.CreateReadWriteRenameAndCleanup`：真实服务下 create/write/read/rename/cleanup。
- `DirectoryAndAttributeOperations`：真实服务下 mkdir/readdir/chmod/chown/utimens/rmdir。
- `RouterAndMissingPathBranches`：真实服务下 router 和 missing path。

## 6. falcon 模块 UT

### 6.1 `tests/falcon/test_falcon_concurrent_queue.cpp`

- `ConstructionAndSetup`：队列构造、初始状态、consumer 设置。
- `SingleElementEnqueueDequeue`：单线程 3 次入队、3 次出队，再从空队列出队。
- `MoveSemantics`：指针类型 move enqueue/dequeue。
- `BulkOperations`：批量入队、批量出队。
- `EdgeCases`：空队列出队、0 count bulk。
- `ProducerManagement`：producer 线程注册、active producer 数量。
- `MultipleProducers`：多个 producer 并发入队，单线程消费。
- `ConsumerThreadRestriction`：`SINGLE_CONSUMER` traits 下非 consumer 线程不能出队。
- `StatisticsEnabled`：`ENABLE_STATS` traits 下统计 enqueue/dequeue 次数。
- `PerformanceTest`：bulk enqueue/dequeue 的基础性能路径。
- `ConcurrentProducersConsumer`：多个 producer 和一个 consumer 并发读写。
- `ThreadExitCleanup`：producer 线程退出后的 producer info 清理。

备注：之前 CI 卡在 `SingleElementEnqueueDequeue`。该用例本身没有循环等待，挂住更可能来自 `ConcurrentQueue` 内部后台 GC 线程、`thread_local` producer cleanup 或析构 join 等生命周期问题。

### 6.2 `tests/falcon/test_connection_pool_coverage.cpp`

覆盖 C++ connection pool 和 worker task：

- worker task 析构时 fail/complete owned jobs。
- serialized data 编码/解码边界。
- 默认 job `MarkFailed` no-op。
- meta service type 到 batch service type 映射。
- shared memory allocator 初始化、分配、释放、满页、hint 分支。
- stubbed pool init/create/stop connections。
- worker task 非法输入。
- allocator failure。
- batch worker task 处理 stat indices、allocation drop。
- PostgreSQL reply shape 损坏。
- split reply 损坏。
- plain command result。
- zero reply shift。
- PostgreSQL error 转 response。

### 6.3 `tests/falcon/test_falcon_connection_pool_c_coverage.cpp`

- `InitializesSharedMemoryAllocator`：初始化 C 侧 shared-memory allocator。
- `ControlFlagsTrackBackgroundServiceAndAbortState`：control flag 跟踪 background service 和 abort 状态。
- `RunsCommunicationPluginAndDaemonMain`：运行 communication plugin 和 daemon main。

### 6.4 其他 falcon coverage UT

- `test_pg_connection_coverage.cpp`：PG connection 构造失败、prepare 失败、任务执行、notify stop、异常清理。
- `test_error_code_coverage.cpp`：错误码解析，覆盖空消息、encoded 消息、未知消息。
- `test_hcom_meta_service_interface_coverage.cpp`：operation type 名称、参数/response 初始化、variant get/set。
- `test_perf_counter_coverage.cpp`：perf counter shared memory、slot 分配、checkpoint、aggregate、release、broadcast、输出格式。
- `test_control_func_coverage.c`：control SQL function、control file、rollback-safe、非法参数。
- `test_control_hook_flag_coverage.c`：hook 开关、control flag 初始化/更新/读取。
- `test_dir_path_hash_coverage.c`：目录路径 shared-memory hash、LRU、SRF 输出、lock、invalid/missing path。
- `test_distributed_backend_coverage.c`：distributed backend SQL wrapper、remote worker dispatch、非法参数和失败返回。
- `test_remote_comm_coverage.c`：remote communication wrapper、plugin missing/failure、serialization、error conversion。
- `test_transaction_coverage.c`：distributed transaction 初始化、callback、commit/abort、prepared command helper。
- `test_transaction_cleanup_coverage.c`：transaction cleanup shared memory、cleanup worker、prepared command hash、orphan cleanup。
- `test_comm_plugin_coverage.c`：测试通信插件符号和插件生命周期。

## 7. metadb 模块 UT

### 7.1 `tests/falcon/metadb/test_metadb_dfs_flows_ut.cpp`

这些用例覆盖 DFS metadata 主流程：

- `DirectoryCreateListRemoveFlow`：mkdir、list、rmdir。
- `FileAttributeUpdateFlow`：create、stat、chmod、chown、utimens。
- `FileAndDirectoryRenameFlow`：文件和目录 rename。
- `MissingPathAttributeAndRenameFailureFlow`：missing path 下 stat/attribute/rename 失败。
- `DuplicateCreateAndMkdirFailureFlow`：重复 create/mkdir。
- `MissingPathOperationFailureFlow`：对不存在路径执行 unlink/rmdir/open 等操作。
- `TypeMismatchAndNonEmptyDirectoryFailureFlow`：文件/目录类型不匹配、非空目录删除失败。
- `CrossDirectoryRenameAndConflictFlow`：跨目录 rename 和目标冲突。
- `KvPutDeleteBoundaryFlow`：KV put/get/delete 边界。
- `SlicePutGetDeleteBoundaryFlow`：slice put/get/delete 边界。
- `UnlinkMakesFileInvisibleFlow`：unlink 后文件不可见。
- `DeepPathFileLifecycleFlow`：深层路径文件生命周期。
- `ConcurrentDirectoryAndFileCreateFlow`：并发创建目录和文件。
- `SliceIdConcurrentAndAllocatorIsolationFlow`：slice id 并发分配和 allocator 隔离。
- `InvalidFilenameBoundaryFlow`：非法文件名。

### 7.2 `tests/falcon/metadb/test_metadb_helper_ut.cpp`

- `SerializedMetaSubParamRoundTrip`：meta sub param 序列化反序列化。
- `SerializedMetaResponseEncodeFlow`：meta response 编码。
- `SerializedMetaSubResponseRoundTripAndErrorFlow`：meta sub response round trip 和错误。
- `SerializedKvResponseEncodeFlow`：KV response 编码。
- `SerializedSliceResponseEncodeFlow`：slice response 编码。
- `SerializedSliceIdResponseEncodeFlow`：slice id response 编码。
- `MetaProcessInfoPathComparatorFlow`：meta process info path comparator。

### 7.3 `tests/falcon/metadb/test_metadb_sql_serialized_ut.cpp`

- `PlainSqlDirectoryLifecycleFlow`：plain SQL 目录生命周期。
- `ControlSqlFunctionsRollbackSafeBranches`：control SQL rollback-safe 分支。
- `PlainSqlFileCreateStatAndReadDirFlow`：plain SQL 文件 create/stat/readdir。
- `DirPathHashSqlLockAndPrintFlow`：dir path hash SQL lock/print。
- `WrongWorkerAndInvalidPathSqlBranches`：wrong worker 和 invalid path。
- `SerializedBatchPublicApiAndSqlErrorBranches`：serialized batch public API 和 SQL error。
- `SerializedSubOperationSqlErrorBranches`：serialized sub operation SQL error。
- `AdminSqlCacheAndShardFlow`：admin cache/shard flow。
- `AdminSqlMoveShardErrorBranches`：move shard 错误分支；当前 extension 没有 `falcon_move_shard` 时 skip。
- `SerializedDirectoryFileAttributeFlow`：serialized directory/file/attribute。
- `SerializedRenameFlow`：serialized rename。
- `SerializedKvFlow`：serialized KV。
- `SerializedSliceFlow`：serialized slice。
- `AdminSqlClearDataFlowRunsLast`：最后执行 admin clear data 清理。

## 8. falcon_plugin 模块 UT

### 8.1 `tests/falcon_plugin/test_plugin_framework.cpp`

- `LoadInlinePluginSuccess`：成功加载 inline 插件。
- `LoadBackgroundPluginSuccess`：成功加载 background 插件。
- `LoadNonExistentPlugin`：插件不存在时失败。
- `InlinePluginFunctionCalls`：调用 inline plugin 的 get_type/work/cleanup。
- `BackgroundPluginFunctionCalls`：调用 background plugin work，验证 background 路径。

### 8.2 `tests/falcon_plugin/test_plugin_loader.cpp`

- `LoadPluginsFromValidDirectory`：从有效目录加载插件。
- `LoadPluginsFromNonExistentDirectory`：目录不存在。
- `LoadPluginsWithNullDirectory`：null directory。
- `EndToEndPluginWorkflow`：插件加载、执行、清理完整流程。

### 8.3 `tests/falcon_plugin/test_plugin_loader_coverage.cpp`

- `HandlesNullAndMissingPluginDirectories`：null/missing plugin dir。
- `InitializesShmemNodeInfoAndRunsPluginPhases`：初始化 shared-memory node info，执行 plugin init/work/cleanup。
- `BackgroundWorkerExitsWhenSlotIsMissing`：slot 缺失时 background worker 退出。
- `BackgroundWorkerExitsWhenPluginCannotLoad`：插件无法加载时 worker 退出。
- `BackgroundWorkerExitsWhenPluginMissingWorkFunctions`：插件缺少 work 函数时 worker 退出。

## 9. private-directory-test UT

### 9.1 `tests/private-directory-test/test_local_run_workload_ut.cpp`

- `InitCreateStatOpenCloseFlow`：初始化 metadata 环境，执行 create/stat/open/close。
- `DirectoryRenameAndAttributeFlow`：目录创建、读取、rename、chmod/chown/utimens、删除。
- `FullMetadataKvSliceFlow`：metadata + KV + slice 完整流程，覆盖 slice id、KV、slice 数据操作。

### 9.2 `tests/private-directory-test/test_local_run_posix_workload_ut.cpp`

- `InitCreateStatOpenCloseFlow`：POSIX-like 本地 workload 初始化、create/stat/open/close。
- `FullFileWorkloadFlow`：文件 create/write/read/rename/unlink 完整流程。

## 10. python_interface UT

### `tests/python_interface/test_pyfalconfs_internal.py`

覆盖 Python C extension 的同步和异步接口：

- module import 和动态库加载。
- `Init` 参数校验。
- mkdir/create/open/close/read/write/readdir 等 metadata API 参数类型校验。
- 同步 metadata API 基础行为。
- async API：
  - `AsyncExists`
  - iterator protocol
  - result polling
  - invalid path fast path
- PR #111 新增 `test_coverage_only_async_internals`：
  - 如果模块没有 `CoverageExerciseInternals`，说明不是 coverage build，显式 skip。
  - 覆盖 async worker 动态增长。
  - 覆盖 AsyncResultBase/AsyncResultIntOnly exception 转 Python exception。
- throw hook coverage：
  - 直接调用 `coverage_print_stacktrace(0)` 和 `coverage_print_stacktrace(1)`。
  - 覆盖 stacktrace helper，不依赖真实 C++ throw interpose。

## 11. skip 和 coverage-only 行为

### 11.1 coverage-only 宏

`FALCON_STORE_COVERAGE_TEST`

- 只在 coverage build 下启用。
- 暴露 FalconStore 私有 helper 给测试。
- 用于注入 mock storage、覆盖 storage helper，不影响正常 build。

`FALCON_PYTHON_COVERAGE_TEST`

- 只在 coverage build 下启用。
- 暴露 Python internal coverage helper。
- 普通 build 没有该 helper，Python 测试会 skip。

### 11.2 条件性 skip

`FalconCMIT`

- 依赖 `zk_endpoint`。
- 没有配置 ZooKeeper endpoint 时跳过。

`MetadbCoverageUT.AdminSqlMoveShardErrorBranches`

- 依赖 extension 中存在 `falcon_move_shard`。
- 当前 build 没有该函数时跳过。

服务依赖 client/metadb 用例

- `coverage --local-run` 会启动本地服务后执行。
- 普通无服务环境只跑 mock/local 分支，或通过条件判断跳过服务路径。

## 12. 稳定性备注

之前 CI run `26080475756` 卡在：

```text
FalconQueueUT.ConcurrentQueueTest.SingleElementEnqueueDequeue
```

从代码看该用例本身只做 3 次入队、3 次出队和一次空队列出队，没有 sleep、循环等待或服务依赖。卡住更可能来自 `ConcurrentQueue` 实现中的后台 GC 线程、`thread_local` producer cleanup、析构 join、锁生命周期等问题。

因此该问题更偏业务基础组件并发/生命周期风险在 UT 中暴露，而不是本 PR 的 mock storage、Python coverage-only helper 或服务覆盖用例导致。
