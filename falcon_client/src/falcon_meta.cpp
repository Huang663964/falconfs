/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "falcon_meta.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sys/stat.h>
#include <sys/time.h>

#include "buffer/dir_open_instance.h"
#include "cm/falcon_cm.h"
#include "disk_cache/disk_cache.h"
#include "falcon_store/falcon_store.h"
#include "inner_falcon_meta.h"
#include "router.h"
#include "utils.h"

constexpr int FILE_NUMBER_PER_EPOCH = 1048576;
constexpr int FILE_NUMBER_PER_WORKER = 4096;
constexpr std::size_t MAX_EVICT_UNLINK_WORKERS = 64;

std::shared_ptr<Router> router;

static int FalconUnlinkMetadataOnly(const std::string &path);

static std::size_t GetEvictUnlinkWorkerCount()
{
    const char *workerCount = std::getenv("EVICT_UNLINK_WORKERS");
    if (workerCount == nullptr) {
        return 1;
    }
    char *end = nullptr;
    unsigned long value = std::strtoul(workerCount, &end, 10);
    if (end == workerCount || value == 0) {
        FALCON_LOG(LOG_WARNING) << "Invalid EVICT_UNLINK_WORKERS value: " << workerCount << ", use 1";
        return 1;
    }
    if (value > MAX_EVICT_UNLINK_WORKERS) {
        FALCON_LOG(LOG_WARNING) << "EVICT_UNLINK_WORKERS " << value << " exceeds max "
                                << MAX_EVICT_UNLINK_WORKERS << ", clamp to " << MAX_EVICT_UNLINK_WORKERS;
        return MAX_EVICT_UNLINK_WORKERS;
    }
    return static_cast<std::size_t>(value);
}

static void UpdateAtomicMax(std::atomic<uint64_t> &target, uint64_t value)
{
    uint64_t current = target.load();
    while (current < value && !target.compare_exchange_weak(current, value)) {
    }
}

static uint64_t Percentile(std::vector<uint64_t> values, double q)
{
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    std::size_t idx = static_cast<std::size_t>(values.size() * q);
    if (idx >= values.size()) {
        idx = values.size() - 1;
    }
    return values[idx];
}

class FalconEvictUnlinkListener : public DiskCacheEvictListener {
  public:
    ~FalconEvictUnlinkListener() override { Stop(); }

    void Start()
    {
        auto localState = state;
        localState->workerCount = GetEvictUnlinkWorkerCount();
        localState->activeWorkers.store(localState->workerCount);
        workers.reserve(localState->workerCount);
        for (std::size_t i = 0; i < localState->workerCount; ++i) {
            workers.emplace_back(&FalconEvictUnlinkListener::Run, localState, i);
        }
        FALCON_LOG(LOG_WARNING) << "FalconEvictUnlinkListener started with " << localState->workerCount
                                << " worker(s)";
    }

    void Stop()
    {
        auto localState = state;
        {
            std::lock_guard<std::mutex> lock(localState->mutex);
            if (localState->stopped) {
                return;
            }
            localState->stopped = true;
        }
        localState->cv.notify_all();

        int timeoutMs = GetStopTimeoutMs();
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        while (localState->activeWorkers.load() > 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        bool timedOut = localState->activeWorkers.load() > 0;
        if (timedOut) {
            FALCON_LOG(LOG_WARNING) << "FalconEvictUnlinkListener stop timed out after " << timeoutMs
                                    << " ms, active workers = " << localState->activeWorkers.load()
                                    << ". Detached workers may still be blocked in metadata unlink RPC.";
        }

        for (auto &worker : workers) {
            if (!worker.joinable()) {
                continue;
            }
            if (timedOut) {
                worker.detach();
            } else {
                worker.join();
            }
        }
        LogStats(localState);
    }

    void OnEvicted(const EvictedItem &item) override
    {
        if (item.path.empty()) {
            FALCON_LOG(LOG_WARNING) << "Skip evicted inode " << item.inode << " without logical path";
            return;
        }
        auto localState = state;
        {
            std::lock_guard<std::mutex> lock(localState->mutex);
            if (localState->stopped) {
                return;
            }
            constexpr std::size_t kMaxPendingEvictions = 10000;
            constexpr std::size_t kHighWatermark = static_cast<std::size_t>(kMaxPendingEvictions * 8 / 10);
            if (localState->pending.size() >= kMaxPendingEvictions) {
                localState->droppedItems.fetch_add(1);
                FALCON_LOG(LOG_WARNING) << "FalconEvictUnlinkListener pending queue at capacity ("
                                        << kMaxPendingEvictions << "), dropping eviction for inode " << item.inode
                                        << " path " << item.path;
                return;
            }

            localState->pending.push(item);
            localState->enqueuedItems.fetch_add(1);
            UpdateAtomicMax(localState->maxPendingItems, static_cast<uint64_t>(localState->pending.size()));
            if (localState->pending.size() == kHighWatermark) {
                FALCON_LOG(LOG_WARNING) << "FalconEvictUnlinkListener pending queue reached high watermark: "
                                        << localState->pending.size() << " items; max capacity is "
                                        << kMaxPendingEvictions;
            }
        }
        localState->cv.notify_one();
    }

  private:
    struct SharedState {
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<EvictedItem> pending;
        bool stopped{false};
        std::size_t workerCount{1};
        std::atomic<uint64_t> activeWorkers{0};
        std::atomic<uint64_t> enqueuedItems{0};
        std::atomic<uint64_t> processedItems{0};
        std::atomic<uint64_t> succeededItems{0};
        std::atomic<uint64_t> failedItems{0};
        std::atomic<uint64_t> droppedItems{0};
        std::atomic<uint64_t> maxPendingItems{0};
        std::atomic<uint64_t> totalUnlinkLatencyUs{0};
        std::atomic<uint64_t> maxUnlinkLatencyUs{0};
        std::mutex statsMutex;
        std::vector<uint64_t> unlinkLatencySamples;
    };

    static int GetStopTimeoutMs()
    {
        constexpr int kDefaultStopTimeoutMs = 10000;
        const char *timeout = std::getenv("EVICT_UNLINK_STOP_TIMEOUT_MS");
        if (timeout == nullptr) {
            return kDefaultStopTimeoutMs;
        }
        char *end = nullptr;
        long value = std::strtol(timeout, &end, 10);
        if (end == timeout || value < 0) {
            FALCON_LOG(LOG_WARNING) << "Invalid EVICT_UNLINK_STOP_TIMEOUT_MS value: " << timeout
                                    << ", use " << kDefaultStopTimeoutMs;
            return kDefaultStopTimeoutMs;
        }
        return static_cast<int>(value);
    }

    static void Run(std::shared_ptr<SharedState> localState, std::size_t workerId)
    {
        while (true) {
            EvictedItem item;
            {
                std::unique_lock<std::mutex> lock(localState->mutex);
                localState->cv.wait(lock, [&localState]() { return localState->stopped || !localState->pending.empty(); });
                if (localState->stopped && localState->pending.empty()) {
                    localState->activeWorkers.fetch_sub(1);
                    return;
                }
                item = localState->pending.front();
                localState->pending.pop();
            }

            auto start = std::chrono::steady_clock::now();
            int ret = FalconUnlinkMetadataOnly(item.path);
            uint64_t elapsedUs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start)
                    .count());
            localState->processedItems.fetch_add(1);
            localState->totalUnlinkLatencyUs.fetch_add(elapsedUs);
            UpdateAtomicMax(localState->maxUnlinkLatencyUs, elapsedUs);
            {
                std::lock_guard<std::mutex> lock(localState->statsMutex);
                localState->unlinkLatencySamples.push_back(elapsedUs);
            }
            if (ret != SUCCESS) {
                localState->failedItems.fetch_add(1);
                FALCON_LOG(LOG_WARNING) << "Evict unlink failed for path " << item.path << ", worker " << workerId
                                        << ", error code: " << ret;
            } else {
                localState->succeededItems.fetch_add(1);
            }
        }
    }

    static void LogStats(const std::shared_ptr<SharedState> &localState)
    {
        uint64_t processed = localState->processedItems.load();
        uint64_t avgLatencyUs = processed == 0 ? 0 : localState->totalUnlinkLatencyUs.load() / processed;
        std::vector<uint64_t> latencySamples;
        {
            std::lock_guard<std::mutex> lock(localState->statsMutex);
            latencySamples = localState->unlinkLatencySamples;
        }
        FALCON_LOG(LOG_WARNING) << "FalconEvictUnlinkListener stopped, workers = " << localState->workerCount
                                << ", enqueued = " << localState->enqueuedItems.load()
                                << ", processed = " << processed
                                << ", succeeded = " << localState->succeededItems.load()
                                << ", failed = " << localState->failedItems.load()
                                << ", dropped = " << localState->droppedItems.load()
                                << ", max pending = " << localState->maxPendingItems.load()
                                << ", avg unlink latency us = " << avgLatencyUs
                                << ", p95 unlink latency us = " << Percentile(latencySamples, 0.95)
                                << ", p99 unlink latency us = " << Percentile(latencySamples, 0.99)
                                << ", max unlink latency us = " << localState->maxUnlinkLatencyUs.load();
    }

    std::shared_ptr<SharedState> state{std::make_shared<SharedState>()};
    std::vector<std::thread> workers;
};

std::unique_ptr<FalconEvictUnlinkListener> evictUnlinkListener;

void StartEvictUnlinkListener()
{
    if (evictUnlinkListener != nullptr) {
        return;
    }
    evictUnlinkListener = std::make_unique<FalconEvictUnlinkListener>();
    evictUnlinkListener->Start();
    DiskCache::GetInstance().SetEvictListener(evictUnlinkListener.get());
}

void StopEvictUnlinkListener()
{
    DiskCache::GetInstance().SetEvictListener(nullptr);
    if (evictUnlinkListener == nullptr) {
        return;
    }
    evictUnlinkListener->Stop();
    evictUnlinkListener.reset();
}

int FalconInit(std::string &coordinatorIp, int coordinatorPort)
{
    int ret = FalconStore::GetInstance()->GetInitStatus();
    if (ret != SUCCESS) {
        return ret;
    }
    ServerIdentifier coordinator(coordinatorIp, coordinatorPort);
    router = std::make_shared<Router>(coordinator);
    StartEvictUnlinkListener();
    return 0;
}

int FalconInitWithZK(std::string zkEndPoint, const std::string &zkPath)
{
    int ret = FalconCM::GetInstance(zkEndPoint, 10000, zkPath)->GetInitStatus();
    if (ret != SUCCESS) {
        return ret;
    }
    // init connection to zk, fetch meta ready info from zk
    FalconCM::GetInstance()->CheckMetaDataStatus();
    {
        std::mutex mu;
        std::unique_lock<std::mutex> lock(mu);
        FalconCM::GetInstance()->GetMetaDataReadyCv().wait(lock, []() {
            return FalconCM::GetInstance()->GetMetaDataStatus();
        });
    }
    // init store, and upload node info to zk
    ret = FalconStore::GetInstance()->GetInitStatus();
    if (ret != SUCCESS) {
        return ret;
    }
    std::string coordinatorIp;
    int coordinatorPort = 0;
    ret = FalconCM::GetInstance()->FetchCoordinatorInfo(coordinatorIp, coordinatorPort);
    if (ret != SUCCESS) {
        return ret;
    }
    ServerIdentifier coordinator(coordinatorIp, coordinatorPort);
    router = std::make_shared<Router>(coordinator);
    StartEvictUnlinkListener();
    return 0;
}

int FalconMkdir(const std::string &path)
{
    std::shared_ptr<Connection> conn = router->GetCoordinatorConn();
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->Mkdir(path.c_str());
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->Mkdir(path.c_str());
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconMkdir failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconCreate(const std::string &path, uint64_t &fd, int oflags, struct stat *stbuf)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    uint64_t inodeId;
    int32_t nodeId;
    int errorCode = conn->Create(path.c_str(), inodeId, nodeId, stbuf);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Create(path.c_str(), inodeId, nodeId, stbuf);
    }
#endif
    /* Handle the case of not exclusively created file */
    if (errorCode == FILE_EXISTS && !(oflags & O_EXCL)) {
        errorCode = SUCCESS;
    }
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconCreate failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
        return errorCode; 
    }

    fd = FalconFd::GetInstance()->AttachFd(inodeId, oflags, nullptr, stbuf->st_size, path, nodeId);
    if (fd == UINT64_MAX) {
        FalconFd::GetInstance()->DeleteOpenInstance(fd);
        return -EMFILE;
    }
    return SUCCESS;
}

int FalconGetStat(const std::string &path, struct stat *stbuf)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Stat(path.c_str(), stbuf);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Stat(path.c_str(), stbuf);
    }
#endif
    if (errorCode != SUCCESS && errorCode != FILE_NOT_EXISTS) {
        FALCON_LOG(LOG_ERROR) << "FalconGetStat failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconOpen(const std::string &path, int oflags, uint64_t &fd, struct stat *stbuf)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->WaitGetNewOpenInstance();
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "new openInstance failed";
        return -EMFILE;
    }
    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Open(path.c_str(), inodeId, size, nodeId, stbuf);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Open(path.c_str(), inodeId, size, nodeId, stbuf);
    }
#endif
    if (errorCode != SUCCESS) {
        FalconFd::GetInstance()->ReleaseOpenInstance();
        FALCON_LOG(LOG_ERROR) << "FalconOpen failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    openInstance->inodeId = inodeId;
    openInstance->originalSize = size;
    openInstance->currentSize = size;
    openInstance->nodeId = nodeId;
    openInstance->path = path;
    openInstance->oflags = oflags;

    /******************* Fetch open meta finish ************************/

    /* allocate fd and handle the small file read */
    if (errorCode == SUCCESS) {
        if (openInstance->originalSize > 0 && openInstance->originalSize < READ_BIGFILE_SIZE &&
            (openInstance->oflags & O_ACCMODE) == O_RDONLY) {
            // For small files: read all when open
            std::shared_ptr<char> buffer;
            if (openInstance->oflags & __O_DIRECT) {
                int alignedNum = openInstance->originalSize / 512 + int(openInstance->originalSize % 512 != 0);
                buffer = std::shared_ptr<char>((char *)aligned_alloc(512, 512 * alignedNum), free);
            } else {
                buffer = std::shared_ptr<char>((char *)malloc(openInstance->originalSize), free);
            }
            if (buffer == nullptr) {
                FALCON_LOG(LOG_ERROR) << "In FalconOpen() malloc failed";
                FalconFd::GetInstance()->ReleaseOpenInstance();
                return -ENOMEM;
            }
            openInstance->readBuffer = buffer;
            openInstance->readBufferSize = openInstance->originalSize;
            int ret = InnerFalconReadSmallFiles(openInstance.get());
            if (ret < 0) {
                FalconFd::GetInstance()->ReleaseOpenInstance();
                return ret;
            }
        }
        fd = FalconFd::GetInstance()->AttachFd(path, openInstance);
    }
    return errorCode;
}

int FalconClose(const std::string &path, uint64_t fd, bool isFlush, int datasync)
{
    OpenInstance *openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd).get();
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "In FalconClose(): fd not found for openInstance";
        return NOT_FOUND_FD;
    }

    size_t size = openInstance->currentSize;
    // only read small files does not open file
    if (openInstance->isOpened) {
        int innerRet = InnerFalconTmpClose(openInstance, isFlush, datasync >= 0); // here may fail, mark in writeFail
        if (innerRet != 0) {
            if (!isFlush) {
                FalconFd::GetInstance()->DeleteOpenInstance(fd);
            }
            return innerRet;
        }
    }
    /* update only once if truncate */
    if (openInstance->readFail || openInstance->writeFail || datasync > 0 ||
        (!openInstance->nodeFail && size == openInstance->originalSize)) {
        bool readFail = openInstance->readFail;
        if (!isFlush) {
            FalconFd::GetInstance()->DeleteOpenInstance(fd);
        }
        if (readFail) {
            return -EIO;
        }
        return SUCCESS;
    }

    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->Close(path.c_str(), size, 0, openInstance->nodeId);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Close(path.c_str(), size, 0, openInstance->nodeId);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconClose failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    openInstance->originalSize = size;
    if (!isFlush) {
        FalconFd::GetInstance()->DeleteOpenInstance(fd);
    }
    return errorCode;
}

static int FalconUnlinkMetadataOnly(const std::string &path)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Unlink(path.c_str(), inodeId, size, nodeId);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Unlink(path.c_str(), inodeId, size, nodeId);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconUnlinkMetadataOnly failed for path: " << path << ", DN: " << conn->server.id
                              << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }

    return errorCode;
}

int FalconUnlink(const std::string &path)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Unlink(path.c_str(), inodeId, size, nodeId);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Unlink(path.c_str(), inodeId, size, nodeId);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconUnlink failed for path: " << path << ", DN: " << conn->server.id << ", ip: "
                              << conn->server.ip << ", error code: " << errorCode;
    }
    int ret = 0;
    if (errorCode == SUCCESS) {
        // delete data
        ret = InnerFalconUnlink(inodeId, nodeId, path);
        if (ret != 0) {
            FALCON_LOG(LOG_WARNING) << "In FalconUnlink(): delete cache " << path << " failed";
        }
    }

    return errorCode;
}

int FalconReadDir(const std::string &path, void *buf, FalconFuseFiller filler, off_t offset, struct FalconFuseInfo *fi)
{
    uint64_t fd = fi->fh;
    int idx = offset;
    std::unordered_map<std::string, std::shared_ptr<Connection>> workerInfo;
    int ret = SUCCESS;

    DirOpenInstance *dirOpenInstance = FalconFd::GetInstance()->GetDirOpenInstanceByFd(fd);
    if (dirOpenInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "In FalconReadDir(): fd not found for dirOpenInstance";
        return NOT_FOUND_FD;
    }
    if (offset == 0) {
        /* offset == 0 means that the readdir function is the first called, we should prepare the connection to all dn
         */
        ret = router->GetAllWorkerConnection(workerInfo);

        if (ret != SUCCESS) {
            FALCON_LOG(LOG_ERROR) << "FalconReadDir failed for path: " << path << ", GET_ALL_WORKER_CONN_FAILED";
            return GET_ALL_WORKER_CONN_FAILED;
        }
        dirOpenInstance->SetAllWorkerInfo(workerInfo);
        idx = 1;
        filler(buf, ".", nullptr, idx++);
        filler(buf, "..", nullptr, idx++);
    }

    int workerNotFinished = dirOpenInstance->workingWorkers.size();
    if (dirOpenInstance->offset >= dirOpenInstance->partialEntryVec.size() && workerNotFinished != 0) {
        uint32_t fileNumberPerWorker = std::min(FILE_NUMBER_PER_EPOCH / workerNotFinished, FILE_NUMBER_PER_WORKER);
        dirOpenInstance->workers.clear();
        dirOpenInstance->workers = dirOpenInstance->workingWorkers;
        dirOpenInstance->workingWorkers.clear();
        dirOpenInstance->partialEntryVec.clear();
        dirOpenInstance->offset = 0;
        for (auto it = dirOpenInstance->workers.begin(); it != dirOpenInstance->workers.end(); ++it) {
            std::string ipPort = it->first;
            std::shared_ptr<Connection> conn = it->second;
            Connection::ReadDirResponse readDirResponse;
            ret = conn->ReadDir(path.c_str(),
                                readDirResponse,
                                fileNumberPerWorker,
                                dirOpenInstance->lastShardIndexes[ipPort],
                                dirOpenInstance->lastFileNames[ipPort].empty()
                                    ? nullptr
                                    : dirOpenInstance->lastFileNames[ipPort].c_str());
#ifdef ZK_INIT
            int cnt = 0;
            while (cnt < RETRY_CNT && ret == SERVER_FAULT) {
                ++cnt;
                sleep(SLEEPTIME);
                conn = router->TryToUpdateWorkerConn(conn);
                ret = conn->ReadDir(path.c_str(),
                                    readDirResponse,
                                    fileNumberPerWorker,
                                    dirOpenInstance->lastShardIndexes[ipPort],
                                    dirOpenInstance->lastFileNames[ipPort].empty()
                                        ? nullptr
                                        : dirOpenInstance->lastFileNames[ipPort].c_str());
            }
#endif
            if (ret != SUCCESS) {
                FALCON_LOG(LOG_ERROR) << "FalconReadDir failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << ret;
                return ret;
            }
            dirOpenInstance->lastShardIndexes[ipPort] = readDirResponse.response->last_shard_index();
            if (readDirResponse.response->last_file_name() == nullptr)
                dirOpenInstance->lastFileNames[ipPort] = "";
            else
                dirOpenInstance->lastFileNames[ipPort] = readDirResponse.response->last_file_name()->str();
            auto result_list = readDirResponse.response->result_list();

            // fill the fuse readdir buffer using metadata
            for (unsigned i = 0; i < result_list->size(); i++) {
                dirOpenInstance->partialEntryVec.push_back(result_list->Get(i)->file_name()->c_str());
                dirOpenInstance->fileModes.push_back(result_list->Get(i)->st_mode());
            }
            if (result_list->size() < fileNumberPerWorker && ret == SUCCESS) {
                dirOpenInstance->lastFileNames.erase(ipPort);
            } else {
                dirOpenInstance->workingWorkers.emplace(ipPort, conn);
            }
        }
    }
    for (size_t i = dirOpenInstance->offset; i < dirOpenInstance->partialEntryVec.size(); i++) {
        struct stat st;
        (void)memset(&st, 0, sizeof(st));

        st.st_mode = static_cast<mode_t>(dirOpenInstance->fileModes[i]);
        if (filler(buf, dirOpenInstance->partialEntryVec[i].c_str(), &st, idx++)) {
            dirOpenInstance->offset = i;
            return 0;
        }
    }
    dirOpenInstance->offset = dirOpenInstance->partialEntryVec.size();
    return ret;
}

int FalconOpenDir(const std::string &path, struct FalconFuseInfo *fi)
{
    std::shared_ptr<Connection> conn = router->GetCoordinatorConn();
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    uint64_t inodeId = 0;
    int errorCode = conn->OpenDir(path.c_str(), inodeId);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->OpenDir(path.c_str(), inodeId);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconOpenDir failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    if (errorCode == 0) {
        uint64_t fd = 0;
        fd = FalconFd::GetInstance()->AttachDirFd(errorCode);
        fi->fh = fd;
    }
    return errorCode;
}

int FalconCloseDir(uint64_t fd)
{
    DirOpenInstance *dirOpenInstance = FalconFd::GetInstance()->GetDirOpenInstanceByFd(fd);
    if (dirOpenInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "In FalconCloseDir(): fd not found for dirOpenInstance";
        return NOT_FOUND_FD;
    }

    int delRet = FalconFd::GetInstance()->DeleteDirOpenInstance(fd);
    return delRet;
}

int FalconDestroy()
{
    StopEvictUnlinkListener();
    FalconStore::GetInstance()->DeleteInstance();

    return 0;
}

int FalconRmDir(const std::string &path)
{
    std::shared_ptr<Connection> conn = router->GetCoordinatorConn();
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Rmdir(path.c_str());
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->Rmdir(path.c_str());
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconRmDir failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconWrite(uint64_t fd, const std::string & /*path*/, const char *buffer, size_t size, off_t offset)
{
    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "In FalconWrite(): fd not found for openInstance";
        return -EBADF;
    }

    openInstance->writeCnt++;
    int ret = InnerFalconWrite(openInstance.get(), buffer, size, offset);
    return ret;
}

int FalconRead(const std::string & /*path*/, uint64_t fd, char *buffer, size_t size, off_t offset)
{
    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    if (openInstance == nullptr) {
        FALCON_LOG(LOG_ERROR) << "In FalconRead(): fd not found for openInstance";
        return -EBADF;
    }

    int ret = InnerFalconRead(openInstance.get(), buffer, size, offset);
    if (ret < 0) {
        openInstance->readFail = true;
    }
    return ret;
}

int FalconRename(const std::string &srcName, const std::string &dstName)
{
    struct stat srcStat;
    (void)memset(&srcStat, 0, sizeof(srcStat));
    int statRet = FalconGetStat(srcName, &srcStat);

    std::shared_ptr<Connection> conn = router->GetCoordinatorConn();
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Rename(srcName.c_str(), dstName.c_str());
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->Rename(srcName.c_str(), dstName.c_str());
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconRename failed for srcName: " << srcName << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    if (errorCode == SUCCESS && statRet == SUCCESS) {
        DiskCache::GetInstance().UpdatePath(srcStat.st_ino, dstName);
    }
    return errorCode;
}

int FalconRenamePersist(const std::string &srcName, const std::string &dstName)
{
    struct stat stbuf;
    (void)memset(&stbuf, 0, sizeof(stbuf));

    int ret = 0;
    ret = FalconGetStat(srcName, &stbuf);
    if (ret != 0) {
        return ret;
    }
    if (!S_ISREG(stbuf.st_mode)) {
        // we do not support rename directory
        return -EOPNOTSUPP;
    }
    // first copy the data in obs
    ret = InnerFalconCopydata(srcName, dstName);
    if (ret != 0) {
        return ret;
    }
    // update the metadata for rename
    std::shared_ptr<Connection> conn = router->GetCoordinatorConn();
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Rename(srcName.c_str(), dstName.c_str());
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->Rename(srcName.c_str(), dstName.c_str());
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconRenamePersist failed for srcName: " << srcName << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    if (errorCode == SUCCESS) {
        DiskCache::GetInstance().UpdatePath(stbuf.st_ino, dstName);
        // delete src object
        InnerFalconDeleteDataAfterRename(srcName);
    } else {
        // delete dst object
        InnerFalconDeleteDataAfterRename(dstName);
    }
    return errorCode;
}

int FalconFsync(const std::string &path, uint64_t fd, int datasync)
{
    return FalconClose(path, fd, true, datasync == 0 ? 0 : 1);
}

int FalconStatFS(struct statvfs *vfsbuf)
{
    int ret = InnerFalconStatFS(vfsbuf);
    return ret;
}

int FalconUtimens(const std::string &path, int64_t accessTime, int64_t modifyTime)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->UtimeNs(path.c_str(), accessTime, modifyTime);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->UtimeNs(path.c_str(), accessTime, modifyTime);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconUtimens failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconChown(const std::string &path, uid_t uid, gid_t gid)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->Chown(path.c_str(), uid, gid);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Chown(path.c_str(), uid, gid);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconChown failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconChmod(const std::string &path, mode_t mode)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->Chmod(path.c_str(), mode);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Chmod(path.c_str(), mode);
    }
#endif
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconChmod failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

// User shouldn't cmake concurrent truncate and open
int FalconTruncate(const std::string &path, off_t size)
{
    int ret = 0;
    uint64_t inodeId = 0;

    int oflags = O_WRONLY;
    uint64_t fd = 0;

    struct stat st;
    memset(&st, 0, sizeof(st));
    ret = FalconOpen(path, oflags, fd, &st);
    if (ret != 0) {
        FalconClose(path, fd, true, -1);
        FalconClose(path, fd, false, -1);
        return ret;
    }
    std::shared_ptr<OpenInstance> openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
    inodeId = openInstance->inodeId;
    auto originalSize = openInstance->originalSize;

    // truncate the concurrent openInstances, update the size
    auto openInstanceSet = FalconFd::GetInstance()->GetInodetoOpenInstanceSet(inodeId);
    for (auto &openInstance : openInstanceSet) {
        ret = InnerFalconTruncateOpenInstance(openInstance.get(), size);
    }

    // truncate the cache file, which may not exist. Must called after TruncateOpenInstance, for size and obs update
    openInstance->writeCnt++;
    openInstance->originalSize = originalSize;

    ret = InnerFalconTruncateFile(openInstance.get(), size);
    if (ret != 0) {
        openInstance->writeFail = true;
        FALCON_LOG(LOG_ERROR) << "truncateFile failed, ret = " << ret;
    }

    // flush and release must called to flush obs, update diskCache, delete openInstance, and update meta
    int flushRet = FalconClose(path, fd, true, -1);
    if (flushRet != 0) {
        ret = ret == 0 ? flushRet : ret;
        FALCON_LOG(LOG_ERROR) << "Truncate Flush File failed, ret = " << ret;
    }
    int releaseRet = FalconClose(path, fd, false, -1);
    if (releaseRet != 0) {
        ret = ret == 0 ? releaseRet : ret;
        FALCON_LOG(LOG_ERROR) << "Truncate Release File failed, ret = " << ret;
    }
    if (ret != 0) {
        return ret;
    }

    return ret;
}
