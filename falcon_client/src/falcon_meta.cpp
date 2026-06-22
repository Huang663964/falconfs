/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "falcon_meta.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
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
constexpr std::size_t UNLINK_LATENCY_SAMPLE_LIMIT = 4096;
constexpr std::size_t UNLINK_FAILURE_LOG_SAMPLE_LIMIT = 3;
std::shared_ptr<Router> router;

static void UpdateAtomicMax(std::atomic<uint64_t> &target, uint64_t value)
{
    uint64_t current = target.load();
    while (current < value && !target.compare_exchange_weak(current, value)) {
    }
}

static const char *FalconErrorCodeName(int errorCode)
{
    switch (errorCode) {
    case SUCCESS:
        return "SUCCESS";
    case PROGRAM_ERROR:
        return "PROGRAM_ERROR";
    case LEASE_CONFLICT:
        return "LEASE_CONFLICT";
    case WRONG_WORKER:
        return "WRONG_WORKER";
    case FILE_NOT_EXISTS:
        return "FILE_NOT_EXISTS";
    case PATH_NOT_EXISTS:
        return "PATH_NOT_EXISTS";
    case SERVER_FAULT:
        return "SERVER_FAULT";
    default:
        return "UNKNOWN";
    }
}

static void ReleaseMetadataLeaseBestEffort(const std::shared_ptr<Connection> &conn,
                                           const std::string &path,
                                           int64_t size,
                                           int32_t nodeId,
                                           const char *reason)
{
    if (conn == nullptr) {
        return;
    }
    FalconErrorCode ret = conn->Close(path.c_str(), size, 0, nodeId);
    if (ret != SUCCESS) {
        FALCON_LOG(LOG_WARNING) << "release metadata lease failed, path=" << path
                                << ", node=" << nodeId << ", size=" << size
                                << ", reason=" << reason << ", error=" << ret;
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

struct UnlinkFailureSummary {
    uint64_t total{0};
    std::vector<std::string> samples;
};

static void AddUnlinkFailure(UnlinkFailureSummary &summary, const EvictedItem &item, int ret, uint64_t elapsedUs)
{
    ++summary.total;
    if (summary.samples.size() >= UNLINK_FAILURE_LOG_SAMPLE_LIMIT) {
        return;
    }
    std::ostringstream oss;
    oss << "{path=" << item.path << ", inode=" << item.inode << ", size=" << item.size
        << ", error=" << ret << "(" << FalconErrorCodeName(ret) << ")"
        << ", elapsed_us=" << elapsedUs << "}";
    summary.samples.push_back(oss.str());
}

static std::string JoinUnlinkSamples(const std::vector<std::string> &samples)
{
    std::ostringstream oss;
    for (std::size_t i = 0; i < samples.size(); ++i) {
        if (i > 0) {
            oss << "; ";
        }
        oss << samples[i];
    }
    return oss.str();
}

class FalconEvictUnlinkListener : public DiskCacheEvictListener {
  public:
    ~FalconEvictUnlinkListener() override { Stop(); }

    void Start()
    {
        FALCON_LOG(LOG_WARNING) << "FalconEvictUnlinkListener started in pre-evict mode";
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
        LogStats(localState);
    }

    void OnEvictingBatch(const std::vector<EvictedItem> &items, std::vector<bool> &results) override
    {
        results.assign(items.size(), false);
        auto localState = state;
        {
            std::lock_guard<std::mutex> lock(localState->mutex);
            if (localState->stopped) {
                return;
            }
        }

        struct BatchGroup {
            std::shared_ptr<Connection> conn;
            std::vector<std::size_t> indices;
            std::vector<std::string> paths;
            std::vector<uint64_t> inodes;
        };
        std::vector<BatchGroup> groups;
        UnlinkFailureSummary routeFailureSummary;

        for (std::size_t i = 0; i < items.size(); ++i) {
            const auto &item = items[i];
            if (item.path.empty()) {
                FALCON_LOG(LOG_WARNING) << "Skip evicted inode " << item.inode << " without logical path";
                results[i] = true;
                continue;
            }

            std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(item.path);
            if (!conn) {
                localState->enqueuedItems.fetch_add(1);
                RecordUnlinkResult(localState, PROGRAM_ERROR, 0);
                AddUnlinkFailure(routeFailureSummary, item, PROGRAM_ERROR, 0);
                continue;
            }

            auto groupIt = std::find_if(groups.begin(), groups.end(), [&conn](const BatchGroup &group) {
                return group.conn->server == conn->server;
            });
            if (groupIt == groups.end()) {
                groups.push_back({conn, {}, {}, {}});
                groupIt = std::prev(groups.end());
            }
            groupIt->indices.push_back(i);
            groupIt->paths.push_back(item.path);
            groupIt->inodes.push_back(item.inode);
        }

        if (routeFailureSummary.total > 0) {
            FALCON_LOG(LOG_WARNING) << "Evict unlink route failed batch, failed = " << routeFailureSummary.total
                                    << ", samples = " << JoinUnlinkSamples(routeFailureSummary.samples);
        }

        for (auto &group : groups) {
            localState->batchCalls.fetch_add(1);
            UpdateAtomicMax(localState->maxBatchSize, group.indices.size());
            auto start = std::chrono::steady_clock::now();
            std::vector<FalconErrorCode> unlinkResults;
            int ret = group.conn->BatchUnlinkIfInodeMatch(group.paths, group.inodes, unlinkResults);
#ifdef ZK_INIT
            int cnt = 0;
            while (cnt < RETRY_CNT && ret == SERVER_FAULT) {
                ++cnt;
                sleep(SLEEPTIME);
                group.conn = router->TryToUpdateWorkerConn(group.conn);
                ret = group.conn->BatchUnlinkIfInodeMatch(group.paths, group.inodes, unlinkResults);
            }
#endif
            uint64_t elapsedUs = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start)
                    .count());
            uint64_t itemElapsedUs = group.indices.empty() ? elapsedUs : elapsedUs / group.indices.size();
            if (ret != SUCCESS && unlinkResults.size() != group.indices.size()) {
                unlinkResults.assign(group.indices.size(), static_cast<FalconErrorCode>(ret));
            }

            UnlinkFailureSummary failureSummary;
            for (std::size_t i = 0; i < group.indices.size(); ++i) {
                std::size_t itemIndex = group.indices[i];
                FalconErrorCode itemRet = i < unlinkResults.size() ? unlinkResults[i] : PROGRAM_ERROR;
                localState->enqueuedItems.fetch_add(1);
                RecordUnlinkResult(localState, itemRet, itemElapsedUs);
                results[itemIndex] = itemRet == SUCCESS;
                if (itemRet != SUCCESS) {
                    AddUnlinkFailure(failureSummary, items[itemIndex], itemRet, itemElapsedUs);
                }
            }
            if (failureSummary.total > 0) {
                FALCON_LOG(LOG_WARNING) << "Evict unlink failed batch, DN = " << group.conn->server.id
                                        << ", ip = " << group.conn->server.ip
                                        << ", port = " << group.conn->server.port
                                        << ", batch_size = " << group.indices.size()
                                        << ", failed = " << failureSummary.total
                                        << ", elapsed_us = " << elapsedUs
                                        << ", samples = " << JoinUnlinkSamples(failureSummary.samples);
            }
        }
    }

    void OnEvicted(const EvictedItem &item) override
    {
        (void)item;
    }

  private:
    struct SharedState {
        std::mutex mutex;
        bool stopped{false};
        std::atomic<uint64_t> enqueuedItems{0};
        std::atomic<uint64_t> processedItems{0};
        std::atomic<uint64_t> succeededItems{0};
        std::atomic<uint64_t> failedItems{0};
        std::atomic<uint64_t> batchCalls{0};
        std::atomic<uint64_t> maxBatchSize{0};
        std::atomic<uint64_t> totalUnlinkLatencyUs{0};
        std::atomic<uint64_t> maxUnlinkLatencyUs{0};
        std::mutex statsMutex;
        std::vector<uint64_t> unlinkLatencySamples;
    };

    static void RecordUnlinkResult(const std::shared_ptr<SharedState> &localState, int ret, uint64_t elapsedUs)
    {
        localState->processedItems.fetch_add(1);
        localState->totalUnlinkLatencyUs.fetch_add(elapsedUs);
        UpdateAtomicMax(localState->maxUnlinkLatencyUs, elapsedUs);
        {
            std::lock_guard<std::mutex> lock(localState->statsMutex);
            if (localState->unlinkLatencySamples.size() < UNLINK_LATENCY_SAMPLE_LIMIT) {
                localState->unlinkLatencySamples.push_back(elapsedUs);
            }
        }
        if (ret != SUCCESS) {
            localState->failedItems.fetch_add(1);
        } else {
            localState->succeededItems.fetch_add(1);
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
        uint64_t failed = localState->failedItems.load();
        std::ostringstream oss;
        oss << "FalconEvictUnlinkListener stopped, enqueued = " << localState->enqueuedItems.load()
            << ", processed = " << processed
            << ", succeeded = " << localState->succeededItems.load()
            << ", failed = " << failed
            << ", batch calls = " << localState->batchCalls.load()
            << ", max batch size = " << localState->maxBatchSize.load()
            << ", latency samples = " << latencySamples.size()
            << ", avg unlink latency us = " << avgLatencyUs
            << ", p95 unlink latency us = " << Percentile(latencySamples, 0.95)
            << ", p99 unlink latency us = " << Percentile(latencySamples, 0.99)
            << ", max unlink latency us = " << localState->maxUnlinkLatencyUs.load();
        if (failed > 0) {
            FALCON_LOG(LOG_WARNING) << oss.str();
        } else {
            FALCON_LOG(LOG_INFO) << oss.str();
        }
    }

    std::shared_ptr<SharedState> state{std::make_shared<SharedState>()};
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
    int64_t size = 0;
    int32_t nodeId;
    struct stat fallbackStbuf;
    (void)memset(&fallbackStbuf, 0, sizeof(fallbackStbuf));
    struct stat *createStat = stbuf != nullptr ? stbuf : &fallbackStbuf;
    int errorCode = conn->Create(path.c_str(), inodeId, nodeId, createStat);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateWorkerConn(conn);
        errorCode = conn->Create(path.c_str(), inodeId, nodeId, createStat);
    }
#endif
    /* Handle the case of not exclusively created file. Open acquires the metadata lease. */
    if (errorCode == FILE_EXISTS && !(oflags & O_EXCL)) {
        errorCode = conn->Open(path.c_str(), inodeId, size, nodeId, createStat);
#ifdef ZK_INIT
        cnt = 0;
        while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
            ++cnt;
            sleep(SLEEPTIME);
            conn = router->TryToUpdateWorkerConn(conn);
            errorCode = conn->Open(path.c_str(), inodeId, size, nodeId, createStat);
        }
#endif
    } else if (errorCode == SUCCESS) {
        size = createStat->st_size;
    }
    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconCreate failed for path: " << path << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
        return errorCode; 
    }

    fd = FalconFd::GetInstance()->AttachFd(inodeId, oflags, nullptr, size, path, nodeId);
    if (fd == UINT64_MAX) {
        ReleaseMetadataLeaseBestEffort(conn, path, size, nodeId, "attach_fd_failed");
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
        return errorCode;
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
                ReleaseMetadataLeaseBestEffort(conn, path, size, nodeId, "read_buffer_alloc_failed");
                FalconFd::GetInstance()->ReleaseOpenInstance();
                return -ENOMEM;
            }
            openInstance->readBuffer = buffer;
            openInstance->readBufferSize = openInstance->originalSize;
            int ret = InnerFalconReadSmallFiles(openInstance.get());
            if (ret < 0) {
                ReleaseMetadataLeaseBestEffort(conn, path, size, nodeId, "small_file_preread_failed");
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
    bool readFail = openInstance->readFail;
    // only read small files does not open file
    if (openInstance->isOpened) {
        int innerRet = InnerFalconTmpClose(openInstance, isFlush, datasync >= 0); // here may fail, mark in writeFail
        if (innerRet != 0) {
            if (!isFlush) {
                ReleaseMetadataLeaseBestEffort(router->GetWorkerConnByPath(path),
                                               path,
                                               openInstance->originalSize,
                                               openInstance->nodeId,
                                               "local_close_failed");
                FalconFd::GetInstance()->DeleteOpenInstance(fd);
            }
            return innerRet;
        }
    }
    /* Flush can skip unchanged metadata updates. Release must still close metadata to release the lease. */
    if (openInstance->readFail || openInstance->writeFail || datasync > 0 ||
        (!openInstance->nodeFail && size == openInstance->originalSize)) {
        if (openInstance->readFail || openInstance->writeFail) {
            size = openInstance->originalSize;
        }
        if (isFlush) {
            if (readFail) {
                return -EIO;
            }
            return SUCCESS;
        }
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
    if (readFail) {
        return -EIO;
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
