/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "disk_cache/disk_cache.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <thread>

#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/time.h>

#include "log/logging.h"
#include "util/utils.h"

std::vector<CacheItem> DiskCache::initCacheVector;
std::mutex DiskCache::initCacheMutex;

namespace {
using DiskCacheClock = std::chrono::steady_clock;
constexpr float BACKGROUND_CLEANUP_FREE_WATERMARK_HEADROOM = 0.10F;
constexpr int ACTIVE_CLEANUP_INTERVAL_MS = 100;
constexpr int IDLE_CLEANUP_INTERVAL_MS = 1000;
constexpr int FAILED_CLEANUP_INTERVAL_MS = 3000;
constexpr int MAX_CONTINUOUS_CLEANUP_ROUNDS = 8;
constexpr std::size_t DEFAULT_EVICT_PREPARE_WORKERS = 8;
constexpr std::size_t MIN_PARALLEL_EVICT_PREPARE_ITEMS = 16;
constexpr std::size_t EVICT_FAILURE_LOG_SAMPLE_LIMIT = 3;
constexpr float DEFAULT_DISKCACHE_EVICTION_RATIO = 0.1F;
constexpr uint64_t EVICT_LOOKUP_WAIT_MS = 1000;
constexpr uint64_t SLOW_LOCK_LOG_US = 10 * 1000;

struct EvictFailureSummary {
    uint64_t metadataUnlinkFailed{0};
    uint64_t itemChanged{0};
    uint64_t removeFailed{0};
    uint64_t processLockBusy{0};
    uint64_t staleIndexCleared{0};
    std::vector<std::string> samples;
};

struct EvictRemoveTask {
    EvictedItem item;
    std::string fileName;
    int lockFd{-1};
};

struct EvictRemoveResult {
    EvictRemoveTask task;
    int ret{0};
    int err{0};
    uint64_t elapsedUs{0};
};

uint64_t ElapsedUs(DiskCacheClock::time_point start, DiskCacheClock::time_point end)
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
}

float GetFloatRatioEnv(const char *name, float defaultValue)
{
    const char *envValue = std::getenv(name);
    if (envValue == nullptr) {
        return defaultValue;
    }
    char *end = nullptr;
    float value = std::strtof(envValue, &end);
    if (end == envValue || value < 0.0F || value > 1.0F) {
        std::ostringstream oss;
        oss << "Invalid " << name << " value: " << envValue << ", use " << defaultValue;
        FALCON_LOG(LOG_WARNING) << oss.str();
        return defaultValue;
    }
    return value;
}

float GetDiskCacheEvictionRatio()
{
    static const float value = GetFloatRatioEnv("DISKCACHE_EVICTION_RATIO", DEFAULT_DISKCACHE_EVICTION_RATIO);
    return value;
}

uint64_t GetEvictTargetCandidateCount(uint64_t trackedKeys)
{
    float evictionRatio = GetDiskCacheEvictionRatio();
    uint64_t targetCount = static_cast<uint64_t>(trackedKeys * evictionRatio);
    if (evictionRatio > 0.0F && targetCount == 0 && trackedKeys > 0) {
        return 1;
    }
    return targetCount;
}

float GetBackgroundCleanupFreeWatermark(float foregroundFreeWatermark)
{
    return std::min(1.0F, foregroundFreeWatermark + BACKGROUND_CLEANUP_FREE_WATERMARK_HEADROOM);
}

void LogSlowDiskCacheLock(const char *operation, uint64_t waitUs)
{
    if (waitUs < SLOW_LOCK_LOG_US) {
        return;
    }
    std::ostringstream oss;
    oss << "DiskCache::" << operation << " waited " << waitUs << " us for mutex";
    FALCON_LOG(LOG_WARNING) << oss.str();
}

void AddEvictFailureSample(EvictFailureSummary &summary,
                           const EvictedItem &item,
                           const std::string &fileName,
                           const std::string &reason)
{
    if (summary.samples.size() >= EVICT_FAILURE_LOG_SAMPLE_LIMIT) {
        return;
    }
    std::ostringstream oss;
    oss << "{reason=" << reason << ", inode=" << item.inode << ", size=" << item.size
        << ", path=" << item.path << ", cache_path=" << fileName << "}";
    summary.samples.push_back(oss.str());
}

std::string JoinSamples(const std::vector<std::string> &samples)
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
} // namespace

DiskCache::DiskCache(float foregroundFreeWatermark)
    : foregroundFreeWatermark(foregroundFreeWatermark),
      backgroundCleanupFreeWatermark(GetBackgroundCleanupFreeWatermark(foregroundFreeWatermark))
{
}

DiskCache::~DiskCache()
{
    SetEvictListener(nullptr);
    stop = true;
    cleanupCv.notify_all();
    spaceCv.notify_all();
    evictCv.notify_all();
    if (cleanupThread.joinable()) {
        cleanupThread.join();
    }
    inodeToCacheIter.clear();
    cacheItems.clear();
}

void DiskCache::SetEvictListener(DiskCacheEvictListener *listener)
{
    std::lock_guard<std::mutex> lock(listenerMutex);
    evictListener = listener;
}

int DiskCache::AcquireProcessLock(uint64_t key, bool exclusive, bool nonBlocking)
{
    if (rootDir.empty()) {
        return -EINVAL;
    }

    std::string lockRoot = rootDir + "/.falcon_cache_locks";
    if (mkdir(lockRoot.c_str(), 0755) != 0 && errno != EEXIST) {
        return -errno;
    }
    uint64_t shard = totalDirNum > 0 ? key % static_cast<uint64_t>(totalDirNum) : 0;
    std::string lockDir = lockRoot + "/" + std::to_string(shard);
    if (mkdir(lockDir.c_str(), 0755) != 0 && errno != EEXIST) {
        return -errno;
    }

    std::string lockFile = lockDir + "/" + std::to_string(key) + ".lock";
    int fd = open(lockFile.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0666);
    if (fd < 0) {
        return -errno;
    }

    int operation = exclusive ? LOCK_EX : LOCK_SH;
    if (nonBlocking) {
        operation |= LOCK_NB;
    }
    if (flock(fd, operation) != 0) {
        int err = errno;
        close(fd);
        return -err;
    }
    return fd;
}

void DiskCache::ReleaseProcessLock(int fd)
{
    if (fd >= 0) {
        close(fd);
    }
}

void DiskCache::PrepareEvictionBatch(std::vector<EvictCandidate> &candidates, std::size_t begin, std::size_t end)
{
    if (begin >= end) {
        return;
    }

    DiskCacheEvictListener *listener = nullptr;
    {
        std::lock_guard<std::mutex> lock(listenerMutex);
        listener = evictListener;
    }
    if (listener == nullptr) {
        for (std::size_t i = begin; i < end; ++i) {
            candidates[i].prepared = true;
        }
        return;
    }

    std::vector<EvictedItem> items;
    items.reserve(end - begin);
    for (std::size_t i = begin; i < end; ++i) {
        items.push_back(candidates[i].item);
    }

    std::vector<bool> results;
    listener->OnEvictingBatch(items, results);
    if (results.size() != items.size()) {
        FALCON_LOG(LOG_ERROR) << "DiskCache::PrepareEvictionBatch(): listener returned " << results.size()
                              << " results for " << items.size() << " items";
        return;
    }

    for (std::size_t i = begin; i < end; ++i) {
        candidates[i].prepared = results[i - begin];
    }
}

void DiskCache::NotifyEvicted(const std::vector<EvictedItem> &evictedItems)
{
    DiskCacheEvictListener *listener = nullptr;
    {
        std::lock_guard<std::mutex> lock(listenerMutex);
        listener = evictListener;
    }
    if (listener == nullptr) {
        return;
    }
    for (const auto &item : evictedItems) {
        listener->OnEvicted(item);
    }
}

int DiskCache::Start(std::string &path, int dirNum, float foregroundFreeWatermark, uint64_t maxLocalDiskSizeBytes)
{
    rootDir = path;
    totalDirNum = dirNum;
    this->foregroundFreeWatermark = foregroundFreeWatermark;
    this->maxLocalDiskSizeBytes = maxLocalDiskSizeBytes;
    int ret = RETURN_OK;
    if (foregroundFreeWatermark == 0) {
        stop = true;
        return ret;
    }
    backgroundCleanupFreeWatermark = GetBackgroundCleanupFreeWatermark(foregroundFreeWatermark);
    FALCON_LOG(LOG_INFO) << "DiskCache evict watermarks: foreground free watermark = " << foregroundFreeWatermark
                         << ", background cleanup free watermark = " << backgroundCleanupFreeWatermark
                         << ", eviction ratio = " << GetDiskCacheEvictionRatio();
    ret = ScanCache();
    if (ret != RETURN_OK) {
        return ret;
    }

    ret = RefreshCurrentFreeRatios();
    if (ret != RETURN_OK) {
        FALCON_LOG(LOG_ERROR) << "Refresh current free ratios failed";
        return ret;
    }
    ret = CheckSpaceEnough();
    if (ret != RETURN_OK) {
        return ret;
    }

    cleanupThread = std::thread(&DiskCache::CheckFreeSpace, this);
    return RETURN_OK;
}

int DiskCache::ScanCache()
{
    std::vector<std::thread> initCacheThreads;

    for (int i = 0; i < totalDirNum; ++i) {
        std::string dirPath = std::string(rootDir) + "/" + std::to_string(i);

        initCacheThreads.emplace_back(Walk, dirPath);
    }
    for (auto &thread : initCacheThreads) {
        thread.join();
    }
    if (initCacheVector.empty()) {
        return RETURN_OK;
    }

    std::sort(initCacheVector.begin(), initCacheVector.end(), [](const CacheItem &first, const CacheItem &second) {
        return first.atime < second.atime;
    });
    for (CacheItem cache : initCacheVector) {
        InsertAndUpdate(cache.inode, cache.size, false);
    }
    initCacheVector.clear();
    return RETURN_OK;
}

int DiskCache::Walk(std::string dirPath)
{
    DIR *const dir = opendir(dirPath.c_str());
    if (!dir) {
        return RETURN_ERROR;
    }
    std::vector<CacheItem> cacheVector;
    for (const struct dirent *f = readdir(dir); f; f = readdir(dir)) {
        if (strcmp(f->d_name, ".") == 0 || strcmp(f->d_name, "..") == 0) {
            continue;
        }
        std::string filePath = dirPath + "/" + f->d_name;
        if (strstr(f->d_name, ".tmp.") != nullptr) {
            (void)remove(filePath.c_str());
            continue;
        }
        struct stat st;
        (void)memset(&st, 0, sizeof(st));
        stat(filePath.c_str(), &st);
        CacheItem cache;
        cache.inode = atoll(f->d_name);
        cache.atime = static_cast<uint64_t>(st.st_atime);
        cache.size = st.st_size;
        cache.refs = 0;
        cacheVector.emplace_back(cache);
    }
    if (closedir(dir)) {
        return RETURN_ERROR;
    }
    std::lock_guard<std::mutex> lk(initCacheMutex);
    initCacheVector.insert(initCacheVector.end(), cacheVector.begin(), cacheVector.end());
    return RETURN_OK;
}

int DiskCache::RefreshCurrentFreeRatios()
{
    struct statfs diskInfo;
    (void)memset(&diskInfo, 0, sizeof(diskInfo));
    int32_t ret = statfs(rootDir.c_str(), &diskInfo);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Get disk(" << rootDir << ") stat ret " << ret << ": " << strerror(errno);
        return RETURN_ERROR;
    }

    uint64_t fsTotalCap = static_cast<uint64_t>(diskInfo.f_bsize) * static_cast<uint64_t>(diskInfo.f_blocks);
    uint64_t fsFreeCap = static_cast<uint64_t>(diskInfo.f_bsize) * static_cast<uint64_t>(diskInfo.f_bavail);
    totalCap = fsTotalCap;
    uint64_t capacity = fsFreeCap;
    if (maxLocalDiskSizeBytes > 0) {
        totalCap = std::min(maxLocalDiskSizeBytes, fsTotalCap);
        uint64_t logicalFreeCap = usedCap >= totalCap ? 0 : totalCap - usedCap;
        capacity = std::min(fsFreeCap, logicalFreeCap);
    }
    freeCap.store(capacity);
    blockFreeRatio = capacity * 1.0 / totalCap;
    totalInodes = static_cast<uint64_t>(diskInfo.f_files);
    freeInodes = static_cast<uint64_t>(diskInfo.f_ffree);
    inodeFreeRatio = freeInodes * 1.0 / totalInodes;

    return RETURN_OK;
}

void DiskCache::CheckFreeSpace()
{
    bool activeCleanup = false;
    bool cleanupBackoff = false;
    while (!stop) {
        {
            std::unique_lock<std::mutex> waitLock(cleanupNotifyMutex);
            int waitMs = IDLE_CLEANUP_INTERVAL_MS;
            if (activeCleanup) {
                waitMs = cleanupBackoff ? FAILED_CLEANUP_INTERVAL_MS : ACTIVE_CLEANUP_INTERVAL_MS;
            }
            auto waitTime = std::chrono::milliseconds(waitMs);
            cleanupCv.wait_for(waitLock, waitTime, [this]() {
                return stop.load() || cleanupRequested.load();
            });
        }
        if (stop) {
            break;
        }

        bool shouldCleanup = false;
        bool didCleanup = false;
        uint64_t requestedBytes = 0;
        {
            std::unique_lock<std::mutex> lock(mutex);
            int ret = RefreshCurrentFreeRatios();
            if (ret != RETURN_OK) {
                break;
            }
            requestedBytes = requestedCleanupBytes.exchange(0);
            bool requested = cleanupRequested.exchange(false);
            shouldCleanup = requested || blockFreeRatio < backgroundCleanupFreeWatermark ||
                            inodeFreeRatio < backgroundCleanupFreeWatermark;
            if (shouldCleanup && usedCap > 0) {
                hasFreeSpace = false;
                if (requestedBytes > 0) {
                    FALCON_LOG(LOG_INFO) << "DiskCache::CheckFreeSpace(): background cleanup requested, bytes = "
                                         << requestedBytes;
                }
                for (int round = 0; round < MAX_CONTINUOUS_CLEANUP_ROUNDS && usedCap > 0; ++round) {
                    bool freed = Cleanup(requestedBytes, lock);
                    didCleanup = didCleanup || freed;
                    requestedBytes = 0;
                    if (!freed || (blockFreeRatio >= backgroundCleanupFreeWatermark &&
                                   inodeFreeRatio >= backgroundCleanupFreeWatermark)) {
                        break;
                    }
                }
            }
            hasFreeSpace = blockFreeRatio >= backgroundCleanupFreeWatermark &&
                           inodeFreeRatio >= backgroundCleanupFreeWatermark;
            bool cleanupFailedWithoutProgress = shouldCleanup && !didCleanup && !hasFreeSpace.load() && usedCap > 0;
            if (cleanupFailedWithoutProgress && !cleanupBackoff) {
                FALCON_LOG(LOG_WARNING) << "DiskCache::CheckFreeSpace(): cleanup made no progress, backoff to "
                                        << FAILED_CLEANUP_INTERVAL_MS << " ms, blockFreeRatio = " << blockFreeRatio
                                        << ", inodeFreeRatio = " << inodeFreeRatio << ", usedCap = " << usedCap;
            }
            cleanupBackoff = cleanupFailedWithoutProgress;
            activeCleanup = !hasFreeSpace.load() && usedCap > 0;
        }
        if (shouldCleanup || didCleanup) {
            spaceCv.notify_all();
        }
    }
}


void DiskCache::CollectEvictCandidates(uint64_t toFreeCap,
                                       uint64_t toFreeInode,
                                       uint64_t targetCandidateCount,
                                       std::vector<EvictCandidate> &candidates,
                                       uint64_t &scannedInode)
{
    if (toFreeCap == 0 && toFreeInode == 0 && targetCandidateCount == 0) {
        return;
    }

    uint64_t selectedCap = 0;
    uint64_t selectedInode = 0;
    for (auto it = cacheItems.begin(); it != cacheItems.end(); ++it) {
        if (it->refs > 0 || it->evicting) {
            continue;
        }
        ++scannedInode;
        it->evicting = true;
        EvictCandidate candidate;
        candidate.item = {it->inode, it->size, it->path};
        candidate.fileName = GetFilePath(it->inode);
        candidates.push_back(candidate);
        selectedCap += it->size;
        ++selectedInode;

        bool reachedSizeTarget = toFreeCap > 0 && selectedCap >= toFreeCap;
        bool reachedInodeTarget = toFreeInode > 0 && selectedInode >= toFreeInode;
        bool reachedRatioTarget = targetCandidateCount > 0 && candidates.size() >= targetCandidateCount;
        if (reachedSizeTarget || reachedInodeTarget || reachedRatioTarget) {
            break;
        }
    }
}

void DiskCache::PrepareEvictions(std::vector<EvictCandidate> &candidates)
{
    if (candidates.empty()) {
        return;
    }

    std::size_t workerCount = std::min(DEFAULT_EVICT_PREPARE_WORKERS, candidates.size());
    if (workerCount <= 1 || candidates.size() < MIN_PARALLEL_EVICT_PREPARE_ITEMS) {
        PrepareEvictionBatch(candidates, 0, candidates.size());
        return;
    }

    std::vector<std::thread> workers;
    workers.reserve(workerCount);
    for (std::size_t worker = 0; worker < workerCount; ++worker) {
        std::size_t begin = candidates.size() * worker / workerCount;
        std::size_t end = candidates.size() * (worker + 1) / workerCount;
        workers.emplace_back([this, &candidates, begin, end]() {
            PrepareEvictionBatch(candidates, begin, end);
        });
    }
    for (auto &worker : workers) {
        worker.join();
    }
}

void DiskCache::FinishPreparedEvictions(std::vector<EvictCandidate> &candidates,
                                        std::vector<EvictedItem> &evictedItems,
                                        std::unique_lock<std::mutex> &lock,
                                        uint64_t &freedCap,
                                        uint64_t &freedInode,
                                        uint64_t &failedInode,
                                        uint64_t &removeElapsedUs,
                                        uint64_t &finishLockWaitUs,
                                        uint64_t &finishLockHeldUs)
{
    EvictFailureSummary failureSummary;
    std::vector<EvictRemoveTask> removeTasks;

    auto lockHeldStart = DiskCacheClock::now();
    for (auto &candidate : candidates) {
        auto mapIt = inodeToCacheIter.find(candidate.item.inode);
        if (mapIt == inodeToCacheIter.end()) {
            continue;
        }

        auto itemIt = mapIt->second;
        if (!itemIt->evicting) {
            continue;
        }

        if (!candidate.prepared) {
            itemIt->evicting = false;
            ++failedInode;
            ++failureSummary.metadataUnlinkFailed;
            AddEvictFailureSample(failureSummary, candidate.item, candidate.fileName, "metadata_unlink_failed");
            continue;
        }

        if (itemIt->refs > 0 || itemIt->size != candidate.item.size || itemIt->path != candidate.item.path) {
            itemIt->evicting = false;
            ++failedInode;
            ++failureSummary.itemChanged;
            AddEvictFailureSample(failureSummary, candidate.item, candidate.fileName, "cache_item_changed");
            continue;
        }

        int lockFd = AcquireProcessLock(candidate.item.inode, true, true);
        if (lockFd < 0) {
            itemIt->evicting = false;
            ++failedInode;
            ++failureSummary.processLockBusy;
            AddEvictFailureSample(failureSummary,
                                  candidate.item,
                                  candidate.fileName,
                                  std::string("process_lock_busy:") + strerror(-lockFd));
            continue;
        }

        removeTasks.push_back({candidate.item, candidate.fileName, lockFd});
    }
    finishLockHeldUs += ElapsedUs(lockHeldStart, DiskCacheClock::now());

    lock.unlock();
    std::vector<EvictRemoveResult> removeResults;
    removeResults.reserve(removeTasks.size());
    for (const auto &task : removeTasks) {
        FALCON_LOG(LOG_DEBUG) << "DiskCache::FinishPreparedEvictions(): remove begin, tid="
                             << std::this_thread::get_id() << ", inode=" << task.item.inode
                             << ", size=" << task.item.size << ", path=" << task.item.path
                             << ", cache_path=" << task.fileName;
        auto removeStart = DiskCacheClock::now();
        int ret = remove(task.fileName.c_str());
        uint64_t elapsedUs = ElapsedUs(removeStart, DiskCacheClock::now());
        int err = ret == 0 ? 0 : errno;
        if (ret == 0 || err == ENOENT) {
            FALCON_LOG(LOG_DEBUG) << "DiskCache::FinishPreparedEvictions(): remove done, tid="
                                 << std::this_thread::get_id() << ", ret=" << ret
                                 << ", err=" << (ret == 0 ? "OK" : strerror(err))
                                 << ", elapsed_us=" << elapsedUs << ", inode=" << task.item.inode
                                 << ", size=" << task.item.size << ", path=" << task.item.path
                                 << ", cache_path=" << task.fileName;
        } else {
            FALCON_LOG(LOG_ERROR) << "DiskCache::FinishPreparedEvictions(): remove done, tid="
                                  << std::this_thread::get_id() << ", ret=" << ret
                                  << ", err=" << strerror(err) << ", elapsed_us=" << elapsedUs
                                  << ", inode=" << task.item.inode << ", size=" << task.item.size
                                  << ", path=" << task.item.path << ", cache_path=" << task.fileName;
        }
        removeElapsedUs += elapsedUs;
        ReleaseProcessLock(task.lockFd);
        removeResults.push_back({task, ret, err, elapsedUs});
    }
    auto removeRelockWaitStart = DiskCacheClock::now();
    lock.lock();
    finishLockWaitUs += ElapsedUs(removeRelockWaitStart, DiskCacheClock::now());

    lockHeldStart = DiskCacheClock::now();
    for (const auto &result : removeResults) {
        auto mapIt = inodeToCacheIter.find(result.task.item.inode);
        if (mapIt == inodeToCacheIter.end()) {
            continue;
        }

        auto itemIt = mapIt->second;
        if (!itemIt->evicting || itemIt->size != result.task.item.size || itemIt->path != result.task.item.path) {
            ++failedInode;
            ++failureSummary.itemChanged;
            AddEvictFailureSample(failureSummary, result.task.item, result.task.fileName, "cache_item_changed_after_remove");
            continue;
        }

        if (result.ret == 0 || result.err == ENOENT) {
            freedCap += result.task.item.size;
            ++freedInode;
            cacheItems.erase(itemIt);
            inodeToCacheIter.erase(mapIt);
            usedCap -= result.task.item.size;
            freeCap += result.task.item.size;
            evictedItems.push_back(result.task.item);
            if (result.err == ENOENT) {
                ++failureSummary.staleIndexCleared;
                AddEvictFailureSample(failureSummary, result.task.item, result.task.fileName, "stale_index_cleared");
            }
            continue;
        }

        itemIt->evicting = false;
        ++failedInode;
        ++failureSummary.removeFailed;
        AddEvictFailureSample(failureSummary, result.task.item, result.task.fileName, strerror(result.err));
    }
    finishLockHeldUs += ElapsedUs(lockHeldStart, DiskCacheClock::now());

    evictCv.notify_all();

    if (failureSummary.metadataUnlinkFailed > 0 || failureSummary.itemChanged > 0 || failureSummary.removeFailed > 0 || failureSummary.processLockBusy > 0 ||
        failureSummary.staleIndexCleared > 0) {
        FALCON_LOG(LOG_WARNING) << "DiskCache::FinishPreparedEvictions(): metadata_unlink_failed = "
                                << failureSummary.metadataUnlinkFailed
                                << ", cache_item_changed = " << failureSummary.itemChanged
                                << ", remove_failed = " << failureSummary.removeFailed
                                << ", process_lock_busy = " << failureSummary.processLockBusy
                                << ", stale_index_cleared = " << failureSummary.staleIndexCleared
                                << ", samples = " << JoinSamples(failureSummary.samples);
    }
}

void DiskCache::CleanupForEvict(uint64_t preAllocSize,
                                std::vector<EvictedItem> &evictedItems,
                                std::unique_lock<std::mutex> &lock)
{
    auto cleanupStart = DiskCacheClock::now();
    uint64_t toFreeCap = 0;
    uint64_t toFreeInode = 0;
    float postReserveBlockFreeRatio = blockFreeRatio - (preAllocSize + reservedCap) * 1.0 / totalCap;
    if (postReserveBlockFreeRatio < foregroundFreeWatermark) {
        toFreeCap = (uint64_t)(totalCap * (foregroundFreeWatermark - postReserveBlockFreeRatio));
        FALCON_LOG(LOG_INFO) << "DiskCache::CleanupForEvict(): Evict file due to block limit, data toFreeCap = "
                             << toFreeCap;
        if (toFreeCap > usedCap) {
            toFreeCap = usedCap;
        }
    }

    if (inodeFreeRatio < foregroundFreeWatermark) {
        toFreeInode = (uint64_t)(totalInodes * (foregroundFreeWatermark - inodeFreeRatio));
        FALCON_LOG(LOG_INFO) << "DiskCache::CleanupForEvict(): Evict file due to inode limit, inodes toFreeInode = "
                             << toFreeInode;
        if (toFreeInode > inodeToCacheIter.size()) {
            toFreeInode = inodeToCacheIter.size();
        }
    }

    uint64_t freedCap = 0;
    uint64_t freedInode = 0;
    uint64_t failedInode = 0;
    uint64_t scannedInode = 0;
    uint64_t removeElapsedUs = 0;
    uint64_t finishLockWaitUs = 0;
    uint64_t finishLockHeldUs = 0;

    std::vector<EvictCandidate> candidates;
    CollectEvictCandidates(toFreeCap, toFreeInode, 0, candidates, scannedInode);
    lock.unlock();
    PrepareEvictions(candidates);
    auto finishLockWaitStart = DiskCacheClock::now();
    lock.lock();
    finishLockWaitUs = ElapsedUs(finishLockWaitStart, DiskCacheClock::now());
    FinishPreparedEvictions(candidates,
                            evictedItems,
                            lock,
                            freedCap,
                            freedInode,
                            failedInode,
                            removeElapsedUs,
                            finishLockWaitUs,
                            finishLockHeldUs);

    freeInodes += freedInode;
    RefreshFreeRatiosFromAccounting();
    if (failedInode > 0) {
        FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evicted " << freedInode << " files, all size is "
                                << freedCap << ", failed files = " << failedInode << ", scanned files = "
                                << scannedInode << ", cleanup elapsed us = "
                                << ElapsedUs(cleanupStart, DiskCacheClock::now()) << ", remove elapsed us = "
                                << removeElapsedUs << ", finish lock wait us = " << finishLockWaitUs
                                << ", finish lock held us = " << finishLockHeldUs
                                << ", blockFreeRatio = " << blockFreeRatio
                                << ", inodeFreeRatio = " << inodeFreeRatio;
    } else {
        FALCON_LOG(LOG_INFO) << "DiskCache::CleanupForEvict(): Evicted " << freedInode << " files, all size is "
                             << freedCap << ", failed files = " << failedInode << ", scanned files = "
                             << scannedInode << ", cleanup elapsed us = "
                             << ElapsedUs(cleanupStart, DiskCacheClock::now()) << ", remove elapsed us = "
                             << removeElapsedUs << ", finish lock wait us = " << finishLockWaitUs
                             << ", finish lock held us = " << finishLockHeldUs
                             << ", blockFreeRatio = " << blockFreeRatio
                             << ", inodeFreeRatio = " << inodeFreeRatio;
    }
}

bool DiskCache::Cleanup(uint64_t requestedBytes, std::unique_lock<std::mutex> &lock)
{
    auto cleanupStart = DiskCacheClock::now();
    std::vector<EvictedItem> evictedItems;
    uint64_t toFreeCap = 0;
    uint64_t toFreeInode = 0;
    uint64_t targetCandidateCount = GetEvictTargetCandidateCount(inodeToCacheIter.size());

    if (blockFreeRatio < backgroundCleanupFreeWatermark) {
        FALCON_LOG(LOG_INFO)
            << "DiskCache::Cleanup(): block free ratio below background free watermark, eviction_ratio = "
            << GetDiskCacheEvictionRatio() << ", target files = " << targetCandidateCount
            << ", blockFreeRatio = " << blockFreeRatio
            << ", backgroundFreeWatermark = " << backgroundCleanupFreeWatermark;
    } else if (requestedBytes > 0) {
        toFreeCap = std::min<uint64_t>(requestedBytes + reservedCap.load(), usedCap);
        FALCON_LOG(LOG_INFO) << "DiskCache::Cleanup(): explicit cleanup requested, data toFreeCap = "
                             << toFreeCap << ", eviction_ratio = " << GetDiskCacheEvictionRatio()
                             << ", target files = " << targetCandidateCount;
    }

    if (inodeFreeRatio < backgroundCleanupFreeWatermark) {
        FALCON_LOG(LOG_INFO)
            << "DiskCache::Cleanup(): inode free ratio below background free watermark, eviction_ratio = "
            << GetDiskCacheEvictionRatio() << ", target files = " << targetCandidateCount
            << ", inodeFreeRatio = " << inodeFreeRatio
            << ", backgroundFreeWatermark = " << backgroundCleanupFreeWatermark;
    }

    uint64_t freedCap = 0;
    uint64_t freedInode = 0;
    uint64_t failedInode = 0;
    uint64_t scannedInode = 0;
    uint64_t removeElapsedUs = 0;
    uint64_t finishLockWaitUs = 0;
    uint64_t finishLockHeldUs = 0;

    std::vector<EvictCandidate> candidates;
    CollectEvictCandidates(toFreeCap, toFreeInode, targetCandidateCount, candidates, scannedInode);
    lock.unlock();
    PrepareEvictions(candidates);
    auto finishLockWaitStart = DiskCacheClock::now();
    lock.lock();
    finishLockWaitUs = ElapsedUs(finishLockWaitStart, DiskCacheClock::now());
    FinishPreparedEvictions(candidates,
                            evictedItems,
                            lock,
                            freedCap,
                            freedInode,
                            failedInode,
                            removeElapsedUs,
                            finishLockWaitUs,
                            finishLockHeldUs);

    freeInodes += freedInode;
    RefreshFreeRatiosFromAccounting();
    if (failedInode > 0) {
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evicted " << freedInode << " files, all size is "
                                << freedCap << ", failed files = " << failedInode << ", scanned files = "
                                << scannedInode << ", target files = " << targetCandidateCount
                                << ", eviction_ratio = " << GetDiskCacheEvictionRatio() << ", cleanup elapsed us = "
                                << ElapsedUs(cleanupStart, DiskCacheClock::now()) << ", remove elapsed us = "
                                << removeElapsedUs << ", finish lock wait us = " << finishLockWaitUs
                                << ", finish lock held us = " << finishLockHeldUs
                                << ", blockFreeRatio = " << blockFreeRatio
                                << ", inodeFreeRatio = " << inodeFreeRatio;
    } else {
        FALCON_LOG(LOG_INFO) << "DiskCache::Cleanup(): Evicted " << freedInode << " files, all size is " << freedCap
                             << ", failed files = " << failedInode << ", scanned files = " << scannedInode
                             << ", target files = " << targetCandidateCount
                             << ", eviction_ratio = " << GetDiskCacheEvictionRatio() << ", cleanup elapsed us = "
                             << ElapsedUs(cleanupStart, DiskCacheClock::now()) << ", remove elapsed us = "
                             << removeElapsedUs << ", finish lock wait us = " << finishLockWaitUs
                             << ", finish lock held us = " << finishLockHeldUs
                             << ", blockFreeRatio = " << blockFreeRatio
                             << ", inodeFreeRatio = " << inodeFreeRatio;
    }
    auto notifyStart = DiskCacheClock::now();
    NotifyEvicted(evictedItems);
    if (!evictedItems.empty()) {
        FALCON_LOG(LOG_INFO) << "DiskCache::Cleanup(): NotifyEvicted " << evictedItems.size()
                             << " items, elapsed us = " << ElapsedUs(notifyStart, DiskCacheClock::now());
    }
    return freedInode > 0;
}

void DiskCache::RefreshFreeRatiosFromAccounting()
{
    if (totalCap > 0) {
        if (maxLocalDiskSizeBytes > 0) {
            uint64_t logicalFreeCap = usedCap >= totalCap ? 0 : totalCap - usedCap;
            uint64_t currentFreeCap = freeCap.load();
            if (currentFreeCap > logicalFreeCap) {
                freeCap.store(logicalFreeCap);
            }
        }
        blockFreeRatio = freeCap.load() * 1.0F / totalCap;
    }
    if (totalInodes > 0) {
        inodeFreeRatio = freeInodes * 1.0F / totalInodes;
    }
}

int DiskCache::Delete(uint64_t key)
{
    if (stop) {
        std::string fileName = GetFilePath(key);
        int ret = remove(fileName.c_str());
        return ret;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        int ret = 0;
        auto elem = inodeToCacheIter[key];
        uint64_t size = elem->size;
        std::string fileName = GetFilePath(key);
        ret = remove(fileName.c_str());
        if (ret != 0) {
            int err = errno;
            FALCON_LOG(LOG_ERROR) << "Delete file: " << fileName << " failed: " << strerror(err);
            return -err;
        }
        cacheItems.erase(elem);
        inodeToCacheIter.erase(key);
        usedCap -= size;
        freeCap += size;
        RefreshFreeRatiosFromAccounting();
        FALCON_LOG(LOG_INFO) << "Delete file: " << fileName;
    }
    return 0;
}

void DiskCache::Pin(uint64_t key)
{
    if (stop) {
        return;
    }
    inodeToCacheIter[key]->refs += 1;
    inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
}

void DiskCache::Unpin(uint64_t key)
{
    if (stop) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end() && inodeToCacheIter[key]->refs > 0) {
        inodeToCacheIter[key]->refs -= 1;
    }
}


bool DiskCache::Find(uint64_t key, bool needPin)
{
    return FindWithWait(key, needPin, 0) == DiskCacheFindResult::HIT;
}

DiskCacheFindResult DiskCache::FindWithWait(uint64_t key, bool needPin)
{
    return FindWithWait(key, needPin, EVICT_LOOKUP_WAIT_MS);
}

DiskCacheFindResult DiskCache::FindWithWait(uint64_t key, bool needPin, uint64_t waitMs)
{
    if (stop) {
        std::string fileName = GetFilePath(key);
        return access(fileName.c_str(), F_OK) == 0 ? DiskCacheFindResult::HIT : DiskCacheFindResult::MISS;
    }

    auto lockWaitStart = DiskCacheClock::now();
    std::unique_lock<std::mutex> lock(mutex);
    LogSlowDiskCacheLock("Find", ElapsedUs(lockWaitStart, DiskCacheClock::now()));

    auto deadline = DiskCacheClock::now() + std::chrono::milliseconds(waitMs);
    while (true) {
        auto item = inodeToCacheIter.find(key);
        if (item == inodeToCacheIter.end()) {
            return DiskCacheFindResult::MISS;
        }
        if (!item->second->evicting) {
            if (needPin) {
                item->second->refs += 1;
                item->second->atime = static_cast<uint64_t>(time(nullptr));
            }
            return DiskCacheFindResult::HIT;
        }
        if (waitMs == 0) {
            return DiskCacheFindResult::EVICTING;
        }
        if (evictCv.wait_until(lock, deadline) == std::cv_status::timeout) {
            return DiskCacheFindResult::EVICTING;
        }
    }
}

void DiskCache::DeleteOldCacheWithNoPin(uint64_t key)
{
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        if (inodeToCacheIter[key]->refs <= 0) {
            int ret = 0;
            auto elem = inodeToCacheIter[key];
            uint64_t size = elem->size;
            std::string fileName = GetFilePath(key);
            int lockFd = AcquireProcessLock(key, true, true);
            if (lockFd < 0 && lockFd != -EINVAL) {
                FALCON_LOG(LOG_WARNING) << "DeleteOldCacheWithNoPin file: " << fileName
                                        << " skipped, process lock busy: " << strerror(-lockFd);
                return;
            }
            ret = remove(fileName.c_str());
            ReleaseProcessLock(lockFd);
            if (ret != 0) {
                int err = errno;
                FALCON_LOG(LOG_ERROR) << "DeleteOldCacheWithNoPin file: " << fileName << " failed: " << strerror(err);
                return;
            }
            cacheItems.erase(elem);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
            RefreshFreeRatiosFromAccounting();
        }
    }
}


void DiskCache::InsertAndUpdate(uint64_t key, uint64_t size, bool needPin, const std::string &path)
{
    if (stop) {
        return;
    }
    auto lockWaitStart = DiskCacheClock::now();
    std::unique_lock<std::mutex> lock(mutex);
    LogSlowDiskCacheLock("InsertAndUpdate", ElapsedUs(lockWaitStart, DiskCacheClock::now()));
    auto item = inodeToCacheIter.find(key);
    if (item != inodeToCacheIter.end()) {
        if (item->second->evicting) {
            return;
        }
        // update
        usedCap += static_cast<int64_t>(size - item->second->size);
        freeCap -= static_cast<int64_t>(size - item->second->size);
        item->second->atime = static_cast<uint64_t>(time(nullptr));
        item->second->size = size;
        item->second->evicting = false;
        if (!path.empty()) {
            item->second->path = path;
        }
        RefreshFreeRatiosFromAccounting();
        return;
    }

    // insert
    CacheItem elem;
    elem.atime = static_cast<uint64_t>(time(nullptr));
    elem.size = size;
    elem.inode = key;
    elem.path = path;
    cacheItems.emplace_back(elem);
    inodeToCacheIter[key] = prev(cacheItems.end());
    usedCap += size;
    freeCap -= size;
    RefreshFreeRatiosFromAccounting();
    if (needPin) {
        auto inserted = inodeToCacheIter.find(key);
        if (inserted != inodeToCacheIter.end()) {
            inserted->second->refs += 1;
            inserted->second->atime = static_cast<uint64_t>(time(nullptr));
        }
    }
}


bool DiskCache::Update(uint64_t key, uint64_t size)
{
    if (stop) {
        return true;
    }
    auto lockWaitStart = DiskCacheClock::now();
    std::unique_lock<std::mutex> lock(mutex);
    LogSlowDiskCacheLock("Update", ElapsedUs(lockWaitStart, DiskCacheClock::now()));
    auto item = inodeToCacheIter.find(key);
    if (item != inodeToCacheIter.end()) {
        // update
        if (item->second->evicting) {
            return false;
        }
        if (size <= item->second->size) {
            return true;
        }
        usedCap += static_cast<int64_t>(size - item->second->size);
        freeCap -= static_cast<int64_t>(size - item->second->size);
        item->second->atime = static_cast<uint64_t>(time(nullptr));
        item->second->size = size;
        item->second->evicting = false;
        RefreshFreeRatiosFromAccounting();
    } else {
        FALCON_LOG(LOG_ERROR) << "In DiskCache::Add(), inode " << key << " not found";
        return false;
    }
    return true;
}


bool DiskCache::Add(uint64_t key, uint64_t size)
{
    if (stop) {
        return true;
    }
    auto lockWaitStart = DiskCacheClock::now();
    std::unique_lock<std::mutex> lock(mutex);
    LogSlowDiskCacheLock("Add", ElapsedUs(lockWaitStart, DiskCacheClock::now()));
    auto item = inodeToCacheIter.find(key);
    if (item != inodeToCacheIter.end()) {
        // update
        if (item->second->evicting) {
            return false;
        }
        usedCap += static_cast<int64_t>(size);
        freeCap -= static_cast<int64_t>(size);
        item->second->atime = static_cast<uint64_t>(time(nullptr));
        item->second->size += size;
        item->second->evicting = false;
        RefreshFreeRatiosFromAccounting();
    } else {
        FALCON_LOG(LOG_ERROR) << "In DiskCache::Add(), inode " << key << " not found";
        return false;
    }
    return true;
}

void DiskCache::Evict(uint64_t size)
{
    std::vector<EvictedItem> evictedItems;
    {
        std::unique_lock<std::mutex> lock(mutex);
        RefreshCurrentFreeRatios();
        CleanupForEvict(size, evictedItems, lock);
    }
    auto notifyStart = DiskCacheClock::now();
    NotifyEvicted(evictedItems);
    if (!evictedItems.empty()) {
        FALCON_LOG(LOG_INFO) << "DiskCache::Evict(): NotifyEvicted " << evictedItems.size()
                             << " items, elapsed us = " << ElapsedUs(notifyStart, DiskCacheClock::now());
    }
}

bool DiskCache::CanReserve(uint64_t size) const
{
    uint64_t currentFreeCap = freeCap.load();
    uint64_t currentReservedCap = reservedCap.load();
    if (size > UINT64_MAX - currentReservedCap || currentReservedCap + size >= currentFreeCap) {
        return false;
    }
    if (totalCap == 0) {
        return true;
    }
    uint64_t freeCapAfterReserve = currentFreeCap - currentReservedCap - size;
    return freeCapAfterReserve * 1.0F / totalCap >= foregroundFreeWatermark;
}

void DiskCache::Reserve(uint64_t size)
{
    reservedCap += size;
}

void DiskCache::RequestBackgroundCleanup(uint64_t size)
{
    cleanupRequested = true;
    requestedCleanupBytes.fetch_add(size);
    cleanupCv.notify_one();
}

bool DiskCache::PreAllocSpace(uint64_t size)
{
    if (stop) {
        return true;
    }
    std::unique_lock<std::mutex> lock(allocMutex);
    //
    if (CanReserve(size)) {
        Reserve(size);
        return true;
    }

    hasFreeSpace = false;
    RequestBackgroundCleanup(size);
    for (int i = 0; i < 3; ++i) {
        spaceCv.wait_for(lock, std::chrono::milliseconds(100));
        if (CanReserve(size)) {
            hasFreeSpace = true;
            Reserve(size);
            return true;
        }
    }

    int retryCnt = 3;
    do {
        if (retryCnt == 0) {
            FALCON_LOG(LOG_WARNING) << "PreAllocSpace failed, size = " << size << " ,reservedCap = " << reservedCap
                                    << " ,freeCap = " << freeCap.load();
            return false;
        }

        {
            lock.unlock();
            std::unique_lock<std::mutex> evictLock(foregroundEvictMutex);
            lock.lock();
            if (!CanReserve(size)) {
                lock.unlock();
                Evict(size);
                lock.lock();
            }
        }
        --retryCnt;
        if (CanReserve(size)) {
            break;
        }
        RequestBackgroundCleanup(size);
        spaceCv.wait_for(lock, std::chrono::milliseconds(100));
    } while (!CanReserve(size) && retryCnt >= 0);
    hasFreeSpace = true;
    Reserve(size);
    return true;
}

void DiskCache::FreePreAllocSpace(uint64_t size)
{
    if (stop) {
        return;
    }
    std::lock_guard<std::mutex> lock(allocMutex);
    reservedCap -= size;
    spaceCv.notify_all();
}

bool DiskCache::HasFreeSpace() { return hasFreeSpace.load(); }

int DiskCache::CheckSpaceEnough()
{
    float blockFreeRatio = (freeCap + usedCap) * 1.0 / totalCap;
    float inodeFreeRatio = (freeInodes + inodeToCacheIter.size()) * 1.0 / totalInodes;
    if (blockFreeRatio <= backgroundCleanupFreeWatermark || inodeFreeRatio <= backgroundCleanupFreeWatermark) {
        FALCON_LOG(LOG_ERROR) << "The free space can not support FalconFS running";
        FALCON_LOG(LOG_ERROR) << "Free space is not enough";
        return RETURN_ERROR;
    }
    return RETURN_OK;
}
