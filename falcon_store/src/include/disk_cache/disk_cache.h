/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <dirent.h>

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifndef RETURN_OK
#define RETURN_OK 0
#endif

#ifndef RETURN_ERROR
#define RETURN_ERROR (-1)
#endif

struct CacheItem
{
    uint64_t inode{0};
    uint64_t size{0};
    uint64_t atime{0};
    uint32_t refs{0};
    bool evicting{false};
    std::string path;
};

struct EvictedItem
{
    uint64_t inode{0};
    uint64_t size{0};
    std::string path;
};

enum class DiskCacheFindResult {
    HIT,
    MISS,
    EVICTING,
};

class DiskCacheEvictListener {
  public:
    virtual ~DiskCacheEvictListener() = default;
    virtual void OnEvictingBatch(const std::vector<EvictedItem> &items, std::vector<bool> &results)
    {
        results.assign(items.size(), true);
    }
    virtual void OnEvicted(const EvictedItem &item) = 0;
};

class DiskCache {
  public:
    static DiskCache &GetInstance()
    {
        static DiskCache instance;
        return instance;
    }
    DiskCache() = default;
    DiskCache(float foregroundFreeWatermark);
    ~DiskCache();
    int Start(std::string &path, int dirNum, float foregroundFreeWatermark, uint64_t maxLocalDiskSizeBytes = 0);
    bool Find(uint64_t key, bool needPin);
    DiskCacheFindResult FindWithWait(uint64_t key, bool needPin);
    DiskCacheFindResult FindWithWait(uint64_t key, bool needPin, uint64_t waitMs);
    void DeleteOldCacheWithNoPin(uint64_t key);
    void InsertAndUpdate(uint64_t key, uint64_t size, bool needPin, const std::string &path = "");
    bool Add(uint64_t key, uint64_t size);
    bool Update(uint64_t key, uint64_t size);
    int Delete(uint64_t key);
    void Evict(uint64_t size);
    void Unpin(uint64_t key);
    void Pin(uint64_t key);
    bool PreAllocSpace(uint64_t size);
    void FreePreAllocSpace(uint64_t size);
    bool HasFreeSpace();
    void SetEvictListener(DiskCacheEvictListener *listener);
    int AcquireProcessLock(uint64_t key, bool exclusive, bool nonBlocking);
    static void ReleaseProcessLock(int fd);

  private:
    uint64_t totalCap{0};
    uint64_t maxLocalDiskSizeBytes{0};
    std::atomic<uint64_t> freeCap{0};
    float blockFreeRatio{0.0};
    uint64_t totalInodes{0};
    uint64_t freeInodes{0};
    float inodeFreeRatio{0.0};
    float foregroundFreeWatermark{0.1};
    float backgroundCleanupFreeWatermark{0.2};

    bool testOBS = false;

    uint64_t usedCap{0};

    std::string rootDir;
    std::list<CacheItem> cacheItems;
    using cacheIterator = std::list<CacheItem>::iterator;
    std::unordered_map<uint64_t, cacheIterator> inodeToCacheIter;
    std::mutex mutex;

    std::thread cleanupThread;
    std::atomic<bool> stop{false};
    std::atomic<bool> hasFreeSpace{true};

    int totalDirNum{101};

    std::atomic<uint64_t> reservedCap{0};
    std::mutex allocMutex;
    std::mutex foregroundEvictMutex;
    std::mutex cleanupNotifyMutex;
    std::condition_variable cleanupCv;
    std::condition_variable spaceCv;
    std::condition_variable evictCv;
    std::atomic<bool> cleanupRequested{false};
    std::atomic<uint64_t> requestedCleanupBytes{0};
    std::mutex listenerMutex;
    DiskCacheEvictListener *evictListener{nullptr};

    static std::mutex initCacheMutex;

    static std::vector<CacheItem> initCacheVector;
    int RefreshCurrentFreeRatios();
    void CheckFreeSpace();
    struct EvictCandidate {
        EvictedItem item;
        std::string fileName;
        bool prepared{false};
    };

    bool Cleanup(uint64_t requestedBytes, std::unique_lock<std::mutex> &lock);
    void CleanupForEvict(uint64_t size, std::vector<EvictedItem> &evictedItems, std::unique_lock<std::mutex> &lock);
    void CollectEvictCandidates(uint64_t toFreeCap,
                                uint64_t toFreeInode,
                                uint64_t targetCandidateCount,
                                std::vector<EvictCandidate> &candidates,
                                uint64_t &scannedInode);
    void PrepareEvictions(std::vector<EvictCandidate> &candidates);
    void FinishPreparedEvictions(std::vector<EvictCandidate> &candidates,
                                 std::vector<EvictedItem> &evictedItems,
                                 std::unique_lock<std::mutex> &lock,
                                 uint64_t &freedCap,
                                 uint64_t &freedInode,
                                 uint64_t &failedInode,
                                 uint64_t &removeElapsedUs,
                                 uint64_t &finishLockWaitUs,
                                 uint64_t &finishLockHeldUs);
    void RefreshFreeRatiosFromAccounting();
    void PrepareEvictionBatch(std::vector<EvictCandidate> &candidates, std::size_t begin, std::size_t end);
    void NotifyEvicted(const std::vector<EvictedItem> &evictedItems);
    bool CanReserve(uint64_t size) const;
    void Reserve(uint64_t size);
    void RequestBackgroundCleanup(uint64_t size);
    int ScanCache();
    static int Walk(std::string dirPath);
    int CheckSpaceEnough();
};
