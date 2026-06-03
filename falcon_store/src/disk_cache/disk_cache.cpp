/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "disk_cache/disk_cache.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>

#include <sys/statfs.h>
#include <sys/time.h>

#include "log/logging.h"
#include "util/utils.h"

std::vector<CacheItem> DiskCache::initCacheVector;
std::mutex DiskCache::initCacheMutex;

namespace {
using DiskCacheClock = std::chrono::steady_clock;
constexpr float CLEANUP_HEADROOM_RATIO = 0.01F;
constexpr int ACTIVE_CLEANUP_INTERVAL_MS = 100;
constexpr int IDLE_CLEANUP_INTERVAL_MS = 1000;
constexpr int MAX_CONTINUOUS_CLEANUP_ROUNDS = 8;

uint64_t ElapsedUs(DiskCacheClock::time_point start, DiskCacheClock::time_point end)
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
}
} // namespace

DiskCache::DiskCache(float ratio) { freeRatio = ratio; }

DiskCache::~DiskCache()
{
    SetEvictListener(nullptr);
    stop = true;
    cleanupCv.notify_all();
    spaceCv.notify_all();
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

int DiskCache::Start(std::string &path, int dirNum, float ratio, float bgEvitRatio)
{
    rootDir = path;
    totalDirNum = dirNum;
    freeRatio = ratio;
    int ret = RETURN_OK;
    if (ratio == 0) {
        stop = true;
        return ret;
    }
    bgFreeRatio = bgEvitRatio;
    ret = ScanCache();
    if (ret != RETURN_OK) {
        return ret;
    }

    ret = GetCurFreeRatio();
    if (ret != RETURN_OK) {
        FALCON_LOG(LOG_ERROR) << "Get Current Free Ratio failed";
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
        struct stat st;
        (void)memset(&st, 0, sizeof(st));
        std::string filePath = dirPath + "/" + f->d_name;
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

int DiskCache::GetCurFreeRatio()
{
    struct statfs diskInfo;
    (void)memset(&diskInfo, 0, sizeof(diskInfo));
    int32_t ret = statfs(rootDir.c_str(), &diskInfo);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Get disk(" << rootDir << ") stat ret " << ret << ": " << strerror(errno);
        return RETURN_ERROR;
    }

    totalCap = static_cast<uint64_t>(diskInfo.f_bsize) * static_cast<uint64_t>(diskInfo.f_blocks);
    uint64_t capacity = static_cast<uint64_t>(diskInfo.f_bsize) * static_cast<uint64_t>(diskInfo.f_bavail);
    freeCap.store(capacity);
    blockRatio = capacity * 1.0 / totalCap;
    totalInodes = static_cast<uint64_t>(diskInfo.f_files);
    freeInodes = static_cast<uint64_t>(diskInfo.f_ffree);
    inodeRatio = freeInodes * 1.0 / totalInodes;

    return RETURN_OK;
}

void DiskCache::CheckFreeSpace()
{
    bool activeCleanup = false;
    while (!stop) {
        {
            std::unique_lock<std::mutex> waitLock(cleanupNotifyMutex);
            auto waitTime = std::chrono::milliseconds(activeCleanup ? ACTIVE_CLEANUP_INTERVAL_MS : IDLE_CLEANUP_INTERVAL_MS);
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
            std::lock_guard<std::mutex> lock(mutex);
            int ret = GetCurFreeRatio();
            if (ret != RETURN_OK) {
                break;
            }
            requestedBytes = requestedCleanupBytes.exchange(0);
            bool requested = cleanupRequested.exchange(false);
            shouldCleanup = requested || blockRatio < bgFreeRatio || inodeRatio < bgFreeRatio;
            if (shouldCleanup && usedCap > 0) {
                hasFreeSpace = false;
                if (requestedBytes > 0) {
                    FALCON_LOG(LOG_WARNING) << "DiskCache::CheckFreeSpace(): background cleanup requested, bytes = "
                                            << requestedBytes;
                }
                float targetRatio = GetCleanupTargetRatio();
                for (int round = 0; round < MAX_CONTINUOUS_CLEANUP_ROUNDS && usedCap > 0; ++round) {
                    bool freed = Cleanup(targetRatio, requestedBytes);
                    didCleanup = didCleanup || freed;
                    requestedBytes = 0;
                    if (!freed || (blockRatio >= bgFreeRatio && inodeRatio >= bgFreeRatio)) {
                        break;
                    }
                    targetRatio = GetCleanupTargetRatio();
                }
            }
            hasFreeSpace = blockRatio >= bgFreeRatio && inodeRatio >= bgFreeRatio;
            activeCleanup = !hasFreeSpace.load() && usedCap > 0;
        }
        if (shouldCleanup || didCleanup) {
            spaceCv.notify_all();
        }
    }
}

void DiskCache::CleanupForEvict(uint64_t preAllocSize, std::vector<EvictedItem> &evictedItems)
{
    auto cleanupStart = DiskCacheClock::now();
    uint64_t toFreeCap = 0;
    uint64_t toFreeInode = 0;
    float freeBlockRatio = blockRatio - (preAllocSize + reservedCap) * 1.0 / totalCap;
    if (freeBlockRatio < freeRatio) {
        toFreeCap = (uint64_t)(totalCap * (freeRatio - freeBlockRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evict file due to block limit, data toFreeCap = "
                                << toFreeCap;
        if (toFreeCap > usedCap) {
            toFreeCap = usedCap;
        }
    }

    if (inodeRatio < freeRatio) {
        toFreeInode = (uint64_t)(totalInodes * (freeRatio - inodeRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evict file due to inode limit, inodes toFreeInode = "
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

    for (auto it = cacheItems.begin(); it != cacheItems.end();) {
        if (it->refs > 0) {
            ++it;
            continue;
        }
        ++scannedInode;
        uint64_t key = it->inode;
        uint64_t size = it->size;
        std::string path = it->path;
        std::string fileName = GetFilePath(key);
        auto removeStart = DiskCacheClock::now();
        int ret = remove(fileName.c_str());
        removeElapsedUs += ElapsedUs(removeStart, DiskCacheClock::now());
        if (ret == 0) {
            freedCap += size;
            freedInode++;
            it = cacheItems.erase(it);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
            evictedItems.push_back({key, size, path});
        } else {
            int err = errno;
            ++failedInode;
            ++it;
            FALCON_LOG(LOG_WARNING) << "Evict file: " << fileName << " failed: " << strerror(err);
        }
        if (freedCap >= toFreeCap && freedInode >= toFreeInode) {
            break;
        }
    }

    freeInodes += freedInode;
    RefreshRatiosFromAccounting();
    FALCON_LOG(LOG_WARNING) << "DiskCache::CleanupForEvict(): Evicted " << freedInode << " files, all size is "
                            << freedCap << ", failed files = " << failedInode << ", scanned files = "
                            << scannedInode << ", cleanup elapsed us = "
                            << ElapsedUs(cleanupStart, DiskCacheClock::now()) << ", remove elapsed us = "
                            << removeElapsedUs << ", blockRatio = " << blockRatio << ", inodeRatio = " << inodeRatio;
}

bool DiskCache::Cleanup(float targetRatio, uint64_t requestedBytes)
{
    auto cleanupStart = DiskCacheClock::now();
    std::vector<EvictedItem> evictedItems;
    uint64_t toFreeCap = 0;
    uint64_t toFreeInode = 0;
    targetRatio = std::max(targetRatio, bgFreeRatio);
    if (blockRatio < bgFreeRatio) {
        toFreeCap = (uint64_t)(totalCap * (targetRatio - blockRatio));
        if (requestedBytes > 0) {
            toFreeCap = std::max(toFreeCap, requestedBytes + reservedCap.load());
        }
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evict file due to block limit, data toFreeCap = "
                                << toFreeCap << ", targetRatio = " << targetRatio;
        if (toFreeCap > usedCap) {
            toFreeCap = usedCap;
        }
    } else if (requestedBytes > 0) {
        toFreeCap = std::min<uint64_t>(requestedBytes + reservedCap.load(), usedCap);
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evict file due to explicit request, data toFreeCap = "
                                << toFreeCap;
    }

    if (inodeRatio < bgFreeRatio) {
        uint64_t recoverableInodes = freeInodes + inodeToCacheIter.size();
        float inodeTargetRatio = std::min(targetRatio, recoverableInodes * 1.0F / totalInodes);
        toFreeInode = (uint64_t)(totalInodes * (inodeTargetRatio - inodeRatio));
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evict file due to inode limit, inodes toFreeInode = "
                                << toFreeInode << ", targetRatio = " << inodeTargetRatio;
        if (toFreeInode > inodeToCacheIter.size()) {
            toFreeInode = inodeToCacheIter.size();
        }
    }

    uint64_t freedCap = 0;
    uint64_t freedInode = 0;
    uint64_t failedInode = 0;
    uint64_t scannedInode = 0;
    uint64_t removeElapsedUs = 0;

    for (auto it = cacheItems.begin(); it != cacheItems.end();) {
        if (it->refs > 0) {
            ++it;
            continue;
        }
        ++scannedInode;
        uint64_t key = it->inode;
        uint64_t size = it->size;
        std::string path = it->path;
        std::string fileName = GetFilePath(key);
        auto removeStart = DiskCacheClock::now();
        int ret = remove(fileName.c_str());
        removeElapsedUs += ElapsedUs(removeStart, DiskCacheClock::now());
        if (ret == 0) {
            freedCap += size;
            freedInode++;
            it = cacheItems.erase(it);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
            evictedItems.push_back({key, size, path});
        } else {
            int err = errno;
            ++failedInode;
            ++it;
            FALCON_LOG(LOG_WARNING) << "Evict file: " << fileName << " failed: " << strerror(err);
        }
        if (freedCap >= toFreeCap && freedInode >= toFreeInode) {
            break;
        }
    }

    freeInodes += freedInode;
    RefreshRatiosFromAccounting();
    FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): Evicted " << freedInode << " files, all size is " << freedCap
                            << ", failed files = " << failedInode << ", scanned files = " << scannedInode
                            << ", cleanup elapsed us = " << ElapsedUs(cleanupStart, DiskCacheClock::now())
                            << ", remove elapsed us = " << removeElapsedUs << ", blockRatio = " << blockRatio
                            << ", inodeRatio = " << inodeRatio;
    auto notifyStart = DiskCacheClock::now();
    NotifyEvicted(evictedItems);
    if (!evictedItems.empty()) {
        FALCON_LOG(LOG_WARNING) << "DiskCache::Cleanup(): NotifyEvicted " << evictedItems.size()
                                << " items, elapsed us = " << ElapsedUs(notifyStart, DiskCacheClock::now());
    }
    return freedInode > 0;
}

float DiskCache::GetCleanupTargetRatio() const
{
    if (totalCap == 0) {
        return bgFreeRatio;
    }
    float recoverableRatio = (freeCap.load() + usedCap) * 1.0F / totalCap;
    return std::min(bgFreeRatio + CLEANUP_HEADROOM_RATIO, recoverableRatio);
}

void DiskCache::RefreshRatiosFromAccounting()
{
    if (totalCap > 0) {
        blockRatio = freeCap.load() * 1.0F / totalCap;
    }
    if (totalInodes > 0) {
        inodeRatio = freeInodes * 1.0F / totalInodes;
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
    if (stop) {
        std::string fileName = GetFilePath(key);
        return access(fileName.c_str(), F_OK) == 0;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        if (needPin) {
            Pin(key);
        }
        return true;
    }
    return false;
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
            ret = remove(fileName.c_str());
            if (ret != 0) {
                int err = errno;
                FALCON_LOG(LOG_ERROR) << "DeleteOldCacheWithNoPin file: " << fileName << " failed: " << strerror(err);
                return;
            }
            cacheItems.erase(elem);
            inodeToCacheIter.erase(key);
            usedCap -= size;
            freeCap += size;
        }
    }
}

bool DiskCache::UpdatePath(uint64_t key, const std::string &path)
{
    if (stop) {
        return true;
    }
    std::lock_guard<std::mutex> lock(mutex);
    auto iter = inodeToCacheIter.find(key);
    if (iter == inodeToCacheIter.end()) {
        return false;
    }
    iter->second->path = path;
    iter->second->atime = static_cast<uint64_t>(time(nullptr));
    return true;
}

void DiskCache::InsertAndUpdate(uint64_t key, uint64_t size, bool needPin, const std::string &path)
{
    if (stop) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        // update
        usedCap += static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        freeCap -= static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
        inodeToCacheIter[key]->size = size;
        if (!path.empty()) {
            inodeToCacheIter[key]->path = path;
        }
        //
    } else {
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
        if (needPin) {
            Pin(key);
        }
        //
    }
}

bool DiskCache::Update(uint64_t key, uint64_t size)
{
    if (stop) {
        return true;
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        // update
        if (size <= inodeToCacheIter[key]->size) {
            return true;
        }
        usedCap += static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        freeCap -= static_cast<int64_t>(size - inodeToCacheIter[key]->size);
        inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
        inodeToCacheIter[key]->size = size;
        // FALCON_LOG(LOG_INFO) << "Add Cache, inode =  " << key << ", size = " << size << ", usedCap = " << usedCap;
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
    std::lock_guard<std::mutex> lock(mutex);
    if (inodeToCacheIter.find(key) != inodeToCacheIter.end()) {
        // update
        usedCap += static_cast<int64_t>(size);
        freeCap -= static_cast<int64_t>(size);
        inodeToCacheIter[key]->atime = static_cast<uint64_t>(time(nullptr));
        inodeToCacheIter[key]->size += size;
        // FALCON_LOG(LOG_INFO) << "Add Cache, inode =  " << key << ", size = " << size << ", usedCap = " << usedCap;
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
        std::lock_guard<std::mutex> lock(mutex);
        GetCurFreeRatio();
        CleanupForEvict(size, evictedItems);
    }
    auto notifyStart = DiskCacheClock::now();
    NotifyEvicted(evictedItems);
    if (!evictedItems.empty()) {
        FALCON_LOG(LOG_WARNING) << "DiskCache::Evict(): NotifyEvicted " << evictedItems.size()
                                << " items, elapsed us = " << ElapsedUs(notifyStart, DiskCacheClock::now());
    }
}

bool DiskCache::CanReserve(uint64_t size) const
{
    return reservedCap + size < freeCap.load();
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
        Evict(size);
        --retryCnt;
        if (CanReserve(size)) {
            break;
        }
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
    float blockRatio = (freeCap + usedCap) * 1.0 / totalCap;
    float inodeRatio = (freeInodes + inodeToCacheIter.size()) * 1.0 / totalInodes;
    if (blockRatio <= bgFreeRatio || inodeRatio <= bgFreeRatio || blockRatio <= freeRatio || inodeRatio < freeRatio) {
        FALCON_LOG(LOG_ERROR) << "The free space can not support FalconFS running";
        FALCON_LOG(LOG_ERROR) << "Free space is not enough";
        return RETURN_ERROR;
    }
    return RETURN_OK;
}
