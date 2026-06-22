#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "test_disk_cache.h"
#include "disk_cache/disk_cache.h"
#include "util/utils.h"

std::string DiskCacheUT::rootPath = "/tmp/testdir/";

class RecordingEvictListener : public DiskCacheEvictListener {
  public:
    void OnEvicted(const EvictedItem &item) override { items.push_back(item); }

    std::vector<EvictedItem> items;
};

class InodeCheckingUnlinkListener : public DiskCacheEvictListener {
  public:
    explicit InodeCheckingUnlinkListener(std::unordered_map<std::string, uint64_t> &entries) : entries(entries) {}

    void OnEvicted(const EvictedItem &item) override
    {
        auto iter = entries.find(item.path);
        if (iter == entries.end()) {
            skippedMissingPath++;
            return;
        }
        if (iter->second != item.inode) {
            skippedInodeMismatch++;
            return;
        }
        deletedPath = item.path;
        deletedInode = iter->second;
        entries.erase(iter);
    }

    std::string deletedPath;
    uint64_t deletedInode{0};
    uint64_t skippedMissingPath{0};
    uint64_t skippedInodeMismatch{0};

  private:
    std::unordered_map<std::string, uint64_t> &entries;
};

class BatchRecordingPreEvictListener : public DiskCacheEvictListener {
  public:
    void OnEvictingBatch(const std::vector<EvictedItem> &items, std::vector<bool> &results) override
    {
        ++batchCalls;
        batchSizes.push_back(items.size());
        results.assign(items.size(), true);
    }

    void OnEvicted(const EvictedItem &item) override { evictedItems.push_back(item); }

    uint64_t batchCalls{0};
    std::vector<std::size_t> batchSizes;
    std::vector<EvictedItem> evictedItems;
};

class FailingPreEvictListener : public DiskCacheEvictListener {
  public:
    void OnEvictingBatch(const std::vector<EvictedItem> &items, std::vector<bool> &results) override
    {
        results.assign(items.size(), false);
        if (!items.empty()) {
            attemptedItem = items.front();
        }
        attempted += items.size();
    }

    void OnEvicted(const EvictedItem &item) override
    {
        (void)item;
        notified++;
    }

    EvictedItem attemptedItem;
    uint64_t attempted{0};
    uint64_t notified{0};
};

class DelayedFailingPreEvictListener : public DiskCacheEvictListener {
  public:
    void OnEvictingBatch(const std::vector<EvictedItem> &items, std::vector<bool> &results) override
    {
        results.assign(items.size(), false);
        if (!items.empty()) {
            attemptedItem = items.front();
        }
        attempted += items.size();
        started = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    void OnEvicted(const EvictedItem &item) override
    {
        (void)item;
        notified++;
    }

    EvictedItem attemptedItem;
    std::atomic<bool> started{false};
    uint64_t attempted{0};
    uint64_t notified{0};
};

TEST_F(DiskCacheUT, Start)
{
    int ret = DiskCache::GetInstance().Start(rootPath, 100, 0.2);
    EXPECT_EQ(ret, 0);
}

TEST_F(DiskCacheUT, StartWithZeroWatermarkUsesDirectFileChecks)
{
    std::string directRoot = "/tmp/testdir_zero_ratio";
    std::filesystem::remove_all(directRoot);
    std::filesystem::create_directories(directRoot + "/0");
    SetRootPath(directRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    EXPECT_EQ(cache.Start(directRoot, 1, 0.0), 0);

    uint64_t key = 100;
    std::string file = GetFilePath(key);
    EXPECT_FALSE(cache.Find(key, false));
    {
        std::ofstream out(file);
        out << "cached";
    }
    EXPECT_TRUE(cache.Find(key, false));
    EXPECT_EQ(cache.Delete(key), 0);
    EXPECT_FALSE(std::filesystem::exists(file));

    std::filesystem::remove_all(directRoot);
}

TEST_F(DiskCacheUT, InsertUpdatePinAndDeleteLifecycle)
{
    std::string cacheRoot = "/tmp/testdir_lifecycle";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 3; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(3);

    DiskCache cache;

    uint64_t key = 301;
    std::string file = GetFilePath(key);
    {
        std::ofstream out(file);
        out << "cache-data";
    }

    EXPECT_FALSE(cache.Find(key, false));
    cache.InsertAndUpdate(key, 10, false);
    EXPECT_TRUE(cache.Find(key, false));
    EXPECT_TRUE(cache.Find(key, true));
    cache.Unpin(key);

    EXPECT_TRUE(cache.Update(key, 20));
    EXPECT_TRUE(cache.Add(key, 5));
    EXPECT_FALSE(cache.Update(999999, 1));
    EXPECT_FALSE(cache.Add(999999, 1));

    EXPECT_EQ(cache.Delete(key), 0);
    EXPECT_FALSE(cache.Find(key, false));
    EXPECT_FALSE(std::filesystem::exists(file));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, DeleteOldCacheSkipsPinnedEntry)
{
    std::string cacheRoot = "/tmp/testdir_delete_old";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 2; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(2);

    DiskCache cache;

    uint64_t pinnedKey = 42;
    uint64_t unpinnedKey = 43;
    std::string pinnedFile = GetFilePath(pinnedKey);
    std::string unpinnedFile = GetFilePath(unpinnedKey);
    {
        std::ofstream out(pinnedFile);
        out << "pinned";
    }
    {
        std::ofstream out(unpinnedFile);
        out << "unpinned";
    }

    cache.InsertAndUpdate(pinnedKey, 6, true);
    cache.InsertAndUpdate(unpinnedKey, 8, false);
    cache.DeleteOldCacheWithNoPin(pinnedKey);
    EXPECT_TRUE(cache.Find(pinnedKey, false));
    EXPECT_TRUE(std::filesystem::exists(pinnedFile));

    cache.DeleteOldCacheWithNoPin(unpinnedKey);
    EXPECT_FALSE(cache.Find(unpinnedKey, false));
    EXPECT_FALSE(std::filesystem::exists(unpinnedFile));

    cache.Unpin(pinnedKey);
    EXPECT_EQ(cache.Delete(pinnedKey), 0);
    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, StartScansExistingCacheFiles)
{
    std::string cacheRoot = "/tmp/testdir_scan";
    std::filesystem::remove_all(cacheRoot);
    for (int i = 0; i < 2; ++i) {
        std::filesystem::create_directories(cacheRoot + "/" + std::to_string(i));
    }
    SetRootPath(cacheRoot);
    SetTotalDirectory(2);

    uint64_t key = 302;
    std::string file = GetFilePath(key);
    {
        std::ofstream out(file);
        out << "existing-cache";
    }

    DiskCache cache;
    EXPECT_EQ(cache.Start(cacheRoot, 2, 0.000001), 0);
    EXPECT_TRUE(cache.Find(key, false));
    EXPECT_EQ(cache.Delete(key), 0);
    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, ZeroWatermarkStopModeCoversNoopBranches)
{
    std::string cacheRoot = "/tmp/testdir_zero_stop";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.0), 0);

    uint64_t key = 404;
    std::string file = GetFilePath(key);
    {
        std::ofstream out(file);
        out << "stop-mode";
    }

    EXPECT_TRUE(cache.Find(key, true));
    cache.InsertAndUpdate(key, 9, true);
    EXPECT_TRUE(cache.Update(key, 10));
    EXPECT_TRUE(cache.Add(key, 1));
    cache.Pin(key);
    cache.Unpin(key);
    EXPECT_TRUE(cache.PreAllocSpace(1024));
    cache.FreePreAllocSpace(1024);
    EXPECT_TRUE(cache.HasFreeSpace());
    EXPECT_EQ(cache.Delete(key), 0);
    EXPECT_EQ(cache.Delete(key), -1);

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, UtilityEnvironmentBranches)
{
    SetRootPath("/tmp/util_root");
    SetTotalDirectory(8);
    EXPECT_EQ(GetFilePath(17), "/tmp/util_root/1/17-large");

    int randomValue = GenerateRandom(1, 3);
    EXPECT_GE(randomValue, 1);
    EXPECT_LE(randomValue, 3);

    setenv("USER", "falcon_user", 1);
    ASSERT_TRUE(GetUserName().has_value());
    EXPECT_EQ(GetUserName().value(), "falcon_user");
    unsetenv("USER");
    EXPECT_FALSE(GetUserName().has_value());

    EXPECT_EQ(SplitIp("10.0.0.1:56039").value(), "10.0.0.1");
    EXPECT_EQ(SplitIp("10.0.0.1").value(), "10.0.0.1");

    unsetenv("POD_IP");
    unsetenv("BRPC_PORT");
    EXPECT_EQ(GetPodIPPort(), "127.0.0.1:56039");
    setenv("POD_IP", "10.1.1.1", 1);
    EXPECT_EQ(GetPodIPPort(), "10.1.1.1:56039");
    setenv("BRPC_PORT", "56100", 1);
    EXPECT_EQ(GetPodIPPort(), "10.1.1.1:56100");
    unsetenv("POD_IP");
    unsetenv("BRPC_PORT");

    unsetenv("STORAGE_THRESHOLD");
    EXPECT_FLOAT_EQ(GetStorageUsedWatermark(), 0.8F);
    setenv("STORAGE_THRESHOLD", "0.42", 1);
    EXPECT_FLOAT_EQ(GetStorageUsedWatermark(), 0.42F);
    unsetenv("STORAGE_THRESHOLD");

    unsetenv("PARENT_PATH_LEVEL");
    EXPECT_EQ(GetParentPathLevel(), -1);
    setenv("PARENT_PATH_LEVEL", "3", 1);
    EXPECT_EQ(GetParentPathLevel(), 3);
    unsetenv("PARENT_PATH_LEVEL");
}

TEST_F(DiskCacheUT, MaxLocalDiskSizeCapsEvictWatermark)
{
    std::string cacheRoot = "/tmp/testdir_max_local_disk_size";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.2F, 1024), 0);

    uint64_t key = 681;
    std::string fileName = GetFilePath(key);
    {
        std::ofstream out(fileName);
        out << std::string(900, 'x');
    }
    cache.InsertAndUpdate(key, 900, false, "/logical/max-local-disk-size");
    ASSERT_TRUE(std::filesystem::exists(fileName));

    EXPECT_TRUE(cache.PreAllocSpace(200));
    cache.FreePreAllocSpace(200);
    EXPECT_FALSE(std::filesystem::exists(fileName));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, ForegroundWatermarkEvictsBeforeBackgroundWatermark)
{
    std::string cacheRoot = "/tmp/testdir_foreground_background_watermark";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.1F, 1000), 0);

    uint64_t key = 682;
    std::string fileName = GetFilePath(key);
    {
        std::ofstream out(fileName);
        out << std::string(750, 'x');
    }
    cache.InsertAndUpdate(key, 750, false, "/logical/foreground-background-watermark");

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_TRUE(std::filesystem::exists(fileName));

    EXPECT_TRUE(cache.PreAllocSpace(151));
    cache.FreePreAllocSpace(151);
    EXPECT_FALSE(std::filesystem::exists(fileName));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, EvictNotifiesListenerWithStoredPath)
{
    std::string cacheRoot = "/tmp/testdir_evict_listener";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    uint64_t key = 701;
    std::string logicalPath = "/logical/evicted";
    {
        std::ofstream out(GetFilePath(key));
        out << "evict-me";
    }

    RecordingEvictListener listener;
    cache.SetEvictListener(&listener);
    cache.InsertAndUpdate(key, 8, false, logicalPath);
    cache.Evict(UINT64_MAX / 4);
    cache.SetEvictListener(nullptr);

    ASSERT_EQ(listener.items.size(), 1U);
    EXPECT_EQ(listener.items[0].inode, key);
    EXPECT_EQ(listener.items[0].size, 8U);
    EXPECT_EQ(listener.items[0].path, logicalPath);
    EXPECT_FALSE(std::filesystem::exists(GetFilePath(key)));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, EvictMetadataUnlinkSkipsReusedPathWithDifferentInode)
{
    std::string cacheRoot = "/tmp/testdir_evict_reused_path_inode_guard";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    uint64_t evictedInode = 901;
    uint64_t recreatedInode = 902;
    std::string reusedPath = "/logical/reused-name";
    {
        std::ofstream out(GetFilePath(evictedInode));
        out << "evicted-data";
    }

    std::unordered_map<std::string, uint64_t> metadataEntries;
    metadataEntries[reusedPath] = recreatedInode;

    InodeCheckingUnlinkListener listener(metadataEntries);
    cache.SetEvictListener(&listener);
    cache.InsertAndUpdate(evictedInode, 12, false, reusedPath);

    cache.Evict(UINT64_MAX / 4);
    cache.SetEvictListener(nullptr);

    EXPECT_TRUE(listener.deletedPath.empty());
    EXPECT_EQ(listener.deletedInode, 0U);
    EXPECT_EQ(listener.skippedInodeMismatch, 1U);
    ASSERT_EQ(metadataEntries.count(reusedPath), 1U);
    EXPECT_EQ(metadataEntries[reusedPath], recreatedInode);
    EXPECT_FALSE(std::filesystem::exists(GetFilePath(evictedInode)));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, EvictSkipsCacheRemovalWhenMetadataUnlinkFails)
{
    std::string cacheRoot = "/tmp/testdir_evict_metadata_failure";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    uint64_t inode = 951;
    std::string logicalPath = "/logical/metadata-fails";
    {
        std::ofstream out(GetFilePath(inode));
        out << "cache-data";
    }

    FailingPreEvictListener listener;
    cache.SetEvictListener(&listener);
    cache.InsertAndUpdate(inode, 10, false, logicalPath);

    cache.Evict(UINT64_MAX / 4);
    cache.SetEvictListener(nullptr);

    EXPECT_EQ(listener.attempted, 1U);
    EXPECT_EQ(listener.notified, 0U);
    EXPECT_EQ(listener.attemptedItem.inode, inode);
    EXPECT_EQ(listener.attemptedItem.path, logicalPath);
    EXPECT_TRUE(cache.Find(inode, false));
    EXPECT_TRUE(std::filesystem::exists(GetFilePath(inode)));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, EvictSkipsCacheRemovalWhenMetadataLeaseConflicts)
{
    std::string cacheRoot = "/tmp/testdir_evict_lease_conflict";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    uint64_t inode = 952;
    std::string logicalPath = "/logical/lease-conflict";
    {
        std::ofstream out(GetFilePath(inode));
        out << "leased-cache-data";
    }

    FailingPreEvictListener leaseConflictListener;
    cache.SetEvictListener(&leaseConflictListener);
    cache.InsertAndUpdate(inode, 17, false, logicalPath);

    cache.Evict(UINT64_MAX / 4);
    cache.SetEvictListener(nullptr);

    EXPECT_EQ(leaseConflictListener.attempted, 1U);
    EXPECT_EQ(leaseConflictListener.notified, 0U);
    EXPECT_EQ(leaseConflictListener.attemptedItem.inode, inode);
    EXPECT_EQ(leaseConflictListener.attemptedItem.path, logicalPath);
    EXPECT_TRUE(cache.Find(inode, false));
    EXPECT_TRUE(std::filesystem::exists(GetFilePath(inode)));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, FindWithWaitHitsAfterEvictingCancelled)
{
    std::string cacheRoot = "/tmp/testdir_evict_wait_cancelled";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    uint64_t inode = 953;
    std::string logicalPath = "/logical/evict-wait-cancelled";
    {
        std::ofstream out(GetFilePath(inode));
        out << "evict-wait-cache-data";
    }

    DelayedFailingPreEvictListener listener;
    cache.SetEvictListener(&listener);
    cache.InsertAndUpdate(inode, 21, false, logicalPath);

    std::thread evictThread([&cache]() {
        cache.Evict(UINT64_MAX / 4);
    });
    for (int i = 0; i < 100 && !listener.started.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(listener.started.load());

    EXPECT_EQ(cache.FindWithWait(inode, true, 1000), DiskCacheFindResult::HIT);
    cache.Unpin(inode);

    evictThread.join();
    cache.SetEvictListener(nullptr);

    EXPECT_EQ(listener.attempted, 1U);
    EXPECT_EQ(listener.notified, 0U);
    EXPECT_EQ(listener.attemptedItem.inode, inode);
    EXPECT_TRUE(cache.Find(inode, false));
    EXPECT_TRUE(std::filesystem::exists(GetFilePath(inode)));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, EvictUsesBatchPreEvictCallback)
{
    std::string cacheRoot = "/tmp/testdir_evict_batch_callback";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    for (uint64_t i = 0; i < 3; ++i) {
        uint64_t inode = 970 + i;
        {
            std::ofstream out(GetFilePath(inode));
            out << "cache-data";
        }
        cache.InsertAndUpdate(inode, 10, false, "/logical/batch/" + std::to_string(inode));
    }

    BatchRecordingPreEvictListener listener;
    cache.SetEvictListener(&listener);
    cache.Evict(UINT64_MAX / 4);
    cache.SetEvictListener(nullptr);

    EXPECT_EQ(listener.batchCalls, 1U);
    ASSERT_EQ(listener.batchSizes.size(), 1U);
    EXPECT_EQ(listener.batchSizes[0], 3U);
    EXPECT_EQ(listener.evictedItems.size(), 3U);

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, EvictClearsStaleIndexWhenLocalCacheFileAlreadyMissing)
{
    std::string cacheRoot = "/tmp/testdir_evict_missing_local_file";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.000001), 0);

    uint64_t inode = 981;
    std::string logicalPath = "/logical/missing-local-cache";
    cache.InsertAndUpdate(inode, 10, false, logicalPath);
    ASSERT_FALSE(std::filesystem::exists(GetFilePath(inode)));
    ASSERT_TRUE(cache.Find(inode, false));

    RecordingEvictListener listener;
    cache.SetEvictListener(&listener);
    cache.Evict(UINT64_MAX / 4);
    cache.SetEvictListener(nullptr);

    EXPECT_FALSE(cache.Find(inode, false));
    ASSERT_EQ(listener.items.size(), 1U);
    EXPECT_EQ(listener.items[0].inode, inode);
    EXPECT_EQ(listener.items[0].path, logicalPath);

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, BackgroundEvictionRatioSkipsPinnedEntries)
{
    std::string cacheRoot = "/tmp/testdir_background_ratio_skip_pinned";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache;
    ASSERT_EQ(cache.Start(cacheRoot, 1, 0.2F, 1024), 0);

    RecordingEvictListener listener;
    cache.SetEvictListener(&listener);

    for (uint64_t i = 0; i < 5; ++i) {
        uint64_t inode = 1100 + i;
        {
            std::ofstream out(GetFilePath(inode));
            out << std::string(150, 'x');
        }
        cache.InsertAndUpdate(inode, 150, i == 0, "/logical/ratio/" + std::to_string(inode));
    }

    for (int i = 0; i < 30 && listener.items.empty(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    cache.SetEvictListener(nullptr);
    cache.Unpin(1100);

    ASSERT_EQ(listener.items.size(), 1U);
    EXPECT_EQ(listener.items[0].inode, 1101U);
    EXPECT_TRUE(cache.Find(1100, false));
    EXPECT_FALSE(cache.Find(1101, false));

    std::filesystem::remove_all(cacheRoot);
}

TEST_F(DiskCacheUT, PublicEvictAndFailureBranches)
{
    std::string cacheRoot = "/tmp/testdir_public_evict";
    std::filesystem::remove_all(cacheRoot);
    std::filesystem::create_directories(cacheRoot + "/0");
    SetRootPath(cacheRoot);
    SetTotalDirectory(1);

    DiskCache cache(0.4);
    EXPECT_EQ(cache.Start(cacheRoot, 1, 2.0), RETURN_ERROR);

    uint64_t pinnedKey = 501;
    uint64_t removableKey = 502;
    uint64_t missingFileKey = 503;
    {
        std::ofstream out(GetFilePath(pinnedKey));
        out << "pinned";
    }
    {
        std::ofstream out(GetFilePath(removableKey));
        out << "removable";
    }
    cache.InsertAndUpdate(pinnedKey, 10, true);
    cache.InsertAndUpdate(removableKey, 10, false);
    cache.InsertAndUpdate(missingFileKey, 10, false);
    cache.InsertAndUpdate(missingFileKey, 20, false);

    cache.Evict(UINT64_MAX / 4);
    EXPECT_TRUE(cache.Find(pinnedKey, false));
    EXPECT_FALSE(cache.Find(removableKey, false));
    EXPECT_FALSE(std::filesystem::exists(GetFilePath(removableKey)));

    std::string missingRoot = "/tmp/testdir_public_evict_start_missing";
    std::filesystem::remove_all(missingRoot);
    DiskCache startFailureCache;
    EXPECT_EQ(startFailureCache.Start(missingRoot, 1, 0.1), RETURN_ERROR);

    uint64_t deleteMissingKey = 504;
    cache.InsertAndUpdate(deleteMissingKey, 1, false);
    EXPECT_LT(cache.Delete(deleteMissingKey), 0);

    uint64_t oldMissingKey = 505;
    cache.InsertAndUpdate(oldMissingKey, 1, false);
    cache.DeleteOldCacheWithNoPin(oldMissingKey);
    EXPECT_TRUE(cache.Find(oldMissingKey, false));

    EXPECT_FALSE(cache.PreAllocSpace(UINT64_MAX / 4));
    EXPECT_FALSE(cache.HasFreeSpace());

    cache.Unpin(pinnedKey);
    EXPECT_EQ(cache.Delete(pinnedKey), 0);

    std::filesystem::remove_all(cacheRoot);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
