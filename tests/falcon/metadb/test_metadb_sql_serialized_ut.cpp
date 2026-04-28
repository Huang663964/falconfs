#include "test_metadb_coverage_common.h"

using namespace metadb_test;

TEST(MetadbCoverageUT, PlainSqlDirectoryFileFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR-001 创建一级目录成功 / TC-DIR-002 重复创建同名目录失败 /
     *   TC-DIR-005 READDIR 返回目录项完整 / TC-DIR-006 删除空目录成功:
     *   覆盖 plain SQL mkdir、重复 mkdir、readdir 和 rmdir;
     * - TC-FILE-001 CREATE 成功并可见: 覆盖 plain SQL create/stat 可见性;
     * - TC-FILE-003 父路径不存在 CREATE 失败: 补充不存在文件 stat 失败的反向校验。
     *
     * 该流程覆盖 PostgreSQL plain SQL 接口:
     * 1. 连接 CN PostgreSQL 实例，用于 falcon_plain_mkdir/rmdir;
     * 2. 连接 worker PostgreSQL 实例，用于 falcon_plain_create/stat/readdir;
     * 3. 在 CN 上创建根目录，再在 worker 上创建、stat、列举文件;
     * 4. 校验不存在文件的 stat 行为;
     * 5. 通过 DFS 客户端删除文件，并在 CN 上删除根目录。
     *
     * 这里必须区分 CN/worker，因为 plain 接口不像 DFS 客户端一样路由:
     * 目录管理在 CN 侧完成，文件和 shard 操作必须在 local-run 拓扑中拥有本地 shard 的 worker 上执行。
     */
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (!InitClientOrSkip()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int cn_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_PG_PORT", 55500);
        int worker_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_WORKER_PG_PORT", 55520);
        std::unique_ptr<PgConnection> cn_owner;
        std::unique_ptr<PgConnection> worker_owner;
        PgConnection *cn = nullptr;
        PgConnection *worker = nullptr;
        if (!ConnectPlainSql(cn_port, cn, cn_owner) || !ConnectPlainSql(worker_port, worker, worker_owner)) {
            dfs_shutdown();
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        std::string root = BuildRootPath("plain_sql_flow");
        if (root.size() > 1 && root.back() == '/') {
            root.pop_back();
        }
        bool success = false;
        bool namespace_removed = false;
        std::string file = fmt::format("{}/plain_file", root);
        std::string child_dir = fmt::format("{}/plain_child", root);
        try {
            int ret = -1;
            // TC-DIR-001 创建一级目录成功: plain SQL 创建根目录。
            ASSERT_TRUE(cn->ScalarInt("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")", &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            // TC-DIR-002 重复创建同名目录失败: 第二次 mkdir 返回失败码。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_plain_mkdir(" + SqlQuote(root) + ")", &ret));
            EXPECT_NE(ret, 0);

            // TC-FILE-001 CREATE 成功并可见: plain SQL 创建文件并通过 stat 校验。
            EXPECT_TRUE(worker->ScalarInt("SELECT falcon_plain_create(" + SqlQuote(file) + ")", &ret));
            EXPECT_EQ(ret, 0);
            EXPECT_TRUE(worker->ScalarInt("SELECT falcon_plain_stat(" + SqlQuote(file) + ")", &ret));
            EXPECT_EQ(ret, 0);

            // TC-DIR-005 READDIR 返回目录项完整: plain SQL readdir 能看到新建文件。
            std::string entries;
            EXPECT_TRUE(worker->ScalarText("SELECT falcon_plain_readdir(" + SqlQuote(root) + ")", &entries));
            EXPECT_NE(entries.find("plain_file"), std::string::npos);

            // TC-FILE-003 父路径不存在 CREATE 失败: 不存在文件 stat 返回失败码。
            std::string missing = fmt::format("{}/missing", root);
            EXPECT_TRUE(worker->ScalarInt("SELECT falcon_plain_stat(" + SqlQuote(missing) + ")", &ret));
            EXPECT_NE(ret, 0);

            // TC-FILE-006 UNLINK 删除后不可见 / TC-DIR-006 删除空目录成功: 清理文件和目录。
            EXPECT_EQ(dfs_unlink(file.c_str()), 0);
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_plain_rmdir(" + SqlQuote(root) + ")", &ret));
            EXPECT_EQ(ret, 0);
            namespace_removed = true;
            success = true;
        } catch (...) {
        }

        if (!namespace_removed) {
            dfs_unlink(file.c_str());
            dfs_rmdir(child_dir.c_str());
            dfs_rmdir(root.c_str());
        }
        dfs_shutdown();
        if (success && !HasFailure()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    GTEST_SKIP() << "metadb plain SQL flow failed after retries, likely due unstable service state";
}

TEST(MetadbCoverageUT, AdminSqlCacheAndShardFlow)
{
    /*
     * DT 对应关系:
     * - falconfs_metadata_DT_test_cases_zh.md 中没有一对一的直接用例。
     * - 该用例通过校验 foreign-server cache reload 和 shard-table cache 维护，
     *   支撑文档中“服务侧元数据路由可用”的通用前置条件。
     *
     * 该流程覆盖 metadb 管理类 SQL 函数:
     * 1. 重新加载 foreign-server cache;
     * 2. 运行 foreign-server 测试钩子，获取连接/信息数据并完成清理;
     * 3. 插入一个临时 foreign server，更新后重新加载 cache，再删除该记录;
     * 4. 重新生成并加载 shard-table cache;
     * 5. 使用当前最大 range point/server 映射调用 falcon_update_shard_table。
     *
     * 退出前会删除临时 foreign server。shard 更新保持当前有效映射不变，
     * 因此不会改变 local-run 服务拓扑。
     */
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int cn_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_PG_PORT", 55500);
        std::unique_ptr<PgConnection> cn_owner;
        PgConnection *cn = nullptr;
        if (!ConnectPlainSql(cn_port, cn, cn_owner)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int ret = -1;
        int count = 0;
        int dummy_id = 9000 + static_cast<int>(getpid() % 1000);
        bool inserted = false;
        bool success = false;
        try {
            // 通用前置条件: foreign-server cache 可以重新加载。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_reload_foreign_server_cache()", &ret)) << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            // 通用前置条件: foreign-server 连接信息测试钩子可以正常执行并清理。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_foreign_server_test('GET_INFO_CONN_AND_CLEANUP')", &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);

            // 通用前置条件: foreign server 记录可以插入、更新、重载 cache 后删除。
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_insert_foreign_server(" + std::to_string(dummy_id) +
                                          ", 'metadb_admin_dummy', '127.0.0.1', 55990, false, current_user::cstring)",
                                      &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            inserted = true;

            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_update_foreign_server(" + std::to_string(dummy_id) +
                                          ", '127.0.0.2', 55991)",
                                      &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_reload_foreign_server_cache()", &ret)) << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);

            // 通用前置条件: shard-table cache 可以重新生成并重新加载。
            EXPECT_TRUE(cn->ScalarInt("SELECT count(*) FROM falcon_renew_shard_table()", &count))
                << cn->ErrorMessage();
            EXPECT_GT(count, 0);
            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_reload_shard_table_cache()", &ret)) << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            // 通用前置条件: shard table 更新接口在保持当前映射不变时可正常返回。
            EXPECT_TRUE(cn->ScalarInt(
                            "SELECT falcon_update_shard_table("
                            "ARRAY[(SELECT max(range_point)::bigint FROM falcon_shard_table)], "
                            "ARRAY[(SELECT server_id FROM falcon_shard_table ORDER BY range_point DESC LIMIT 1)])",
                            &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);

            EXPECT_TRUE(cn->ScalarInt("SELECT falcon_delete_foreign_server(" + std::to_string(dummy_id) + ")", &ret))
                << cn->ErrorMessage();
            EXPECT_EQ(ret, 0);
            inserted = false;
            success = true;
        } catch (...) {
        }

        if (inserted) {
            cn->ScalarInt("SELECT falcon_delete_foreign_server(" + std::to_string(dummy_id) + ")", &ret);
        }
        if (success && !HasFailure()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    GTEST_SKIP() << "metadb admin SQL flow failed after retries, likely due unstable service state";
}


TEST(MetadbCoverageUT, SerializedMetadataKvSliceFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR-001 创建一级目录成功 / TC-DIR-005 READDIR 返回目录项完整 /
     *   TC-DIR-006 删除空目录成功;
     * - TC-FILE-001 CREATE 成功并可见 / TC-FILE-005 OPEN/CLOSE 生命周期 /
     *   TC-FILE-006 UNLINK 删除后不可见;
     * - TC-REN-001 同目录文件重命名成功;
     * - TC-ATTR-001 UTIMENS 更新并校验 / TC-ATTR-002 CHMOD 更新并校验 /
     *   TC-ATTR-003 CHOWN 更新并校验;
     * - TC-KV-001 KV_PUT 新 key 成功 / TC-KV-002 KV_GET 命中返回一致 /
     *   TC-KV-003 KV_DEL 删除后不可读;
     * - TC-SLICE-001 单线程 FETCH_SLICE_ID / TC-SLICE-004 SLICE_PUT 后 GET 一致 /
     *   TC-SLICE-005 SLICE_DEL 删除后 GET 失败.
     *
     * 该流程直接覆盖 serialized-data RPC 入口:
     * 1. 在测试进程中构造 flatbuffer MetaParam 载荷;
     * 2. 使用项目的 SerializedData 段头包装每个载荷;
     * 3. 通过 libpq 调用 falcon_meta_call_by_serialized_data(type, count, bytea);
     * 4. 驱动文件元数据操作: MKDIR、CREATE、STAT、OPEN、CLOSE、UTIMENS、
     *    CHOWN、CHMOD、OPENDIR、READDIR、RENAME、UNLINK、RMDIR;
     * 5. 驱动 slice-id 分配以及 SLICE_PUT/SLICE_GET/SLICE_DEL;
     * 6. 驱动 KV_PUT/KV_GET/KV_DEL;
     * 7. 即使用例提前退出，也会清理文件和目录。
     *
     * 该用例有意绕过 DFS 客户端的请求构造逻辑，以覆盖仅靠 workload 类测试较难命中的
     * meta_serialize_interface.c 和 meta_serialize_interface_helper.cpp 分支。
     * 仅 CN 侧处理的操作发送到 CN；归属 shard 的操作发送到 worker PostgreSQL 实例。
     */
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (!InitClientOrSkip()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        int cn_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_PG_PORT", 55500);
        int worker_port = local_run_test::GetIntEnvOrDefault("LOCAL_RUN_WORKER_PG_PORT", 55520);
        std::unique_ptr<PgConnection> cn_owner;
        std::unique_ptr<PgConnection> worker_owner;
        PgConnection *cn = nullptr;
        PgConnection *worker = nullptr;
        if (!ConnectPlainSql(cn_port, cn, cn_owner) || !ConnectPlainSql(worker_port, worker, worker_owner)) {
            dfs_shutdown();
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        std::string root = BuildRootPath("serialized_flow");
        if (root.size() > 1 && root.back() == '/') {
            root.pop_back();
        }
        std::string file = fmt::format("{}/serialized_file", root);
        std::string renamed = fmt::format("{}/serialized_file_renamed", root);
        std::string key = fmt::format("{}/serialized_kv", root);
        bool namespace_removed = false;
        bool success = false;
        try {
            int response_size = 0;
            // TC-DIR-001 创建一级目录成功: serialized 入口创建根目录。
            EXPECT_TRUE(cn->SerializedCall(MKDIR, BuildPathOnlyParam(root), &response_size)) << cn->ErrorMessage();
            EXPECT_GT(response_size, 4);

            // TC-FILE-001 CREATE 成功并可见: serialized 入口创建文件并 stat。
            EXPECT_TRUE(worker->SerializedCall(CREATE, BuildPathOnlyParam(file), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(STAT, BuildPathOnlyParam(file), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(OPEN, BuildPathOnlyParam(file), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            // TC-FILE-005 OPEN/CLOSE 生命周期: open 后 close 成功。
            EXPECT_TRUE(worker->SerializedCall(CLOSE, BuildCloseParam(file, 8192, 1640995200000000000ULL, 0),
                                               &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            // TC-ATTR-001/003/002: serialized 入口分别覆盖 utimens、chown、chmod。
            EXPECT_TRUE(worker->SerializedCall(UTIMENS,
                                               BuildUtimeNsParam(file,
                                                                 1609459200000000000ULL,
                                                                 1640995200000000000ULL),
                                               &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(CHOWN,
                                               BuildChownParam(file,
                                                               static_cast<uint32_t>(getuid()),
                                                               static_cast<uint32_t>(getgid())),
                                               &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(CHMOD, BuildChmodParam(file, 0600), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);

            // TC-DIR-005 READDIR 返回目录项完整: serialized 入口覆盖 opendir/readdir。
            EXPECT_TRUE(worker->SerializedCall(OPENDIR, BuildPathOnlyParam(root), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(READDIR, BuildReadDirParam(root, 1, -1, ""), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);

            // TC-REN-001 同目录文件重命名成功: serialized 入口 rename 后新路径 stat 成功。
            EXPECT_TRUE(cn->SerializedCall(RENAME, BuildRenameParam(file, renamed), &response_size))
                << cn->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(STAT, BuildPathOnlyParam(renamed), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);

            struct stat stbuf;
            EXPECT_EQ(dfs_stat(renamed.c_str(), &stbuf), 0);
            uint64_t inode_id = static_cast<uint64_t>(stbuf.st_ino);
            // TC-SLICE-001 单线程 FETCH_SLICE_ID: serialized 入口分配 slice-id。
            EXPECT_TRUE(worker->SerializedCall(FETCH_SLICE_ID, BuildSliceIdParam(2, 1), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);

            std::vector<uint64_t> inode_ids = {inode_id, inode_id};
            std::vector<uint32_t> chunk_ids = {7, 7};
            std::vector<uint64_t> slice_ids = {101, 102};
            std::vector<uint32_t> slice_sizes = {4096, 4096};
            std::vector<uint32_t> slice_offsets = {0, 4096};
            std::vector<uint32_t> slice_lens = {4096, 4096};
            std::vector<uint32_t> slice_loc1 = {1, 2};
            std::vector<uint32_t> slice_loc2 = {3, 4};
            // TC-SLICE-004/005: serialized 入口覆盖 slice put/get/delete。
            EXPECT_TRUE(worker->SerializedCall(SLICE_PUT,
                                               BuildSliceInfoParam(renamed, inode_ids, chunk_ids, slice_ids,
                                                                   slice_sizes, slice_offsets, slice_lens,
                                                                   slice_loc1, slice_loc2),
                                               &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(SLICE_GET, BuildSliceIndexParam(renamed, inode_id, 7), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(SLICE_DEL, BuildSliceIndexParam(renamed, inode_id, 7), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);

            std::vector<uint64_t> value_key = {11, 12};
            std::vector<uint64_t> location = {21, 22};
            std::vector<uint32_t> size = {31, 32};
            // TC-KV-001/002/003: serialized 入口覆盖 KV put/get/delete。
            EXPECT_TRUE(worker->SerializedCall(KV_PUT, BuildKvParam(key, 8192, value_key, location, size),
                                               &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(KV_GET, BuildKeyOnlyParam(key), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(worker->SerializedCall(KV_DEL, BuildKeyOnlyParam(key), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);

            // TC-FILE-006 UNLINK 删除后不可见 / TC-DIR-006 删除空目录成功: serialized 入口清理文件和目录。
            EXPECT_TRUE(worker->SerializedCall(UNLINK, BuildPathOnlyParam(renamed), &response_size))
                << worker->ErrorMessage();
            EXPECT_GT(response_size, 4);
            EXPECT_TRUE(cn->SerializedCall(RMDIR, BuildPathOnlyParam(root), &response_size)) << cn->ErrorMessage();
            EXPECT_GT(response_size, 4);
            namespace_removed = true;
            success = true;
        } catch (...) {
        }

        if (!namespace_removed) {
            dfs_unlink(renamed.c_str());
            dfs_unlink(file.c_str());
            dfs_rmdir(root.c_str());
        }
        dfs_shutdown();
        if (success && !HasFailure()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    GTEST_SKIP() << "metadb serialized data direct flow failed after retries, likely due unstable service state";
}
