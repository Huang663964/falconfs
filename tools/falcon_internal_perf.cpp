#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "brpc/brpc_server.h"
#include "conf/falcon_property_key.h"
#include "falcon_meta.h"
#include "init/falcon_init.h"

namespace {

using Clock = std::chrono::steady_clock;
std::unique_ptr<std::thread> g_brpcServerThread;

struct Options {
    std::string mode;
    std::string dir;
    std::string output;
    int files = 0;
    int fileSize = 0;
    int window = 0;
    int unlinkFiles = 0;
    int waitSec = 0;
    int workers = 1;
};

double ElapsedSec(Clock::time_point begin, Clock::time_point end)
{
    return std::chrono::duration<double>(end - begin).count();
}

double Percentile(std::vector<double> values, double q)
{
    if (values.empty()) {
        return 0;
    }
    std::sort(values.begin(), values.end());
    size_t idx = static_cast<size_t>(values.size() * q);
    if (idx >= values.size()) {
        idx = values.size() - 1;
    }
    return values[idx];
}

void WriteJsonField(std::ofstream &out, const std::string &name, const std::string &value, bool comma = true)
{
    out << "  \"" << name << "\": " << value << (comma ? "," : "") << "\n";
}

std::string Quote(const std::string &value)
{
    return "\"" + value + "\"";
}

int ParseInt(const char *value)
{
    return static_cast<int>(std::strtol(value, nullptr, 10));
}

Options ParseArgs(int argc, char **argv)
{
    Options opt;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> const char * {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for " + arg);
            }
            return argv[++i];
        };
        if (arg == "--mode") {
            opt.mode = next();
        } else if (arg == "--dir") {
            opt.dir = next();
        } else if (arg == "--output") {
            opt.output = next();
        } else if (arg == "--files") {
            opt.files = ParseInt(next());
        } else if (arg == "--file-size") {
            opt.fileSize = ParseInt(next());
        } else if (arg == "--window") {
            opt.window = ParseInt(next());
        } else if (arg == "--unlink-files") {
            opt.unlinkFiles = ParseInt(next());
        } else if (arg == "--wait-sec") {
            opt.waitSec = ParseInt(next());
        } else if (arg == "--workers") {
            opt.workers = ParseInt(next());
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }
    if (opt.mode.empty() || opt.dir.empty() || opt.output.empty() || opt.files <= 0 || opt.fileSize <= 0) {
        throw std::runtime_error("required: --mode --dir --output --files --file-size");
    }
    if (opt.unlinkFiles <= 0) {
        opt.unlinkFiles = opt.files;
    }
    if (opt.workers <= 0) {
        opt.workers = 1;
    }
    return opt;
}

std::string PathJoin(const std::string &dir, int i)
{
    return dir + "/f_" + std::to_string(100000000 + i).substr(1);
}

struct ParallelResult {
    std::vector<double> latencies;
    int errors = 0;
    double elapsedSec = 0;
};

int WorkerCount(const Options &opt, int total)
{
    if (total <= 0) {
        return 1;
    }
    return std::max(1, std::min(opt.workers, total));
}

double Rate(size_t count, double elapsed)
{
    return elapsed > 0 ? static_cast<double>(count) / elapsed : 0;
}

std::vector<double> MergeLatencies(const std::vector<std::vector<double>> &perWorker)
{
    size_t total = 0;
    for (const auto &latencies : perWorker) {
        total += latencies.size();
    }
    std::vector<double> merged;
    merged.reserve(total);
    for (const auto &latencies : perWorker) {
        merged.insert(merged.end(), latencies.begin(), latencies.end());
    }
    return merged;
}

ParallelResult RunParallel(int total, int workers, const std::function<int(int)> &operation)
{
    workers = std::max(1, std::min(workers, std::max(1, total)));
    std::vector<std::vector<double>> perWorker(workers);
    std::vector<std::thread> threads;
    threads.reserve(workers);
    std::atomic<int> nextIndex{0};
    std::atomic<int> errors{0};
    std::atomic<bool> start{false};

    for (int w = 0; w < workers; ++w) {
        threads.emplace_back([&, w]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (errors.load(std::memory_order_relaxed) == 0) {
                int i = nextIndex.fetch_add(1, std::memory_order_relaxed);
                if (i >= total) {
                    break;
                }
                auto opBegin = Clock::now();
                int ret = operation(i);
                if (ret != 0) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                perWorker[w].push_back(ElapsedSec(opBegin, Clock::now()));
            }
        });
    }

    auto begin = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }
    auto end = Clock::now();

    ParallelResult result;
    result.latencies = MergeLatencies(perWorker);
    result.errors = errors.load(std::memory_order_relaxed);
    result.elapsedSec = ElapsedSec(begin, end);
    return result;
}

void EnsureDir(const std::string &dir)
{
    std::string current;
    for (char ch : dir) {
        current.push_back(ch);
        if (ch == '/' && current.size() > 1) {
            FalconMkdir(current.substr(0, current.size() - 1));
        }
    }
    FalconMkdir(dir);
}

int CreateWriteClose(const std::string &path, const std::vector<char> &payload)
{
    uint64_t fd = 0;
    struct stat st;
    std::memset(&st, 0, sizeof(st));
    int ret = FalconCreate(path, fd, O_CREAT | O_RDWR, &st);
    if (ret != 0) {
        return ret;
    }
    ret = FalconWrite(fd, path, payload.data(), payload.size(), 0);
    if (ret != 0) {
        FalconClose(path, fd, false, -1);
        return ret;
    }
    ret = FalconClose(path, fd, true, -1);
    if (ret != 0) {
        FalconClose(path, fd, false, -1);
        return ret;
    }
    return FalconClose(path, fd, false, -1);
}

void StartFalcon()
{
    int ret = GetInit().Init();
    if (ret != 0) {
        throw std::runtime_error("GetInit().Init failed: " + std::to_string(ret));
    }
    auto &config = GetInit().GetFalconConfig();
    falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
    std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);
    int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
    size_t start = 0;
    for (int i = 0; i <= nodeId; ++i) {
        size_t comma = clusterView.find(',', start);
        std::string endpoint = clusterView.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        if (i == nodeId) {
            server.endPoint = endpoint;
            break;
        }
        start = comma + 1;
    }
    g_brpcServerThread = std::make_unique<std::thread>(&falcon::brpc_io::RemoteIOServer::Run, &server);
    {
        std::unique_lock<std::mutex> lk(server.mutexStart);
        server.cvStart.wait(lk, [&server]() { return server.isStarted; });
    }

    std::string serverIp = config->GetString(FalconPropertyKey::FALCON_SERVER_IP);
    std::string serverPort = config->GetString(FalconPropertyKey::FALCON_SERVER_PORT);
    ret = FalconInit(serverIp, std::stoi(serverPort));
    if (ret != 0) {
        server.Stop();
        if (g_brpcServerThread != nullptr && g_brpcServerThread->joinable()) {
            g_brpcServerThread->join();
        }
        throw std::runtime_error("FalconInit failed: " + std::to_string(ret));
    }
    server.SetReadyFlag();
}

void StopFalcon()
{
    falcon::brpc_io::RemoteIOServer::GetInstance().Stop();
    if (g_brpcServerThread != nullptr && g_brpcServerThread->joinable()) {
        g_brpcServerThread->join();
    }
    FalconDestroy();
}


void WriteLatencyStats(std::ofstream &out, const std::string &prefix, const std::vector<double> &latencies)
{
    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double avg = latencies.empty() ? 0 : sum / latencies.size();
    double maxValue = latencies.empty() ? 0 : *std::max_element(latencies.begin(), latencies.end());
    WriteJsonField(out, prefix + "_avg_sec", std::to_string(avg));
    WriteJsonField(out, prefix + "_p50_sec", std::to_string(Percentile(latencies, 0.50)));
    WriteJsonField(out, prefix + "_p95_sec", std::to_string(Percentile(latencies, 0.95)));
    WriteJsonField(out, prefix + "_p99_sec", std::to_string(Percentile(latencies, 0.99)));
    WriteJsonField(out, prefix + "_max_sec", std::to_string(maxValue));
}

void RunWriteOnly(const Options &opt, const std::vector<char> &payload)
{
    int workers = WorkerCount(opt, opt.files);
    auto result = RunParallel(opt.files, workers, [&](int i) {
        return CreateWriteClose(PathJoin(opt.dir, i), payload);
    });
    if (opt.waitSec > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(opt.waitSec));
    }
    std::ofstream out(opt.output);
    out << "{\n";
    WriteJsonField(out, "mode", Quote(opt.mode));
    WriteJsonField(out, "dir", Quote(opt.dir));
    WriteJsonField(out, "workers", std::to_string(workers));
    WriteJsonField(out, "files", std::to_string(opt.files));
    WriteJsonField(out, "file_size_bytes", std::to_string(opt.fileSize));
    WriteJsonField(out, "written_files", std::to_string(result.latencies.size()));
    WriteJsonField(out, "elapsed_sec", std::to_string(result.elapsedSec));
    WriteJsonField(out, "throughput_files_per_sec", std::to_string(Rate(result.latencies.size(), result.elapsedSec)));
    WriteJsonField(out, "throughput_mib_per_sec", std::to_string(Rate(result.latencies.size(), result.elapsedSec) * opt.fileSize / 1048576.0));
    WriteLatencyStats(out, "write_latency", result.latencies);
    WriteJsonField(out, "error_count", std::to_string(result.errors), false);
    out << "}\n";
}

void RunUnlinkOnly(const Options &opt, const std::vector<char> &payload)
{
    int workers = WorkerCount(opt, opt.files);
    auto createResult = RunParallel(opt.files, workers, [&](int i) {
        return CreateWriteClose(PathJoin(opt.dir, i), payload);
    });
    auto unlinkResult = RunParallel(static_cast<int>(createResult.latencies.size()), workers, [&](int i) {
        return FalconUnlink(PathJoin(opt.dir, i));
    });
    std::ofstream out(opt.output);
    out << "{\n";
    WriteJsonField(out, "mode", Quote(opt.mode));
    WriteJsonField(out, "dir", Quote(opt.dir));
    WriteJsonField(out, "workers", std::to_string(workers));
    WriteJsonField(out, "files", std::to_string(opt.files));
    WriteJsonField(out, "file_size_bytes", std::to_string(opt.fileSize));
    WriteJsonField(out, "created_files", std::to_string(createResult.latencies.size()));
    WriteJsonField(out, "unlinked_files", std::to_string(unlinkResult.latencies.size()));
    WriteJsonField(out, "create_elapsed_sec", std::to_string(createResult.elapsedSec));
    WriteJsonField(out, "unlink_elapsed_sec", std::to_string(unlinkResult.elapsedSec));
    WriteJsonField(out, "create_files_per_sec", std::to_string(Rate(createResult.latencies.size(), createResult.elapsedSec)));
    WriteJsonField(out, "unlink_files_per_sec", std::to_string(Rate(unlinkResult.latencies.size(), unlinkResult.elapsedSec)));
    WriteLatencyStats(out, "create_latency", createResult.latencies);
    WriteLatencyStats(out, "unlink_latency", unlinkResult.latencies);
    WriteJsonField(out, "error_count", std::to_string(createResult.errors + unlinkResult.errors), false);
    out << "}\n";
}

void RunCreateUnlink(const Options &opt, const std::vector<char> &payload)
{
    std::vector<double> createLatencies;
    std::vector<double> unlinkLatencies;
    std::vector<std::string> pending;
    int errors = 0;
    auto begin = Clock::now();
    for (int i = 0; i < opt.files; ++i) {
        auto path = PathJoin(opt.dir, i);
        auto opBegin = Clock::now();
        int ret = CreateWriteClose(path, payload);
        if (ret != 0) {
            ++errors;
            break;
        }
        createLatencies.push_back(ElapsedSec(opBegin, Clock::now()));
        pending.push_back(path);
        if (static_cast<int>(pending.size()) > opt.window) {
            auto old = pending.front();
            pending.erase(pending.begin());
            opBegin = Clock::now();
            ret = FalconUnlink(old);
            if (ret != 0) {
                ++errors;
                break;
            }
            unlinkLatencies.push_back(ElapsedSec(opBegin, Clock::now()));
        }
    }
    auto tailBegin = Clock::now();
    while (!pending.empty() && errors == 0) {
        auto old = pending.front();
        pending.erase(pending.begin());
        auto opBegin = Clock::now();
        int ret = FalconUnlink(old);
        if (ret != 0) {
            ++errors;
            break;
        }
        unlinkLatencies.push_back(ElapsedSec(opBegin, Clock::now()));
    }
    auto end = Clock::now();
    double elapsed = ElapsedSec(begin, end);
    std::ofstream out(opt.output);
    out << "{\n";
    WriteJsonField(out, "mode", Quote(opt.mode));
    WriteJsonField(out, "dir", Quote(opt.dir));
    WriteJsonField(out, "files", std::to_string(opt.files));
    WriteJsonField(out, "file_size_bytes", std::to_string(opt.fileSize));
    WriteJsonField(out, "window", std::to_string(opt.window));
    WriteJsonField(out, "created_files", std::to_string(createLatencies.size()));
    WriteJsonField(out, "unlinked_files", std::to_string(unlinkLatencies.size()));
    WriteJsonField(out, "mixed_elapsed_sec", std::to_string(elapsed));
    WriteJsonField(out, "tail_cleanup_elapsed_sec", std::to_string(ElapsedSec(tailBegin, end)));
    WriteJsonField(out, "create_files_per_sec", std::to_string(createLatencies.size() / elapsed));
    WriteJsonField(out, "unlink_files_per_sec", std::to_string(unlinkLatencies.size() / elapsed));
    WriteJsonField(out, "total_ops_per_sec", std::to_string((createLatencies.size() + unlinkLatencies.size()) / elapsed));
    WriteLatencyStats(out, "create_latency", createLatencies);
    WriteLatencyStats(out, "unlink_latency", unlinkLatencies);
    WriteJsonField(out, "error_count", std::to_string(errors), false);
    out << "}\n";
}


void RunConcurrentUnlink(const Options &opt, const std::vector<char> &payload)
{
    Options writeOpt = opt;
    Options deleteOpt = opt;
    writeOpt.dir = opt.dir + "/write";
    deleteOpt.dir = opt.dir + "/delete";
    EnsureDir(writeOpt.dir);
    EnsureDir(deleteOpt.dir);

    int workers = WorkerCount(opt, std::max(opt.files, opt.unlinkFiles));
    auto prepareResult = RunParallel(opt.unlinkFiles, workers, [&](int i) {
        return CreateWriteClose(PathJoin(deleteOpt.dir, i), payload);
    });

    std::vector<std::vector<double>> writePerWorker(workers);
    std::vector<std::vector<double>> unlinkPerWorker(workers);
    std::vector<std::thread> threads;
    threads.reserve(workers * 2);
    std::vector<Clock::time_point> writerBegins(workers);
    std::vector<Clock::time_point> writerEnds(workers);
    std::vector<Clock::time_point> deleterBegins(workers);
    std::vector<Clock::time_point> deleterEnds(workers);
    std::atomic<int> nextWrite{0};
    std::atomic<int> nextDelete{0};
    std::atomic<int> errors{prepareResult.errors};
    std::atomic<bool> start{false};
    int deleteTotal = static_cast<int>(prepareResult.latencies.size());

    for (int w = 0; w < workers; ++w) {
        threads.emplace_back([&, w]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            writerBegins[w] = Clock::now();
            while (errors.load(std::memory_order_relaxed) == 0) {
                int i = nextWrite.fetch_add(1, std::memory_order_relaxed);
                if (i >= opt.files) {
                    break;
                }
                auto opBegin = Clock::now();
                int ret = CreateWriteClose(PathJoin(writeOpt.dir, i), payload);
                if (ret != 0) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                writePerWorker[w].push_back(ElapsedSec(opBegin, Clock::now()));
            }
            writerEnds[w] = Clock::now();
        });
        threads.emplace_back([&, w]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            deleterBegins[w] = Clock::now();
            while (errors.load(std::memory_order_relaxed) == 0) {
                int i = nextDelete.fetch_add(1, std::memory_order_relaxed);
                if (i >= deleteTotal) {
                    break;
                }
                auto opBegin = Clock::now();
                int ret = FalconUnlink(PathJoin(deleteOpt.dir, i));
                if (ret != 0) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                    break;
                }
                unlinkPerWorker[w].push_back(ElapsedSec(opBegin, Clock::now()));
            }
            deleterEnds[w] = Clock::now();
        });
    }

    auto mixedBegin = Clock::now();
    start.store(true, std::memory_order_release);
    for (auto &thread : threads) {
        thread.join();
    }
    auto mixedEnd = Clock::now();

    auto elapsedWindow = [](const std::vector<Clock::time_point> &begins, const std::vector<Clock::time_point> &ends) {
        auto minBegin = *std::min_element(begins.begin(), begins.end());
        auto maxEnd = *std::max_element(ends.begin(), ends.end());
        return ElapsedSec(minBegin, maxEnd);
    };

    auto writeLatencies = MergeLatencies(writePerWorker);
    auto unlinkLatencies = MergeLatencies(unlinkPerWorker);
    double mixedElapsed = ElapsedSec(mixedBegin, mixedEnd);
    double writerElapsed = elapsedWindow(writerBegins, writerEnds);
    double deleterElapsed = elapsedWindow(deleterBegins, deleterEnds);
    std::ofstream out(opt.output);
    out << "{\n";
    WriteJsonField(out, "mode", Quote(opt.mode));
    WriteJsonField(out, "dir", Quote(opt.dir));
    WriteJsonField(out, "write_dir", Quote(writeOpt.dir));
    WriteJsonField(out, "delete_dir", Quote(deleteOpt.dir));
    WriteJsonField(out, "workers", std::to_string(workers));
    WriteJsonField(out, "files", std::to_string(opt.files));
    WriteJsonField(out, "unlink_files", std::to_string(opt.unlinkFiles));
    WriteJsonField(out, "file_size_bytes", std::to_string(opt.fileSize));
    WriteJsonField(out, "prepared_delete_files", std::to_string(prepareResult.latencies.size()));
    WriteJsonField(out, "prepare_elapsed_sec", std::to_string(prepareResult.elapsedSec));
    WriteJsonField(out, "written_files", std::to_string(writeLatencies.size()));
    WriteJsonField(out, "unlinked_files", std::to_string(unlinkLatencies.size()));
    WriteJsonField(out, "mixed_elapsed_sec", std::to_string(mixedElapsed));
    WriteJsonField(out, "writer_elapsed_sec", std::to_string(writerElapsed));
    WriteJsonField(out, "deleter_elapsed_sec", std::to_string(deleterElapsed));
    WriteJsonField(out, "write_files_per_sec_by_writer", std::to_string(Rate(writeLatencies.size(), writerElapsed)));
    WriteJsonField(out, "write_mib_per_sec_by_writer", std::to_string(Rate(writeLatencies.size(), writerElapsed) * opt.fileSize / 1048576.0));
    WriteJsonField(out, "unlink_files_per_sec_by_deleter", std::to_string(Rate(unlinkLatencies.size(), deleterElapsed)));
    WriteJsonField(out, "unlink_mib_per_sec_by_deleter", std::to_string(Rate(unlinkLatencies.size(), deleterElapsed) * opt.fileSize / 1048576.0));
    WriteJsonField(out, "write_files_per_sec_by_mixed_window", std::to_string(Rate(writeLatencies.size(), mixedElapsed)));
    WriteJsonField(out, "unlink_files_per_sec_by_mixed_window", std::to_string(Rate(unlinkLatencies.size(), mixedElapsed)));
    WriteLatencyStats(out, "prepare_latency", prepareResult.latencies);
    WriteLatencyStats(out, "write_latency", writeLatencies);
    WriteLatencyStats(out, "unlink_latency", unlinkLatencies);
    WriteJsonField(out, "error_count", std::to_string(errors.load(std::memory_order_relaxed)), false);
    out << "}\n";
}

} // namespace

int main(int argc, char **argv)
{
    try {
        Options opt = ParseArgs(argc, argv);
        setenv("CONFIG_FILE", "/usr/local/falconfs/falcon_client/config/config.json", 0);
        StartFalcon();
        EnsureDir(opt.dir);
        std::vector<char> payload(opt.fileSize, 'a');
        if (opt.mode == "idle_server") {
            if (opt.waitSec <= 0) {
                opt.waitSec = 3600;
            }
            std::ofstream out(opt.output);
            out << "{\n";
            WriteJsonField(out, "mode", Quote(opt.mode));
            WriteJsonField(out, "wait_sec", std::to_string(opt.waitSec), false);
            out << "}\n";
            out.close();
            std::this_thread::sleep_for(std::chrono::seconds(opt.waitSec));
        } else if (opt.mode == "write_only" || opt.mode == "create_evict") {
            RunWriteOnly(opt, payload);
        } else if (opt.mode == "unlink_only") {
            RunUnlinkOnly(opt, payload);
        } else if (opt.mode == "create_unlink") {
            RunCreateUnlink(opt, payload);
        } else if (opt.mode == "concurrent_unlink") {
            RunConcurrentUnlink(opt, payload);
        } else {
            throw std::runtime_error("unknown mode: " + opt.mode);
        }
        std::_Exit(0);
    } catch (const std::exception &e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
}
