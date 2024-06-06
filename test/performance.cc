/**
 * @file performance.cc
 * @author creeper 
 * @brief LSM-KVStore 性能测试
 * @version 0.1
 * @date 2024-06-06
 * 
 * @copyright Copyright (c) 2024
 * 
 */
#include <iostream>
#include <chrono>
#include <string>
#include <list>
#define ENABLE_LOG
#include "../utils/logger.h"
#include "../kvstore.h"
void test_operations(KVStoreAPI &store, int num_operations) {
    using namespace std::chrono;
    store.reset();

    LOG_INFO("=== Regular test ===");

    LOG_INFO("Regular PUT test");
    auto start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i) {
        store.put(i, "value" + std::to_string(i));
    }
    auto end_time = high_resolution_clock::now();
    duration<double> elapsed = end_time - start_time;
    double throughput = num_operations / elapsed.count();
    double avg_latency = elapsed.count() / num_operations;
    LOG_INFO("PUT Throughput: %f ops/sec, Avg Latency: %f sec/op", throughput, avg_latency);

    LOG_INFO("Regular GET test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i) {
        store.get(i);
    }
    end_time = high_resolution_clock::now();
    elapsed = end_time - start_time;
    throughput = num_operations / elapsed.count();
    avg_latency = elapsed.count() / num_operations;
    LOG_INFO("GET Throughput: %f ops/sec, Avg Latency: %f sec/op", throughput, avg_latency);

    LOG_INFO("Regular SCAN test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i) {
        std::list<std::pair<uint64_t, std::string>> list;
        store.scan(i, i + 100, list);
    }
    end_time = high_resolution_clock::now();
    elapsed = end_time - start_time;
    throughput = num_operations / elapsed.count();
    avg_latency = elapsed.count() / num_operations;
    LOG_INFO("SCAN Throughput: %f ops/sec, Avg Latency: %f sec/op", throughput, avg_latency);

    LOG_INFO("Regular DEL test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i) {
        store.del(i);
    }
    end_time = high_resolution_clock::now();
    elapsed = end_time - start_time;
    throughput = num_operations / elapsed.count();
    avg_latency = elapsed.count() / num_operations;
    LOG_INFO("DEL Throughput: %f ops/sec, Avg Latency: %f sec/op", throughput, avg_latency);

    LOG_INFO("=== Regular test finished ===");

    LOG_INFO("=== Compaction test ===");

    LOG_INFO("Compaction PUT test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i) {
        store.put(i, "value" + std::to_string(i));
        if (i % 1000 == 0) {
            end_time = high_resolution_clock::now();
            elapsed = end_time - start_time;
            throughput = 1000 / elapsed.count();
            LOG_INFO("PUT Throughput: %f ops/sec", throughput);
            start_time = high_resolution_clock::now();
        }
    }

    LOG_INFO("=== Compaction test finished ===");
}

int main() {
    // 初始化存储
    KVStore store("data", "data/vlog");
    
    // 测试操作次数
    int num_operations = 1024;
    
    // 运行测试
    test_operations(store, num_operations);

    return 0;
}
