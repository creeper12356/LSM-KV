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
using namespace std::chrono;
void do_regular_test(KVStoreAPI &store, int num_operations)
{

    LOG_INFO("=== Regular test ===");
    store.reset();

    LOG_INFO("Regular PUT test");
    auto start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i)
    {
        store.put(i, "value" + std::to_string(i));
    }
    auto end_time = high_resolution_clock::now();
    duration<double> elapsed = end_time - start_time;
    double throughput = num_operations / elapsed.count();
    double avg_latency = elapsed.count() / num_operations;
    LOG_INFO("PUT Throughput: %f KOs/sec, Avg Latency: %f ms/op", throughput / 1000, avg_latency * 1000);

    LOG_INFO("Regular GET test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i)
    {
        store.get(i);
    }
    end_time = high_resolution_clock::now();
    elapsed = end_time - start_time;
    throughput = num_operations / elapsed.count();
    avg_latency = elapsed.count() / num_operations;
    LOG_INFO("GET Throughput: %f KOps/sec, Avg Latency: %f ms/op", throughput / 1000, avg_latency * 1000);

    LOG_INFO("Regular SCAN test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i)
    {
        std::list<std::pair<uint64_t, std::string>> list;
        store.scan(i, i + 100, list);
    }
    end_time = high_resolution_clock::now();
    elapsed = end_time - start_time;
    throughput = num_operations / elapsed.count();
    avg_latency = elapsed.count() / num_operations;
    LOG_INFO("SCAN Throughput: %f KOps/sec, Avg Latency: %f ms/op", throughput / 1000, avg_latency * 1000);

    LOG_INFO("Regular DEL test");
    start_time = high_resolution_clock::now();
    for (int i = 0; i < num_operations; ++i)
    {
        store.del(i);
    }
    end_time = high_resolution_clock::now();
    elapsed = end_time - start_time;
    throughput = num_operations / elapsed.count();
    avg_latency = elapsed.count() / num_operations;
    LOG_INFO("DEL Throughput: %f KOps/sec, Avg Latency: %f ms/op", throughput / 1000, avg_latency * 1000);

    LOG_INFO("=== Regular test finished ===");
}

void do_compaction_test(KVStoreAPI &store, size_t duration_seconds)
{
    LOG_INFO("=== Compaction test ===");
    store.reset();
    std::vector<size_t> throughput;
    auto start_time = std::chrono::steady_clock::now();
    size_t put_count = 0;

    for (size_t i = 0; i < duration_seconds; ++i) {
        auto next_second = start_time + std::chrono::seconds(i + 1);
        while (std::chrono::steady_clock::now() < next_second) {
            store.put(i * 1000000 + put_count, "value");
            ++put_count;
        }
        throughput.push_back(put_count);
        put_count = 0;
    }


    for(size_t i = 0; i < throughput.size(); ++i) {
        LOG_INFO("Second %zu: %zu PUT requests", i, throughput[i]);
    }

    LOG_INFO("=== Compaction test finished ===");
}
int main()
{
    int regular_test_num_operations = 100 * 1024;
    int compaction_test_duration_seconds = 60;
    KVStore store("data", "data/vlog");
    do_regular_test(store, regular_test_num_operations);
    do_compaction_test(store, compaction_test_duration_seconds);
    return 0;
}
