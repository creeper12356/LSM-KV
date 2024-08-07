//
// Created by creeper on 24-4-3.
//

#include "ss_table.h"
#include "bloom_filter.h"
#include "skip_list.h"
#include "inc.h"
#include "utils/logger.h"
#include <iostream>
#include <fstream>
#include <limits>
#include <vector>
#include <queue>

#include <chrono>
namespace ss_table {
    SSTable::~SSTable() {
        delete bloom_filter_;
    }

    void SSTable::WriteToFile() const {
        std::ofstream fout;
        fout.open(file_name_ , std::ios::out | std::ios::binary);
        fout.write(reinterpret_cast<const char*> (&header_), sizeof(Header));

        bloom_filter_->WriteToFile(fout);

        for(const auto& tuple: key_offset_vlen_tuple_list_) {
            // tuple结构体末尾存在padding, sizeof(KeyOffsetVlenTuple) == 24，此处写入20字节即可
            fout.write(reinterpret_cast<const char*> (&tuple), 20);
        }
        fout.close();
    }

    void SSTable::set_header(uint64_t time_stamp, uint64_t key_count, uint64_t min_key, uint64_t max_key) {
        header_.time_stamp = time_stamp;
        header_.key_count = key_count;
        header_.min_key = min_key;
        header_.max_key = max_key;
    }

    std::optional<SSTableGetResult> SSTable::Get(uint64_t key) const
    {
        if(key > header_.max_key || key < header_.min_key) {
            return std::nullopt;
        }
        if(!bloom_filter_->Search(key)) {
            return std::nullopt;
        }

        // 二分查找元组列表
        SSTableGetResult result;
        uint64_t lh = 0, rh = header_.key_count - 1, mid;
        while(lh <= rh) {
            mid = (lh + rh) / 2;
            if(key == key_offset_vlen_tuple_list_[mid].key) {
                result.offset = key_offset_vlen_tuple_list_[mid].offset;
                result.vlen = key_offset_vlen_tuple_list_[mid].vlen;
                return result;
            } else if(key < key_offset_vlen_tuple_list_[mid].key) {
                rh = mid - 1;
            } else {
                lh = mid + 1;
            }
        }
        
        return std::nullopt;
    }

    const Header &SSTable::header() const {
        return header_;
    }
    const std::vector<KeyOffsetVlenTuple> &SSTable::key_offset_vlen_tuple_list() const {
        return key_offset_vlen_tuple_list_;
    }
    const std::string &SSTable::file_name() const {
        return file_name_;
    }


    std::vector<TimeStampedKeyOffsetVlenTuple> SSTable::MergeSSTables(
        const std::vector<std::shared_ptr<SSTable>> &ss_table_list
    ) {
        auto cmp = [](const TimeStampedKeyOffsetVlenTuple &a, const TimeStampedKeyOffsetVlenTuple &b) {
        return a.key_offset_vlen_tuple.key > b.key_offset_vlen_tuple.key 
               || (a.key_offset_vlen_tuple.key == b.key_offset_vlen_tuple.key && a.time_stamp > b.time_stamp)
               || (a.key_offset_vlen_tuple.key == b.key_offset_vlen_tuple.key && a.time_stamp == b.time_stamp && a.ss_table_index < b.ss_table_index);
        };
        std::priority_queue<
            TimeStampedKeyOffsetVlenTuple,
            std::vector<TimeStampedKeyOffsetVlenTuple>,
            decltype(cmp)
        > pq(cmp);

        size_t ss_table_index = 0;
        for (const auto &ss_table : ss_table_list) {
            for (const auto &tuple : ss_table->key_offset_vlen_tuple_list()) {
                pq.emplace(ss_table->header().time_stamp, tuple, ss_table_index);
            }
            ++ ss_table_index;
        }

        std::vector<TimeStampedKeyOffsetVlenTuple> merged_results;
        while (!pq.empty()) {
            auto current = pq.top();
            pq.pop();

            // 取出key相同的元组中，优先级最低的元组
            while (!pq.empty() && pq.top().key_offset_vlen_tuple.key == current.key_offset_vlen_tuple.key) {
                current = pq.top();
                pq.pop();
            }

            merged_results.push_back(current);
        }

        return merged_results;
    }

    Header SSTable::ReadSSTableHeaderDirectly(const std::string &ss_table_file_name) {
        std::ifstream fin;
        fin.open(ss_table_file_name);
        if(!fin) {
            LOG_ERROR("Read SSTable file header `%s` error", ss_table_file_name.c_str());
            return {0, 0, 0, 0};
        }

        Header header;
        fin.read(reinterpret_cast<char*>(&header), sizeof(Header));
        fin.close();
        return header;
    }

    int SSTable::SSTableMaxCountAtLevel(int level) {
        return 2 << level;
    }



    std::string SSTable::BuildSSTableFileName(const std::string &dir, int level, const std::string &base_name) {
        return dir + "/level-" + std::to_string(level) + "/" + base_name;
    }
    std::string SSTable::BuildSSTableDirName(const std::string &dir, int level) {
        return dir + "/level-" + std::to_string(level);
    }

    std::string SSTable::BuildUniqueSSTableFileName(const std::string &dir, int level) {
        return dir + "/level-" + std::to_string(level) + "/" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + ".sst";
    }


}
