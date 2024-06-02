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

namespace ss_table {
    std::unique_ptr<SSTable> SSTable::FromFile(const std::string &file_name)
    {
        std::ifstream fin;
        fin.open(file_name);
        if(!fin) {
            LOG_ERROR("Read SSTable file `%s` error", file_name.c_str());
            return nullptr;
        }

        std::unique_ptr<SSTable> new_ss_table_uptr(new SSTable());
        fin.read(reinterpret_cast<char*>(&new_ss_table_uptr.get()->header_), sizeof(Header));
        
        new_ss_table_uptr.get()->bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE);
        new_ss_table_uptr.get()->bloom_filter_->ReadFromFile(fin);

        auto key_count = new_ss_table_uptr.get()->header_.key_count;
        for(uint64_t i = 0;i < key_count; ++i) {
            new_ss_table_uptr.get()->key_offset_vlen_tuple_list_.emplace_back(&fin);
        }
        fin.close();
        new_ss_table_uptr.get()->file_name_ = file_name;

        return new_ss_table_uptr;
    }
    
    std::unique_ptr<SSTable> SSTable::NewSSTable(const std::string &file_name, uint64_t time_stamp, const std::vector<KeyOffsetVlenTuple> &inserted_tuples)
    {
        std::unique_ptr<SSTable> new_ss_table_uptr(new SSTable());
        new_ss_table_uptr.get()->bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE);
        
        uint64_t min_key = std::numeric_limits<uint64_t>::max(),
                 max_key = std::numeric_limits<uint64_t>::min();
        for(const auto &tuple: inserted_tuples) {
            min_key = tuple.key < min_key ? tuple.key : min_key;
            max_key = tuple.key > max_key ? tuple.key : max_key;

            new_ss_table_uptr.get()->bloom_filter_->Insert(tuple.key);

            new_ss_table_uptr.get()->key_offset_vlen_tuple_list_.push_back(tuple);
        }
        new_ss_table_uptr.get()->header_ = {time_stamp, inserted_tuples.size(), min_key, max_key};
        new_ss_table_uptr.get()->file_name_ = file_name;
        return new_ss_table_uptr;
    }


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
        const std::vector<std::unique_ptr<SSTable>> &ss_table_list
    ) {
        auto cmp = [](const TimeStampedKeyOffsetVlenTuple &a, const TimeStampedKeyOffsetVlenTuple &b) {
            return a.key_offset_vlen_tuple.key < b.key_offset_vlen_tuple.key 
                    || (a.key_offset_vlen_tuple.key == b.key_offset_vlen_tuple.key && a.time_stamp > b.time_stamp);
        };
        std::priority_queue<
            TimeStampedKeyOffsetVlenTuple,
            std::vector<TimeStampedKeyOffsetVlenTuple>,
            decltype(cmp)
        > pq(cmp);

        for(const auto &ss_table: ss_table_list) {
            for(const auto &tuple: ss_table->key_offset_vlen_tuple_list()) {
                pq.emplace(ss_table->header().time_stamp, tuple);
            }
        }

        std::vector<TimeStampedKeyOffsetVlenTuple> merged_results;
        while(!pq.empty()) {
            if(!merged_results.empty() && pq.top().key_offset_vlen_tuple.key == merged_results.back().key_offset_vlen_tuple.key) {
                // 时间戳相等时，取最大的时间戳
                pq.pop();
                continue;
            }

            merged_results.push_back(pq.top());
            pq.pop();
        }

        return merged_results;
    }



    std::string SSTable::BuildSSTableFileName(const std::string &dir, int level, const std::string &base_name) {
        return dir + "/level-" + std::to_string(level) + "/" + base_name;
    }
    std::string SSTable::BuildSSTableDirName(const std::string &dir, int level) {
        return dir + "/level-" + std::to_string(level);
    }


}
