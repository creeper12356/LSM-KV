//
// Created by creeper on 24-4-3.
//

#include "ss_table.h"
#include "bloom_filter.h"
#include "skip_list.h"
#include "inc.h"
#include <iostream>
#include <fstream>

namespace ss_table {

    SSTable::SSTable(uint64_t time_stamp, const skip_list::SkipList& mem_table) {
        bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE);
        header_.time_stamp = time_stamp;
    }

    SSTable::~SSTable() {
        delete bloom_filter_;
    }

    bool SSTable::ReadFromFile(const std::string &file_name) {
        std::ifstream fin;
        fin.open(file_name);
        if(!fin) {
            return false;
        }
        fin.read(reinterpret_cast<char*>(&header_), sizeof(Header));
        bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE);

        bloom_filter_->ReadFromFile(fin);

        for(int i = 0;i < header_.key_count; ++i) {
            KeyOffsetVlenTuple tuple = {};
            fin.read(reinterpret_cast<char*>(&tuple), 20);
            key_offset_vlen_tuple_list_.push_back(tuple);
        }

        fin.close();
        return true;
    }
    void SSTable::WriteToFile(const std::string &file_name) const {
        std::ofstream fout;
        fout.open(file_name , std::ios::out | std::ios::binary);
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

    void SSTable::InsertKeyOffsetVlenTuple(uint64_t key, uint64_t offset, uint32_t vlen) {
        key_offset_vlen_tuple_list_.push_back({key, offset, vlen});
    }

    void SSTable::InsertKeyToBloomFilter(uint64_t key) {
        bloom_filter_->Insert(key);
    }

    SSTable::SSTable() {

    }

    bool SSTable::Get(uint64_t key, uint64_t& offset, uint32_t& vlen) const {
        if(key > header_.max_key || key < header_.min_key) {
            return false;
        }
        if(!bloom_filter_->Search(key)) {
            return false;
        }

        // 二分查找元组列表
        uint64_t lh = 0, rh = header_.key_count - 1, mid;
        while(lh <= rh) {
            mid = (lh + rh) / 2;
            if(key == key_offset_vlen_tuple_list_[mid].key) {
                offset = key_offset_vlen_tuple_list_[mid].offset;
                vlen = key_offset_vlen_tuple_list_[mid].vlen;
                return true;
            } else if(key < key_offset_vlen_tuple_list_[mid].key) {
                rh = mid - 1;
            } else {
                lh = mid + 1;
            }
        }
        return false;
    }


}
