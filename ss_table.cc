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
        bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE, 4);
        header_.time_stamp = time_stamp;
    }

    SSTable::~SSTable() {
        delete bloom_filter_;
    }

    void SSTable::WriteToFile(const std::string &file_name) const {
        std::ofstream fout;
        fout.open(file_name , std::ios::out | std::ios::binary);
        fout.write((char*) &header_, sizeof(Header));

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

    void SSTable::InsertKeyOffsetVlenTuple(uint64_t key, uint64_t offset, uint32_t v_len) {
        key_offset_vlen_tuple_list_.push_back({key, offset, v_len});
    }

    void SSTable::InsertKeyToBloomFilter(uint64_t key) {
        bloom_filter_->Insert(key);
    }

}
