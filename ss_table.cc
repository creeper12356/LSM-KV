//
// Created by creeper on 24-4-3.
//

#include "ss_table.h"
#include "bloom_filter.h"
#include "skip_list.h"
#include <fstream>

namespace ss_table {

    SSTable::SSTable(uint64_t time_stamp, const skip_list::SkipList& mem_table) {
        bloom_filter_ = new bloom_filter::BloomFilter(8192, 4);
        header_.time_stamp = time_stamp;
        mem_table.InsertAllKeys(bloom_filter_, &header_);
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
            fout.write((char*) &tuple, sizeof(KeyOffsetVlenTuple));
        }
        fout.close();
    }
}
