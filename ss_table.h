//
// Created by creeper on 24-4-3.
//

#ifndef LSMKV_HANDOUT_SS_TABLE_H
#define LSMKV_HANDOUT_SS_TABLE_H
#include <string>
#include <list>
namespace skip_list {
    class SkipList;
}
namespace bloom_filter {
    class BloomFilter;
}

namespace ss_table {
    struct Header {
        uint64_t time_stamp;
        uint64_t key_count;
        uint64_t min_key;
        uint64_t max_key;
    };
    struct KeyOffsetVlenTuple {
        uint64_t key;
        uint64_t offset;
        uint32_t v_len;
    };
    class SSTable {
    public:
        explicit SSTable(uint64_t time_stamp, const skip_list::SkipList& mem_table);
    public:
        void WriteToFile(const std::string &file_name) const;

        virtual ~SSTable();

    private:
        Header header_;
        bloom_filter::BloomFilter* bloom_filter_;
        std::list<KeyOffsetVlenTuple> key_offset_vlen_tuple_list_;
    };
}



#endif //LSMKV_HANDOUT_SS_TABLE_H
