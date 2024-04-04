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
        virtual ~SSTable();
    public:
        /**
         * @brief 设置SSTable的Header部分
         * @param time_stamp 时间戳
         * @param key_count 键总数
         * @param min_key 最小的键
         * @param max_key 最大的键
         */
        void set_header(uint64_t time_stamp, uint64_t key_count, uint64_t min_key, uint64_t max_key);

        /**
         * @brief 向Bloom过滤器中插入键
         * @param key 插入的键
         */
        void InsertKeyToBloomFilter(uint64_t key);

        /**
         * @brief 向SSTable中插入一个<Key,Offset,Vlen> 元组
         * @param key 键
         * @param offset VLog文件中的偏移量
         * @param v_len 值的长度
         */
        void InsertKeyOffsetVlenTuple(uint64_t key, uint64_t offset, uint32_t v_len);
        void WriteToFile(const std::string &file_name) const;

    private:
        Header header_;
        bloom_filter::BloomFilter* bloom_filter_;
        std::list<KeyOffsetVlenTuple> key_offset_vlen_tuple_list_;
    };
}



#endif //LSMKV_HANDOUT_SS_TABLE_H
