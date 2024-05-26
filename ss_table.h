//
// Created by creeper on 24-4-3.
//

#ifndef LSMKV_HANDOUT_SS_TABLE_H
#define LSMKV_HANDOUT_SS_TABLE_H
#include <string>
#include <vector>
#include <optional>
#include <memory>
#include <fstream>

namespace skip_list
{
    class SkipList;
}
namespace bloom_filter
{
    class BloomFilter;
}

namespace ss_table
{
    struct SSTableGetResult
    {
        uint64_t offset;
        uint32_t vlen;
    };
    struct Header
    {
        uint64_t time_stamp;
        uint64_t key_count;
        uint64_t min_key;
        uint64_t max_key;
    };
    struct KeyOffsetVlenTuple
    {
        uint64_t key;
        uint64_t offset;
        uint32_t vlen;

        KeyOffsetVlenTuple(uint64_t key, uint64_t offset, uint32_t vlen) 
            : key(key), offset(offset), vlen(vlen) { }

        KeyOffsetVlenTuple(std::istream *is) {
            is->read(reinterpret_cast<char *>(&key), sizeof(uint64_t));
            is->read(reinterpret_cast<char *>(&offset), sizeof(uint64_t));
            is->read(reinterpret_cast<char *>(&vlen), sizeof(uint32_t));
        }
    };
    class SSTable
    {
    public:
        /**
         * @brief 从SSTable文件生成缓存
         * 
         * @param file_name 
         * @return std::unique_ptr<SSTable> 
         */
        static std::unique_ptr<SSTable> FromFile(const std::string &file_name);

        /**
         * @brief 创建新的SSTable缓存
         * 
         * @param time_stamp 
         * @param inserted_tuples 
         * @return std::unique_ptr<SSTable> 
         */
        static std::unique_ptr<SSTable> NewSSTable(uint64_t time_stamp, const std::vector<KeyOffsetVlenTuple> &inserted_tuples);
    
    private:
        SSTable() = default;
    public:
        ~SSTable();

    public:
        /**
         * @brief 设置SSTable的Header部分
         * @param time_stamp 时间戳
         * @param key_count 键总数
         * @param min_key 最小的键
         * @param max_key 最大的键
         */
        void set_header(uint64_t time_stamp, uint64_t key_count, uint64_t min_key, uint64_t max_key);

        const Header &header() const;
        const std::vector<KeyOffsetVlenTuple> &key_offset_vlen_tuple_list() const;

        /**
         * @brief 将当前SSTable状态写入文件
         * @param file_name 文件名
         */
        void WriteToFile(const std::string &file_name) const;

        /**
         * @brief 在SSTable中查找键
         *
         * @param key 查找的键
         * @return std::optional<SSTableGetResult> 查找结果，当查找成功时返回查找结果，否则返回std::nullopt
         */
        std::optional<SSTableGetResult> Get(uint64_t key) const;

        static std::vector<KeyOffsetVlenTuple> MergeSSTables(const std::vector<SSTable *> &ss_table_list);

    private:
        Header header_;
        bloom_filter::BloomFilter *bloom_filter_ = nullptr;
        std::vector<KeyOffsetVlenTuple> key_offset_vlen_tuple_list_;
    };
}

#endif // LSMKV_HANDOUT_SS_TABLE_H
