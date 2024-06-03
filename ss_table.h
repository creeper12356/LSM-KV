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
    struct TimeStampedKeyOffsetVlenTuple
    {
        uint64_t time_stamp;
        KeyOffsetVlenTuple key_offset_vlen_tuple;

        TimeStampedKeyOffsetVlenTuple(uint64_t time_stamp, const KeyOffsetVlenTuple &key_offset_vlen_tuple)
            : time_stamp(time_stamp), key_offset_vlen_tuple(key_offset_vlen_tuple) { }
    };
    struct SSTableMetaData {
        Header header;
        std::string ss_table_file_name;
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
         * @param file_name
         * @param time_stamp 
         * @param inserted_tuples 
         * @return std::unique_ptr<SSTable> 
         */
        static std::unique_ptr<SSTable> NewSSTable(const std::string &file_name, uint64_t time_stamp, const std::vector<KeyOffsetVlenTuple> &inserted_tuples);
    
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
        const std::string &file_name() const;

        /**
         * @brief 生成SSTable文件名
         * 
         * @param dir 基准路径，如"data"
         * @param level 层数，如"0"
         * @param base_name 不包含路径，包含后缀的文件名，如"1.sst"
         * @return std::string 如"data/level-0/1.sst"
         */
        static std::string BuildSSTableFileName(const std::string &dir, int level, const std::string &base_name);
        
        /**
         * @brief 生成SSTable目录名
         * 
         * @param dir 基准路径，如"data"
         * @param level 层数，如"0"
         * @return std::string 如"data/level-0"
         */
        static std::string BuildSSTableDirName(const std::string &dir, int level);


        /**
         * @brief 生成唯一的SSTable文件名
         * 
         * @param dir 基准路径
         * @param level 层数
         * @return std::string 如"data/level-0/17000000.sst"
         */
        static std::string BuildUniqueSSTableFileName(const std::string &dir, int level);


        /**
         * @brief 将当前SSTable状态写入文件
         */
        void WriteToFile() const;

        /**
         * @brief 在SSTable中查找键
         *
         * @param key 查找的键
         * @return std::optional<SSTableGetResult> 查找结果，当查找成功时返回查找结果，否则返回std::nullopt
         */
        std::optional<SSTableGetResult> Get(uint64_t key) const;


        static std::vector<TimeStampedKeyOffsetVlenTuple> MergeSSTables(const std::vector<std::unique_ptr<SSTable>> &ss_table_list);
        
        /**
         * @brief 直接读取SSTable文件的Header部分
         * 
         * @param ss_table_file_name 文件完整路径
         * @return Header 读取到的Header
         */
        static Header ReadSSTableHeaderDirectly(const std::string &ss_table_file_name);

        /**
         * @brief SSTable第level层最多允许的SSTable数量
         * 
         * @param level 层数
         * @return int 
         */
        static int SSTableMaxCountAtLevel(int level);
    
    private:
        Header header_;
        bloom_filter::BloomFilter *bloom_filter_ = nullptr;
        std::vector<KeyOffsetVlenTuple> key_offset_vlen_tuple_list_;
        std::string file_name_;
    };
}

#endif // LSMKV_HANDOUT_SS_TABLE_H
