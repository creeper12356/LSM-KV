//
// Created by creeper on 24-4-3.
//

#ifndef LSMKV_HANDOUT_V_LOG_H
#define LSMKV_HANDOUT_V_LOG_H
#include <string>
#include <vector>

namespace v_log {
    const char kMagic = 0xff;

    struct VLogEntry {
        uint32_t check_sum;
        uint64_t key;
        uint32_t vlen;
        std::string val;

        /**
         * @brief 从文件输入流中读取字段，不进行检查校验，返回读取是否有效
         * 
         * @param fin 
         * @return 读取成功，返回value在文件中的偏移量，读取失败，返回0
         */
        uint64_t ReadFromFile(std::ifstream &fin);

        /**
         * @brief 检查校验和，需要先读取字段，返回检查结果
         * 
         * @return true 
         * @return false 
         */
        bool InspectChecksum() const;

        /**
         * @brief 在VLogEntry 在文件中占用的字节数，包括Magic, Checksum, key-vlen-value部分
         * 
         * @return uint64_t 
         */
        uint64_t size() const;

    };

    struct VLogEntryKeyOffsetTuple {
        uint64_t key;
        uint64_t offset;

        VLogEntryKeyOffsetTuple(uint64_t key, uint64_t offset) : key(key) , offset(offset) {}
    };

    class VLog {
    public:
        VLog(const std::string &v_log_file_name);
        /**
         * 向VLog文件中插入键值对
         * @param key
         * @param val
         * @return 插入的值在文件中的偏移量
         */
        uint64_t Insert(uint64_t key, const std::string &val);

        /**
         * @brief 从VLog文件中读取值
         * 
         * @param offset 偏移量
         * @param vlen 值的长度
         * @return std::string 
         */
        std::string Get(uint64_t offset, uint32_t vlen);
        


        const std::string &file_name() const {
            return file_name_;
        }

        /**
         * @brief 扫描VLog文件，返回扫描到的Entry信息列表
         * 
         * @param chunck_size 至少扫描的字节数
         * @return std::vector<VLogEntryKeyOffsetTuple> 
         */
        std::vector<VLogEntryKeyOffsetTuple> ScanVLogEntries(uint64_t chunck_size);


    private:
        std::string file_name_;
    };
}


#endif //LSMKV_HANDOUT_V_LOG_H
