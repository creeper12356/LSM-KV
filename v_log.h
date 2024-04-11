//
// Created by creeper on 24-4-3.
//

#ifndef LSMKV_HANDOUT_V_LOG_H
#define LSMKV_HANDOUT_V_LOG_H
#include <string>

namespace v_log {
    const char kMagic = 0xff;

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
        std::string Get(uint64_t offset, uint32_t vlen);
    private:
        std::string file_name_;

    };
}


#endif //LSMKV_HANDOUT_V_LOG_H
