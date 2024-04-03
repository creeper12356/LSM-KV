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
        void Insert(uint64_t key, const std::string &val);
    private:
        std::string file_name_;

    };
}


#endif //LSMKV_HANDOUT_V_LOG_H
