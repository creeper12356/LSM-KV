//
// Created by creeper on 24-4-3.
//
#include "v_log.h"
#include "utils.h"
#include <fstream>
#include <vector>

v_log::VLog::VLog(const std::string &v_log_file_name): file_name_(v_log_file_name) {
    // 创建新的vlog文件
    std::ofstream fout;
    fout.open(file_name_);
    fout.close();
}

void v_log::VLog::Insert(uint64_t key, const std::string &val) {
    std::ofstream fout;
    fout.open(file_name_, std::ios::app | std::ios::binary);

    fout.write(&kMagic, 1);

    // 拼接字符串
    std::string data_str;
    uint32_t vlen = val.size();
    data_str.append(reinterpret_cast<const char*> (&key), sizeof (uint64_t));
    data_str.append(reinterpret_cast<const char*> (&vlen), sizeof (uint32_t));
    data_str.append(val);

    // 调用crc16函数，写入校验和
    std::vector<unsigned char> data(data_str.begin(), data_str.end());
    uint16_t check_sum = utils::crc16(data);

    fout.write(reinterpret_cast<const char*>(&check_sum), 2);
    // 写入拼接结果
    fout.write(data_str.c_str(), data_str.size());

    fout.close();
}
