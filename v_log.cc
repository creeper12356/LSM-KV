//
// Created by creeper on 24-4-3.
//
#include "v_log.h"
#include "utils.h"
#include "utils/logger.h"

#include <fstream>
#include <vector>
#include <iostream>

v_log::VLog::VLog(const std::string &v_log_file_name): file_name_(v_log_file_name) {
    // TODO: 使用别的方式检查VLog文件是否存在
    std::ifstream fin;
    fin.open(file_name_, std::ios::binary);
    if (!fin) {
        std::ofstream fout;
        fout.open(file_name_, std::ios::binary);
        fout.close();
        tail_ = 0;
        return ;
    }

    tail_ = utils::seek_data_block(file_name_);
    LOG_INFO("VLog tail: %lu", tail_);
    fin.seekg(tail_);
    VLogEntry v_log_entry;
    while(true) {
        uint64_t val_offset = v_log_entry.ReadFromFile(fin);
        if(!val_offset) {
            LOG_ERROR("Failed to read VLog entry");
            fin.close();
            return ;
        }
        if(v_log_entry.InspectChecksum()) {
            break;
        }
        LOG_WARNING("Inspect checksum failed, try next...");
        tail_ += v_log_entry.size();
    }
    
    fin.close();
}

uint64_t v_log::VLog::Insert(uint64_t key, const std::string &val) {
    std::ofstream fout;

    fout.open(file_name_, std::ios::app | std::ios::binary);

    // 写入Magic byte
    fout.write(&kMagic, 1);

    // 拼接key-vlen-value字符串
    std::string data_str;
    uint32_t vlen = val.size();
    data_str.append(reinterpret_cast<const char*> (&key), sizeof (uint64_t));
    data_str.append(reinterpret_cast<const char*> (&vlen), sizeof (uint32_t));
    data_str.append(val);

    // 调用crc16函数，写入校验和
    std::vector<unsigned char> data(data_str.begin(), data_str.end());
    uint16_t check_sum = utils::crc16(data);

    fout.write(reinterpret_cast<const char*>(&check_sum), 2);

    // 写入key-vlen-value拼接结果
    fout.write(reinterpret_cast<const char*> (&key), sizeof (uint64_t));
    fout.write(reinterpret_cast<const char*> (&vlen), sizeof (uint32_t));
    uint64_t offset;
    offset = fout.tellp();
    offset = fout.tellp();
    fout.write(val.c_str(), val.size());

    fout.close();

    
    return offset;
}

std::string v_log::VLog::Get(uint64_t offset, uint32_t vlen) {
    std::ifstream fin;
    fin.open(file_name_, std::ios::binary);
    if(!fin) {
        // 无法打开文件，返回空字符串
        return "";
    }
    
    fin.seekg(offset);
    std::string val = "";
    char* val_c_str = new char[vlen + 1];
    val_c_str[vlen] = '\0';
    fin.read(val_c_str, vlen);
    val = val_c_str;

    delete [] val_c_str;
    fin.close();
    return val;
}

void v_log::VLog::Reset() {
    tail_ = 0;
    if(utils::rmfile(file_name_) < 0) {
        LOG_WARNING("Failed to remove VLog file");
    }
}

bool v_log::VLog::DeallocSpace(
    uint64_t chunck_size, 
    std::vector<v_log::DeallocVLogEntryInfo> &dealloc_entry_list
) {
    std::ifstream fin;
    fin.open(file_name_);
    if(!fin) {
        LOG_ERROR("Failed to open VLog file");
        return false;
    }

    fin.seekg(tail_);
    uint64_t read_chunck_size = 0;

    while(read_chunck_size < chunck_size) {
        VLogEntry v_log_entry;
        uint64_t val_offset;

        if(!(val_offset = v_log_entry.ReadFromFile(fin))) {
            break;
        }

        if(!v_log_entry.InspectChecksum()) {
            // LOG_WARNING("Inspect checksum failed, try next...");
            continue;
        }

        dealloc_entry_list.emplace_back(v_log_entry.key, val_offset, v_log_entry.val);
        read_chunck_size += v_log_entry.size();
    }

    // 文件打洞
    utils::de_alloc_file(file_name_, tail_, read_chunck_size);
    tail_ += read_chunck_size;
    return true;
}

uint64_t v_log::VLogEntry::ReadFromFile(std::ifstream &fin) {
    char ch = 0;
    while(ch != kMagic) {
        fin.read(&ch, 1);
        if(fin.eof()) {
            LOG_ERROR("Reach EOF before reading Magic byte");
            return 0;
        }
        if(!fin) {
            LOG_ERROR("Read Magic byte error");
            return 0;
        }
    }

    // 读取check sum 
    if(!fin.read(reinterpret_cast<char *> (&check_sum), sizeof(check_sum))) {
        LOG_ERROR("Read checksum error");
        return 0;
    }

    // 读取key-vlen-val部分
    fin.read(reinterpret_cast<char *> (&key), sizeof(key));
    if(!fin) {
        LOG_ERROR("Read Key error");
        return 0;
    }
    fin.read(reinterpret_cast<char *> (&vlen), sizeof(vlen));
    if(!fin) {
        LOG_ERROR("Read Vlen error");
        return 0;
    }
    uint64_t val_offset;
    val_offset = fin.tellg();
    char *v_log_entry_val_c_str = new char[vlen + 1];
    v_log_entry_val_c_str[vlen] = '\0';
    fin.read(v_log_entry_val_c_str, vlen);
    val = std::string(v_log_entry_val_c_str);
    delete [] v_log_entry_val_c_str;

    if(!fin) {
        LOG_ERROR("Read Value error");
        return 0;
    }

    return val_offset;

}

bool v_log::VLogEntry::InspectChecksum() const {
    // 拼接key-vlen-value字符串
    std::string key_vlen_value_str;
    key_vlen_value_str.append(reinterpret_cast<const char*> (&key), sizeof (key));
    key_vlen_value_str.append(reinterpret_cast<const char*> (&vlen), sizeof (vlen));
    key_vlen_value_str.append(val);
    std::vector<unsigned char> key_vlen_value_data(
        key_vlen_value_str.begin(), 
        key_vlen_value_str.end()
    );

    return check_sum == utils::crc16(key_vlen_value_data);
}

uint64_t v_log::VLogEntry::size() const {
    return sizeof(char) + sizeof(check_sum) + sizeof(key) + sizeof(vlen) + vlen;
}