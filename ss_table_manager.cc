#include "ss_table_manager.h"
#include "utils/logger.h"
#include "bloom_filter.h"
#include "inc.h"
#include "utils.h"

#include <limits>

namespace ss_table {
    std::shared_ptr<SSTable> SSTableManager::FromFile(const std::string &file_name)
    {
        if(ss_table_read_cache_.find(file_name) != ss_table_read_cache_.end()) {
            // LOG_INFO("Cache hit for SSTable file `%s`", file_name.c_str());
            return ss_table_read_cache_[file_name];
        }

        std::ifstream fin;
        fin.open(file_name);
        if(!fin) {
            LOG_ERROR("Read SSTable file `%s` error", file_name.c_str());
            return nullptr;
        }

        std::shared_ptr<SSTable> new_ss_table = SSTable::create();
        fin.read(reinterpret_cast<char*>(&new_ss_table.get()->header_), sizeof(Header));
        
        new_ss_table.get()->bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE);
        new_ss_table.get()->bloom_filter_->ReadFromFile(fin);

        auto key_count = new_ss_table.get()->header_.key_count;
        for(uint64_t i = 0;i < key_count; ++i) {
            new_ss_table.get()->key_offset_vlen_tuple_list_.emplace_back(&fin);
        }
        fin.close();
        new_ss_table.get()->file_name_ = file_name;

        ss_table_read_cache_[file_name] = new_ss_table;
        return new_ss_table;
    }
    
    std::shared_ptr<SSTable> SSTableManager::NewSSTable(const std::string &file_name, uint64_t time_stamp, const std::vector<KeyOffsetVlenTuple> &inserted_tuples)
    {
        std::shared_ptr<SSTable> new_ss_table = SSTable::create();
        new_ss_table.get()->bloom_filter_ = new bloom_filter::BloomFilter(BLOOM_FILTER_VECTOR_SIZE);
        
        uint64_t min_key = std::numeric_limits<uint64_t>::max(),
                 max_key = std::numeric_limits<uint64_t>::min();
        for(const auto &tuple: inserted_tuples) {
            min_key = tuple.key < min_key ? tuple.key : min_key;
            max_key = tuple.key > max_key ? tuple.key : max_key;

            new_ss_table.get()->bloom_filter_->Insert(tuple.key);

            new_ss_table.get()->key_offset_vlen_tuple_list_.push_back(tuple);
        }
        new_ss_table.get()->header_ = {time_stamp, inserted_tuples.size(), min_key, max_key};
        new_ss_table.get()->file_name_ = file_name;

        ss_table_read_cache_[file_name] = new_ss_table;
        return new_ss_table;
    }
    
    void SSTableManager::WriteSSTableToFile(const std::shared_ptr<SSTable> &ss_table)
    {
        ss_table->WriteToFile();
    }
    
    void SSTableManager::DeleteSSTableFiles(const std::vector<std::string> &file_name_list)
    {
        // 从缓存中删除
        for(const auto &file_name: file_name_list) {
            ss_table_read_cache_.erase(file_name);
        }
        // 删除磁盘文件
        if(utils::rmfiles(file_name_list) < 0) {
            LOG_WARNING("Failed to remove old SSTable files");
        }
    }
    
    void SSTableManager::ResetCache()
    {
        ss_table_read_cache_.clear();
    }
}