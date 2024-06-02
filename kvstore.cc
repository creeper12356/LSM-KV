#include "kvstore.h"
#include "skip_list.h"
#include "bloom_filter.h"
#include "ss_table.h"
#include "v_log.h"
#include "utils.h"
#include "inc.h"
#include "utils/logger.h"

#include <iostream>
#include <string>
#include <sstream>
#include <memory>
#include <limits>
#include <algorithm>
#include <chrono>

KVStore::KVStore(const std::string &dir, const std::string &vlog)
    : KVStoreAPI(dir, vlog), dir_(dir)
{
    LOG_INFO("KVStore is created");

    std::vector<std::string> data_dir_entry_list;
    utils::scanDir(this->dir_, data_dir_entry_list);

    LOG_INFO("Check SSTable files begins");
    int level = 0;
    while(std::find(data_dir_entry_list.begin(), data_dir_entry_list.end(), "level-" + std::to_string(level)) != data_dir_entry_list.end()) {
        std::vector<std::string> ss_table_file_name_list;
        utils::scanDir(this->dir_ + "/level-" + std::to_string(level), ss_table_file_name_list);
        for (const auto &ss_table_file_name : ss_table_file_name_list)
        {
            if(!ss_table_file_name.ends_with(".sst")) {
                LOG_WARNING("Invalid file in level-%d: %s found", level, ss_table_file_name);
            }
        }
        ++level;
    }
    LOG_INFO("Check SSTable files complete");
    LOG_INFO("%d SSTable level(s) detected", level);

    mem_table_ = new skip_list::SkipList;
    v_log_ = new v_log::VLog(vlog);
}

KVStore::~KVStore()
{
    LOG_INFO("KVStore is destroyed");
    delete mem_table_;
    delete v_log_;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    mem_table_->Put(key, s);
    if (mem_table_->size() == MEM_TABLE_CAPACITY)
    {
        // 将所有跳表数据写入level 0 SSTable文件
        ConvertMemTableToSSTable();
        mem_table_->Reset();

        if(!utils::dirExists(dir_ + "/level-0")) {
            utils::mkdir(dir_ + "/level-0");
        }
        std::vector<std::string> level_0_ss_table_file_list;
        utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, 0), level_0_ss_table_file_list);
        if(level_0_ss_table_file_list.size() > 2) {
        //     // level 0 SSTable文件数量大于2，进行合并
            // LOG_INFO("Compaction begins");
            std::vector<std::unique_ptr<ss_table::SSTable>>ss_table_list;

            uint64_t min_key = std::numeric_limits<uint64_t>::max(), 
                     max_key = std::numeric_limits<uint64_t>::min();

            for(const auto &level_0_ss_table_file_name: level_0_ss_table_file_list) {
                auto ss_table = 
                    ss_table::SSTable::FromFile(ss_table::SSTable::BuildSSTableFileName(
                        dir_, 
                        0, 
                        level_0_ss_table_file_name
                    )
                );
                if(!ss_table) {
                    continue;
                }
                min_key = ss_table->header().min_key < min_key ? ss_table->header().min_key : min_key;
                max_key = ss_table->header().max_key > max_key ? ss_table->header().max_key : max_key;
                ss_table_list.push_back(std::move(ss_table));
            }

            // TODO: use BuildSSTableDir function
            if(!utils::dirExists(dir_ + "/level-1")) {
                utils::mkdir(dir_ + "/level-1");
            }
            std::vector<std::string> level_1_ss_table_file_list;
            utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, 1), level_1_ss_table_file_list);
            for(const auto &level_1_ss_table_file_name: level_1_ss_table_file_list) {
                auto ss_table = ss_table::SSTable::FromFile(ss_table::SSTable::BuildSSTableFileName(dir_, 1, level_1_ss_table_file_name));
                if(!ss_table) {
                    continue;
                }
                if(ss_table->header().max_key < min_key || ss_table->header().min_key > max_key) {
                    // SSTable 区间与[min_key, max_key]无交集
                    continue;
                }

                // SSTable区间与[min_key, max_key]有交集 
                ss_table_list.push_back(std::move(ss_table));
            }

            // 刪除旧的SSTable文件
            for(const auto &ss_table: ss_table_list) {
                int rmfile_res = utils::rmfile(ss_table->file_name());
                if(rmfile_res < 0) {
                    LOG_WARNING("Failed to remove file: %s", ss_table.get()->file_name().c_str());
                }
            }

            // 使用归并排序，将ss_table_list中的SSTable合并
            auto merged_time_stamped_tuple_list = ss_table::SSTable::MergeSSTables(ss_table_list);
            // 每16kB分成一个新的SSTable文件
            uint64_t merged_time_stamped_tuple_count = merged_time_stamped_tuple_list.size();
            for(uint64_t i = 0;i < merged_time_stamped_tuple_count; i += MEM_TABLE_CAPACITY) {
                // 判断是不是最后一组
                int sublist_size = 
                    i + MEM_TABLE_CAPACITY < merged_time_stamped_tuple_count ?
                    MEM_TABLE_CAPACITY : merged_time_stamped_tuple_count - i;
                std::vector<ss_table::TimeStampedKeyOffsetVlenTuple> 
                    merged_time_stamped_tuple_sublist(
                        merged_time_stamped_tuple_list.begin() + i, 
                        merged_time_stamped_tuple_list.begin() + i + sublist_size
                    );

                // 计算子列表的最大时间戳
                uint64_t max_time_stamp = std::numeric_limits<uint64_t>::min();
                std::vector<ss_table::KeyOffsetVlenTuple> inserted_tuples;
                for(const auto &time_stamped_tuple: merged_time_stamped_tuple_sublist) {
                    max_time_stamp = time_stamped_tuple.time_stamp > max_time_stamp ? time_stamped_tuple.time_stamp : max_time_stamp;
                    inserted_tuples.push_back(time_stamped_tuple.key_offset_vlen_tuple);
                }
                
                // 将SSTable写入文件
                auto ss_table = ss_table::SSTable::NewSSTable(
                    ss_table::SSTable::BuildUniqueSSTableFileName(
                        dir_,
                        1
                    ),
                    max_time_stamp, 
                    inserted_tuples
                );
                ss_table->WriteToFile();
            }
        }
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string mem_table_get_result = mem_table_->Get(key);
    if (mem_table_get_result == DELETED)
    {
        // 内存表中查找到删除标记
        return "";
    }
    if (!mem_table_get_result.empty())
    {
        // 内存表中查找成功
        return mem_table_get_result;
    }

    // 从SSTable的level-0中查找
    std::string result = get_in_level(key, 0);
    if(!result.empty()) {
        return result;
    }
    return get_in_level(key, 1);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if (this->get(key).empty())
    {
        return false;
    }

    mem_table_->Put(key, DELETED);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    mem_table_->Reset();

    // 删除data/level-0下的所有文件
    std::vector<std::string> ss_table_file_name_list;
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, 0), ss_table_file_name_list);
    for (const auto &ss_table_file_name : ss_table_file_name_list)
    {
        utils::rmfile(ss_table::SSTable::BuildSSTableFileName(dir_, 0, ss_table_file_name));
    }

    // 删除data/level-1下的所有文件
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, 1), ss_table_file_name_list);
    for (const auto &ss_table_file_name : ss_table_file_name_list)
    {
        utils::rmfile(ss_table::SSTable::BuildSSTableFileName(dir_, 1, ss_table_file_name));
    }

}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    //    mem_table_->Scan(key1, key2, list);
    // naive 实现
    for (auto i = key1; i <= key2; ++i)
    {
        auto get_result = get(i);
        if (!get_result.empty())
        {
            list.emplace_back(i, get_result);
        }
    }
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}

void KVStore::ConvertMemTableToSSTable()
{
    utils::mkdir(dir_ + "/level-0");
    // 准备inserted_tuples
    std::vector<ss_table::KeyOffsetVlenTuple> inserted_tuples;
    uint64_t v_log_offset;  // 写入VLog的偏移量
    for(auto it = mem_table_->begin(); it != mem_table_->end(); ++it) {
        if((*it).val() == DELETED) {
            inserted_tuples.push_back({(*it).key(), 0, 0});
        } else {
            v_log_offset = v_log_->Insert((*it).key(), (*it).val());
            inserted_tuples.emplace_back((*it).key(), v_log_offset, (*it).val().size());
        }
    }

    // 将SSTable写入文件
    auto ss_table = ss_table::SSTable::NewSSTable(
        ss_table::SSTable::BuildUniqueSSTableFileName(
            dir_, 
            0
        ),
        std::chrono::system_clock::now().time_since_epoch().count(),
        inserted_tuples
    );
    ss_table->WriteToFile();
}

std::string KVStore::get_in_level(uint64_t key, int level) {
    std::vector<std::string> ss_table_file_name_list;
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), ss_table_file_name_list);

    std::unique_ptr<ss_table::SSTable> ss_table;
    uint64_t latest_time_stamp = 0;
    std::string result;
    for (const auto &ss_table_file_name : ss_table_file_name_list)
    {
        ss_table = ss_table::SSTable::FromFile(
            ss_table::SSTable::BuildSSTableFileName(
                dir_, 
                level, 
                ss_table_file_name
            )
        );
        if(!ss_table) {
            continue;
        }

        auto ss_table_get_res = ss_table->Get(key);
        if (ss_table_get_res.has_value() && ss_table->header().time_stamp > latest_time_stamp)
        {
            result = ss_table_get_res.value().vlen ?
                this->v_log_->Get(ss_table_get_res.value().offset, ss_table_get_res.value().vlen) : 
                "";
            latest_time_stamp = ss_table->header().time_stamp;
        }
    }
    return result;
}