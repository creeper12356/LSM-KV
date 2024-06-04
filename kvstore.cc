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
#include <optional>

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
    // TODO: 将SSTable文件部分加载到内存表中
    LOG_INFO("Check SSTable files complete");
    LOG_INFO("%d SSTable level(s) detected", level);

    mem_table_ = new skip_list::SkipList;
    v_log_ = new v_log::VLog(vlog);

}

KVStore::~KVStore()
{
    LOG_INFO("KVStore is destroyed");
    
    if(mem_table_->size()) {
        LOG_INFO("Store mem table to SSTable");
        ConvertMemTableToSSTable();
        DoCascadeCompaction();
    }

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
        ConvertMemTableToSSTable();
        mem_table_->Reset();
        DoCascadeCompaction();
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

    // 从SSTable逐层查找
    int level = 0;
    std::string result;
    while (utils::dirExists(ss_table::SSTable::BuildSSTableDirName(dir_, level)))
    {
        result = GetValueInSSTable(key, level);
        if(result == DELETED) {
            // 查找到删除标记
            return "";
        }
        else if(!result.empty()) {
            // 查找到有效的值
            return result;
        }
        // 未查找到任何记录，继续查找下一层
        ++ level;
    }

    return result;
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
    if (mem_table_->size() == MEM_TABLE_CAPACITY)
    {
        ConvertMemTableToSSTable();
        mem_table_->Reset();
        DoCascadeCompaction();
    }
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    // 清空内存表
    mem_table_->Reset();

    // 删除所有SSTable文件及其目录
    int level = 0;
    while(utils::dirExists(ss_table::SSTable::BuildSSTableDirName(dir_, level))) {
        std::vector<std::string> base_file_name_list;
        utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), base_file_name_list);
        for(const auto &base_file_name: base_file_name_list) {
            if(utils::rmfile(ss_table::SSTable::BuildSSTableFileName(dir_, level, base_file_name)) < 0) {
                LOG_WARNING("Failed to remove SSTable file %s", base_file_name.c_str());
            }
        }
        if(utils::rmdir(ss_table::SSTable::BuildSSTableDirName(dir_, level)) < 0) {
            LOG_WARNING("Failed to remove SSTable directory level-%d", level);
        }
        ++level;
    }
    
    // 重置VLog
    v_log_->Reset();
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
    std::vector<v_log::DeallocVLogEntryInfo> dealloc_entries;
    v_log_->DeallocSpace(chunk_size, dealloc_entries);
    for(const auto &entry: dealloc_entries) {
        if(!IsVLogEntryOutdated(entry.key, entry.offset)) {
            mem_table_->Put(entry.key, entry.val);
        }
    }
    
    ConvertMemTableToSSTable();
    mem_table_->Reset();
    DoCascadeCompaction();
}

void KVStore::ConvertMemTableToSSTable()
{
    utils::mkdir(dir_ + "/level-0");
    // 准备inserted_tuples
    std::vector<ss_table::KeyOffsetVlenTuple> inserted_tuples;
    uint64_t v_log_offset;  // 写入VLog的偏移量
    for(auto it = mem_table_->begin(); it != mem_table_->end(); ++it) {
        if((*it).val() == DELETED) {
            inserted_tuples.emplace_back((*it).key(), 0, 0);
        } else {
            v_log_offset = v_log_->Insert((*it).key(), (*it).val());
            inserted_tuples.emplace_back((*it).key(), v_log_offset, (*it).val().size());
        }
    }

    // 将SSTable写入文件
    uint64_t now = std::chrono::system_clock::now().time_since_epoch().count();
    auto ss_table = ss_table::SSTable::NewSSTable(
        ss_table::SSTable::BuildSSTableFileName(
            dir_, 
            0, 
            std::to_string(now) + ".sst"
        ),
        now,
        inserted_tuples
    );
    ss_table->WriteToFile();
}


void KVStore::LoadSSTablesToMemory(
    const std::vector<std::string> &ss_table_file_name_list, 
    int level,
    std::vector<std::unique_ptr<ss_table::SSTable>> &ss_table_list,
    uint64_t &min_key,
    uint64_t &max_key
) {
    min_key = std::numeric_limits<uint64_t>::max();
    max_key = std::numeric_limits<uint64_t>::min();

    for(const auto &ss_table_file_name: ss_table_file_name_list) {
        auto ss_table = 
            ss_table::SSTable::FromFile(ss_table::SSTable::BuildSSTableFileName(
                dir_, 
                level,
                ss_table_file_name
            )
        );
        if(!ss_table) {
            continue;
        }
        min_key = ss_table->header().min_key < min_key ? ss_table->header().min_key : min_key;
        max_key = ss_table->header().max_key > max_key ? ss_table->header().max_key : max_key;
        ss_table_list.push_back(std::move(ss_table));
    }
}

void KVStore::LoadSSTablesInRangeToMemory(
    int level,
    uint64_t min_key, 
    uint64_t max_key,
    std::vector<std::unique_ptr<ss_table::SSTable>> &ss_table_list
) {
    std::string ss_table_dir_name = ss_table::SSTable::BuildSSTableDirName(dir_, level);
    if(!utils::dirExists(ss_table_dir_name)) {
        LOG_WARNING("SSTable directory %s does not exist", ss_table_dir_name.c_str());
        return ;
    }
    std::vector<std::string> ss_table_file_name_list;
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), ss_table_file_name_list);
    for(const auto &ss_table_file_name: ss_table_file_name_list) {
        auto ss_table = 
            ss_table::SSTable::FromFile(ss_table::SSTable::BuildSSTableFileName(dir_, level, ss_table_file_name));
        if(!ss_table) {
            continue;
        }
        if(ss_table->header().max_key < min_key || ss_table->header().min_key > max_key) {
            // SSTable 区间与[min_key, max_key]无交集
            continue;
        }

        ss_table_list.push_back(std::move(ss_table));
    }
}

void KVStore::StoreSSTablesToDisk(
    int level,
    const std::vector<ss_table::TimeStampedKeyOffsetVlenTuple> 
        &merged_time_stamped_tuple_list
) {
    std::string ss_table_dir_name = ss_table::SSTable::BuildSSTableDirName(dir_, level);
    if(!utils::dirExists(ss_table_dir_name)) {
        utils::mkdir(ss_table_dir_name);
    }

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
                level
            ),
            max_time_stamp, 
            inserted_tuples
        );
        ss_table->WriteToFile();
    }
}

std::optional<ss_table::SSTableGetResult> KVStore::GetInSSTable (
    uint64_t key, 
    int level, 
    KeyStatus &status
) const {
    // 初始化返回值
    status = KeyStatus::kNotFound; // 默认为未找到状态
    std::optional<ss_table::SSTableGetResult> result;

    std::vector<std::string> ss_table_base_file_name_list;
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), ss_table_base_file_name_list);

    std::unique_ptr<ss_table::SSTable> ss_table;
    uint64_t latest_time_stamp = std::numeric_limits<uint64_t>::min();
    for (const auto &ss_table_base_file_name : ss_table_base_file_name_list)
    {
        auto ss_table_file_name = ss_table::SSTable::BuildSSTableFileName(
            dir_,
            level,
            ss_table_base_file_name
        );
        auto header = ss_table::SSTable::ReadSSTableHeaderDirectly(ss_table_file_name);
        if(header.key_count == 0) {
            // 读取SSTable文件header失败
            continue;
        }
        if(key < header.min_key || key > header.max_key) {
            // key不在SSTable的键范围内
            continue;
        }

        // 将整个SSTable文件读入内存
        ss_table = ss_table::SSTable::FromFile(ss_table_file_name);
        if(!ss_table) {
            // 读取SSTable文件失败
            continue;
        }

        auto ss_table_get_res = ss_table->Get(key);
        if (ss_table_get_res.has_value() && ss_table->header().time_stamp > latest_time_stamp)
        {
            // 找到了一条记录且时间戳更新（找到vlog中的索引或者删除标记），
            // 更新返回值和状态
            if(ss_table_get_res.value().vlen) {
                result = ss_table_get_res.value();
                status = KeyStatus::kFound;
            } else {
                result = std::nullopt;
                status = KeyStatus::kDeleted;
            }
            latest_time_stamp = ss_table->header().time_stamp;
        }
    }

    return result;
}

std::string KVStore::GetValueInSSTable(uint64_t key, int level) const {
    KeyStatus key_status;
    std::optional<ss_table::SSTableGetResult> optional_offset = GetInSSTable(key, level, key_status);

    switch (key_status)
    {
    case KeyStatus::kFound:
        return v_log_->Get(optional_offset->offset, optional_offset->vlen);
        break;
    case KeyStatus::kDeleted:
        return DELETED;
        break;
    case KeyStatus::kNotFound:
    default:
        return "";
        break;
    }
}

void KVStore::DoCompaction(
    const std::vector<std::string> ss_table_base_file_name_list, 
    int from_level,
    int to_level
) {
    // 将SSTable文件读入内存
    std::vector<std::unique_ptr<ss_table::SSTable>>ss_table_list;
    uint64_t min_key = std::numeric_limits<uint64_t>::max(), 
                max_key = std::numeric_limits<uint64_t>::min();
    LoadSSTablesToMemory(ss_table_base_file_name_list, from_level,
                        ss_table_list, min_key, max_key);
    LoadSSTablesInRangeToMemory(to_level, min_key, max_key, ss_table_list);

    // 刪除旧的SSTable文件
    std::vector<std::string> deleted_ss_table_file_name_list;
    for(const auto &ss_table: ss_table_list) {
        // 此处ss_table-> file_name()为完整路径
        deleted_ss_table_file_name_list.push_back(ss_table->file_name());
    }
    if(utils::rmfiles(deleted_ss_table_file_name_list) < 0) {
        LOG_WARNING("Failed to remove old SSTable files");
    }

    // 合并SSTable文件, 并将合并后的SSTable文件写入磁盘
    auto merged_time_stamped_tuple_list = ss_table::SSTable::MergeSSTables(ss_table_list);
    StoreSSTablesToDisk(to_level, merged_time_stamped_tuple_list);
}

void KVStore::DoCascadeCompaction() {
    int level = 0;
    while(CheckSSTableLevelOverflow(level)) {
        std::vector<std::string> ss_table_base_file_name_list;
        utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), ss_table_base_file_name_list);
        if(level == 0) {
            DoCompaction(ss_table_base_file_name_list, level, level + 1);
            ++ level;
            continue;
        }

        std::vector<std::string> compacted_ss_table_base_file_name_list;
        FilterSSTableFiles(
            ss_table_base_file_name_list,
            level, 
            ss_table_base_file_name_list.size() - ss_table::SSTable::SSTableMaxCountAtLevel(level),
            compacted_ss_table_base_file_name_list
        );
        DoCompaction(compacted_ss_table_base_file_name_list, level, level + 1);
        ++ level;
    }
}

void KVStore::FilterSSTableFiles(
    const std::vector<std::string> &ss_table_base_file_name_list,
    int level,
    int filter_size,
    std::vector<std::string> &filtered_ss_table_base_file_name_list
) {
    std::vector<ss_table::SSTableMetaData> ss_table_meta_data_list;
    for(const auto &ss_table_file_name: ss_table_base_file_name_list) {
        ss_table_meta_data_list.push_back({
            ss_table::SSTable::ReadSSTableHeaderDirectly(
                ss_table::SSTable::BuildSSTableFileName(dir_, level, ss_table_file_name)
            ),
            ss_table_file_name
        });
    }

    std::sort(ss_table_meta_data_list.begin(), ss_table_meta_data_list.end(),
        [](const ss_table::SSTableMetaData &a, const ss_table::SSTableMetaData &b) {
            // 首先按照时间戳升序排序，然后按照最小键升序排序
            return a.header.time_stamp < b.header.time_stamp 
            || (a.header.time_stamp == b.header.time_stamp && a.header.min_key < b.header.min_key);
        }
    );
    
    for(int i = 0; i < filter_size; ++i) {
        // LOG_INFO("timestamp: %lu", ss_table_meta_data_list[i].header.time_stamp);
        filtered_ss_table_base_file_name_list.push_back(ss_table_meta_data_list[i].ss_table_file_name);
    }
}


bool KVStore::CheckSSTableLevelOverflow(int level) const {
    std::vector<std::string> ss_table_file_name_list;
    std::string dir = ss_table::SSTable::BuildSSTableDirName(dir_, level);
    if(!utils::dirExists(dir)) {
        return false;
    }
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), ss_table_file_name_list);
    return ss_table_file_name_list.size() > ss_table::SSTable::SSTableMaxCountAtLevel(level);
}

bool KVStore::IsVLogEntryOutdated(uint64_t v_log_entry_key, uint64_t v_log_entry_offset) const {
    std::string mem_table_get_result = mem_table_->Get(v_log_entry_key);
        if(!mem_table_get_result.empty()) {
            // 内存表中查找成功，或者在内存表中找到删除标记
            return true;
        }

    // 从SSTable逐层查找
    int level = 0;
    KeyStatus key_status;
    std::optional<ss_table::SSTableGetResult> result;
    while (utils::dirExists(ss_table::SSTable::BuildSSTableDirName(dir_, level)))
    {
        result = GetInSSTable(v_log_entry_key, level, key_status);
        if(key_status == KeyStatus::kDeleted) {
            // 在SSTable中查找到删除标记
            return true;
        }
        if(key_status == KeyStatus::kFound) {
            // 查找到有效的值
            return result->offset != v_log_entry_offset;
        }

        // 未查找到任何记录，继续查找下一层
        ++ level;
    }

    // 没有查找到任何记录
    return true;
}




/* For Test Only */
void KVStore::get_everywhere(uint64_t key) {
    int level = 0;
    auto mem_table_get_res = mem_table_->Get(key);
    if(!mem_table_get_res.empty()) {
        LOG_WARNING("key %lu found in mem table: %s", key, mem_table_get_res.c_str());
    } else {
        LOG_INFO("key %lu not found in mem table", key);
    }
    
    while(utils::dirExists(ss_table::SSTable::BuildSSTableDirName(dir_, level))) {
        LOG_INFO("## read level %d ##", level);
        get_everywhere_in_level(key, level);
        ++ level;
    }
}

void KVStore::get_everywhere_in_level(uint64_t key, int level) {
    std::vector<std::string> ss_table_base_file_name_list;
    utils::scanDir(ss_table::SSTable::BuildSSTableDirName(dir_, level), ss_table_base_file_name_list);

    std::unique_ptr<ss_table::SSTable> ss_table;
    for (const auto &ss_table_base_file_name : ss_table_base_file_name_list)
    {
        auto ss_table_file_name = ss_table::SSTable::BuildSSTableFileName(
            dir_,
            level,
            ss_table_base_file_name
        );
        auto header = ss_table::SSTable::ReadSSTableHeaderDirectly(ss_table_file_name);
        if(header.key_count == 0) {
            // 读取SSTable文件header失败
            continue;
        }
        if(key < header.min_key || key > header.max_key) {
            // key不在SSTable的键范围内
            continue;
        }

        // 将整个SSTable文件读入内存
        ss_table = ss_table::SSTable::FromFile(ss_table_file_name);
        if(!ss_table) {
            // 读取SSTable文件失败
            continue;
        }

        auto ss_table_get_res = ss_table->Get(key);
        if (ss_table_get_res.has_value())
        {
            // 找到了key对应的一条记录
            if(ss_table_get_res.value().vlen) {
                LOG_WARNING("key %lu found in level %d: %s", key, level, this->v_log_->Get(ss_table_get_res.value().offset, ss_table_get_res.value().vlen).c_str());
                LOG_WARNING("time stamp: %lu", ss_table->header().time_stamp);
            } else {
                LOG_WARNING("key %lu found in level %d: DELETED", key, level);
                LOG_WARNING("time stamp: %lu", ss_table->header().time_stamp);
            }
        }
    }
}