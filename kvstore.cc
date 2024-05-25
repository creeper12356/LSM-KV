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

KVStore::KVStore(const std::string &dir, const std::string &vlog)
    : KVStoreAPI(dir, vlog), dir_(dir), ss_table_count_(0)
{
    mem_table_ = new skip_list::SkipList;
    v_log_ = new v_log::VLog(vlog);
}

KVStore::~KVStore()
{
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

    // 从SSTable中查找
    std::vector<std::string> ss_table_file_name_list;
    utils::scanDir("data/level-0", ss_table_file_name_list);

    ss_table::SSTable *ss_table;
    uint64_t latest_time_stamp = 0;
    std::string result;
    for (const auto &ss_table_file_name : ss_table_file_name_list)
    {
        ss_table = new ss_table::SSTable;
        if (!ss_table->ReadFromFile("data/level-0/" + ss_table_file_name)) {
            LOG_ERROR("Reading SSTable file error");
            delete ss_table;
            continue;
        }

        auto ss_table_get_res = ss_table->Get(key);
        if (ss_table_get_res.has_value() && ss_table->header().time_stamp > latest_time_stamp)
        {
            if(ss_table_get_res.value().vlen) {
                result = this->v_log_->Get(ss_table_get_res.value().offset, ss_table_get_res.value().vlen);
            } else {
                result = "";
            }
            latest_time_stamp = ss_table->header().time_stamp;
        }
        delete ss_table;
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
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    mem_table_->Reset();

    // 删除data/level-0目录下的所有文件
    std::vector<std::string> ss_table_file_name_list;
    utils::scanDir("data/level-0", ss_table_file_name_list);
    for (const auto &ss_table_file_name : ss_table_file_name_list)
    {
        utils::rmfile("data/level-0/" + ss_table_file_name);
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
    ss_table::SSTable ss_table(++ss_table_count_, *mem_table_);

    // SSTable Header参数
    uint64_t time_stamp, key_count = 0, min_key, max_key;
    // 写入VLog的偏移量
    uint64_t v_log_offset;

    time_stamp = ss_table_count_;
    auto it = mem_table_->begin();
    // 处理第一个结点
    min_key = (*it).key();

    ss_table.InsertKeyToBloomFilter((*it).key());
    if ((*it).val() == DELETED)
    {
        ss_table.InsertKeyOffsetVlenTuple((*it).key(), 0, 0);
    }
    else
    {
        v_log_offset = v_log_->Insert((*it).key(), (*it).val());
        ss_table.InsertKeyOffsetVlenTuple((*it).key(), v_log_offset, (*it).val().size());
    }
    ++key_count;
    ++it;
    while (it != mem_table_->end())
    {
        ss_table.InsertKeyToBloomFilter((*it).key());
        if ((*it).val() == DELETED)
        {
            ss_table.InsertKeyOffsetVlenTuple((*it).key(), 0, 0);
        }
        else
        {
            v_log_offset = v_log_->Insert((*it).key(), (*it).val());
            ss_table.InsertKeyOffsetVlenTuple((*it).key(), v_log_offset, (*it).val().size());
        }
        ++key_count;
        if (it.Next() == mem_table_->end())
        {
            // 最后一个结点
            max_key = (*it).key();
        }
        ++it;
    }
    ss_table.set_header(time_stamp, key_count, min_key, max_key);

    // 将SSTable写入文件
    std::stringstream stream;
    stream << dir_ << "/level-0/" << ss_table_count_ << ".sst";
    ss_table.WriteToFile(stream.str());
}
