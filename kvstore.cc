#include "kvstore.h"
#include "skip_list.h"
#include "bloom_filter.h"
#include "ss_table.h"
#include "v_log.h"
#include "utils.h"

#include <iostream>
#include <string>
#include <sstream>

#define MEM_TABLE_CAPACITY 411

KVStore::KVStore(const std::string &dir, const std::string &vlog)
: KVStoreAPI(dir, vlog)
, dir_(dir)
, ss_table_count_(0)
{
    std::cout << "dir = " <<  dir_ << std::endl;
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
    if(mem_table_->size() == MEM_TABLE_CAPACITY) {
        // 写入SSTable
        utils::mkdir(dir_ + "/level-0");
        ss_table::SSTable ss_table(++ ss_table_count_, *mem_table_);

        // SSTable Header参数
        uint64_t time_stamp, key_count = 0, min_key, max_key;
        // 写入VLog的偏移量
        uint64_t v_log_offset;

        time_stamp = ss_table_count_;
        auto it = mem_table_->begin();
        // 处理第一个结点
        min_key = (*it).key();

        ss_table.InsertKeyToBloomFilter((*it).key());
        v_log_offset = v_log_->Insert((*it).key(), (*it).val());
        ss_table.InsertKeyOffsetVlenTuple((*it).key(), v_log_offset, (*it).val().size());
        ++ key_count;
        ++ it;
        while(it != mem_table_->end()) {
            ss_table.InsertKeyToBloomFilter((*it).key());
            v_log_offset = v_log_->Insert((*it).key(), (*it).val());
            ss_table.InsertKeyOffsetVlenTuple((*it).key(), v_log_offset, (*it).val().size());
            ++ key_count;
            if(it.Next() == mem_table_->end()) {
                // 最后一个结点
                max_key = (*it).key();
            }
            ++ it;
        }
        ss_table.set_header(time_stamp, key_count, min_key, max_key);

        // 将SSTable写入文件
        std::stringstream stream;
        stream << dir_ << "/level-0/" << ss_table_count_ << ".sst";
        ss_table.WriteToFile(stream.str());

        mem_table_->Reset();
    }
}
/**
 * Returns the (string) value of the given key_.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	return mem_table_->Get(key);
}
/**
 * Delete the given key_-value pair if it exists.
 * Returns false iff the key_ is not found.
 */
bool KVStore::del(uint64_t key)
{
	return mem_table_->Del(key);
}

/**
 * This resets the kvstore. All key_-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    mem_table_->Reset();
}

/**
 * Return a list including all the key_-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    mem_table_->Scan(key1, key2, list);
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}