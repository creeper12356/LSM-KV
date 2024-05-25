#pragma once

#include "kvstore_api.h"

namespace skip_list
{
	class SkipList;
}
namespace v_log
{
	class VLog;
}
class KVStore : public KVStoreAPI
{
public:
	/**
	 * @brief Construct a new KVStore object
	 *
	 * @param dir SSTable文件存储目录
	 * @param vlog vlog文件路径
	 */
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

private:
	/**
	 * @brief 将内存表（跳表）写入SSTable文件
	 */
	void ConvertMemTableToSSTable();

private:
	std::string dir_;
	skip_list::SkipList *mem_table_;
	uint64_t ss_table_count_;
	v_log::VLog *v_log_;
};
