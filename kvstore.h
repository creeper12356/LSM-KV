#pragma once

#include "kvstore_api.h"
#include <vector>
#include <memory>
#include <string>
#include <list>

namespace skip_list
{
	class SkipList;
}
namespace v_log
{
	class VLog;
}
namespace ss_table
{
	class SSTable;
	struct TimeStampedKeyOffsetVlenTuple;
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

	/**
	 * @brief 将SSTable文件加载到内存
	 * 
	 * @param ss_table_file_name_list 需要加载的SSTable文件名列表（不是完整路径）
	 * @param level 加载的SSTable所在的层级
	 * @param ss_table_list 将所有加载的SSTable追加到该列表中
	 * @param min_key 加载后的SSTable中的最小key
	 * @param max_key 加载后的SSTable中的最大key
	 */
	void LoadSSTablesToMemory(
		const std::vector<std::string> &ss_table_file_name_list, 
		int level,
		std::vector<std::unique_ptr<ss_table::SSTable>> &ss_table_list,
		uint64_t &min_key,
		uint64_t &max_key
	);

	/**
	 * @brief 将指定范围内的SSTable文件加载到内存
	 * 
	 * @param level 加载的SSTable所在的层级
	 * @param min_key 最小key
	 * @param max_key 最大key
	 * @param ss_table_list 将所有加载的SSTable追加到该列表中
	 */
	void LoadSSTablesInRangeToMemory(
		int level,
		uint64_t min_key, 
		uint64_t max_key,
		std::vector<std::unique_ptr<ss_table::SSTable>> &ss_table_list
	);

	/**
	 * @brief 从tuples生产新的SSTable文件，写入第level层
	 * 
	 * @param level 层数
	 * @param tuples 合并后得到的带有时间戳的KeyOffsetVlen元组 
	 */
	void StoreSSTableToDisk(
		int level,
		const std::vector<ss_table::TimeStampedKeyOffsetVlenTuple> &tuples
	);



	std::string get_in_level(uint64_t key, int level);


private:
	std::string dir_;
	skip_list::SkipList *mem_table_;
	v_log::VLog *v_log_;
};
