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

	/**
	 * @brief 执行合并操作
	 * 
	 * @param ss_table_base_file_name_list 需要合并的SSTable文件名列表（不是完整路径）
	 * @param from_level 合并的SSTable所在的层级
	 * @param to_level 合并后的SSTable所在的层级
	 */
	void DoCompaction(
		const std::vector<std::string> ss_table_base_file_name_list,
		 int from_level,
		 int to_level
	);

	/**
	 * @brief 从level层过滤出时间戳最小的filter_size个SSTable文件
	 * 
	 * @param ss_table_file_name_list 候选的SSTable文件名列表
	 * @param level 层数
	 * @param filter_size 需要过滤出的SSTable文件个数 
	 * @param filtered_ss_table_file_name_list 过滤后的SSTable文件名列表 
	 */
	void FilterSSTableFiles(
		const std::vector<std::string> &ss_table_base_file_name_list,
		int level,
		int filter_size,
		std::vector<std::string> &filtered_ss_table_base_file_name_list
	);

	/**
	 * @brief 判断level层的SSTable文件是否溢出
	 * 
	 * @param level 
	 * @return true 
	 * @return false 
	 */
	bool CheckSSTableLevelOverflow(int level) const;



	/**
	 * @brief 在第level层查找key
	 * 
	 * @param key 键
	 * @param level 层数
	 * @return std::string 如果返回""，表示未找到任何记录，如果返回DELETED，表示该记录已被删除，否则返回对应的值
	 */
	std::string get_in_level(uint64_t key, int level);


private:
	std::string dir_;
	skip_list::SkipList *mem_table_;
	v_log::VLog *v_log_;
};
