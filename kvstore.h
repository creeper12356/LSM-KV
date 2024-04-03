#pragma once

#include "kvstore_api.h"

namespace skiplist {
	class Skiplist;
}
class KVStore : public KVStoreAPI
{
	// You can add your implementation here
public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;
private:
	skiplist::Skiplist* m_skiplist;
};
