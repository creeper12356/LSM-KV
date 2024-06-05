#ifndef SS_TABLE_MANAGER_H
#define SS_TABLE_MANAGER_H
#include <map>
#include "ss_table.h"
namespace ss_table {
    class SSTableManager {
    public:
        SSTableManager() = default;
        std::shared_ptr<SSTable> FromFile(const std::string &file_name);

        std::shared_ptr<SSTable> NewSSTable(
            const std::string &file_name, 
            uint64_t time_stamp, 
            const std::vector<KeyOffsetVlenTuple> &inserted_tuples
        );

    private:
        std::map<std::string, std::shared_ptr<SSTable>> ss_table_cache_;
    };
}

#endif //SS_TABLE_MANAGER_H