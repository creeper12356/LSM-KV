#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <memory>
#include <list>
#include <optional>
#include <queue>
#include "../kvstore.h"
std::string gen_value(uint64_t key) {
    return "s" + std::to_string(key);
}
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <directory> <vlog>" << std::endl;
        return 1;
    }

    std::string dir = argv[1];
    std::string vlog = argv[2];
    KVStore kvstore(dir, vlog);

    std::string command;
    while (true) {
        std::cout << "> ";
        std::cin >> command;

        if (command == "put") {
            uint64_t key;
            std::cin >> key;
            kvstore.put(key, gen_value(key));
        } else if (command == "putrange") {
            uint64_t start, end, step;
            std::cin >> start >> end >> step;
            for(uint64_t i = start; i <= end; i += step) {
                kvstore.put(i, gen_value(i));
            }
        } 
        else if (command == "get") {
            uint64_t key;
            std::cin >> key;
            std::string value = kvstore.get(key);
            if (value.empty()) {
                std::cout << "Not found" << std::endl;
            } else {
                std::cout << value << std::endl;
            }
        } else if (command == "del") {
            uint64_t key;
            std::cin >> key;
            bool result = kvstore.del(key);
            if (!result) {
                std::cout << "Not found" << std::endl;
            }
        } else if(command == "delrange") {
            uint64_t start, end, step;
            std::cin >> start >> end >> step;
            int found_count = 0;
            int not_found_count = 0;
            for(uint64_t i = start; i <= end; i += step) {
                kvstore.del(i) ? ++found_count : ++not_found_count;
            }
            std::cout << "Found: " << found_count << ", Not found: " << not_found_count << std::endl;
        } 
        else if (command == "reset") {
            kvstore.reset();
        } else if (command == "exit") {
            break;
        } else if (command == "getall") {
            uint64_t key;
            std::cin >> key;
            kvstore.get_everywhere(key);
        } 
        else {
            std::cout << "Unknown command" << std::endl;
        }
    }

    return 0;
}