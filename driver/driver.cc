#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <memory>
#include <list>
#include <optional>
#include <queue>
#include "../kvstore.h"

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
            std::string value;
            std::cin >> key;
            std::getline(std::cin, value);
            value = value.substr(1); // Remove leading space
            kvstore.put(key, value);
        } else if (command == "get") {
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
        } else if (command == "reset") {
            kvstore.reset();
        } else if (command == "exit") {
            break;
        } else {
            std::cout << "Unknown command" << std::endl;
        }
    }

    return 0;
}