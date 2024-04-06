//
// Created by creeper on 24-3-12.
//
#include <hash_map>

#include "bloom_filter.h"
#include "MurmurHash3.h"
namespace bloom_filter {
    BloomFilter::BloomFilter(int vector_size)
            : vector_size_(vector_size) {
        hash_vector_ = new bool[vector_size];
    }


    void BloomFilter::Insert(uint64_t key) {
        uint32_t hash[4] = {};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(uint32_t res : hash) {
            hash_vector_[res % vector_size_] = true;
        }
    }

    bool BloomFilter::Search(uint64_t key) {
        uint32_t hash[4] = {};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(uint32_t res: hash) {
            if(!hash_vector_[res % vector_size_]) {
                return false;
            }
        }
        return true;
    }

    BloomFilter::~BloomFilter() {
        delete [] hash_vector_;
    }

    void BloomFilter::WriteToFile(std::ofstream& fout) {
        // 将0-1字符串转换为比特流
        bool* cur = hash_vector_;
        int count = 0;
        unsigned char byte = 0;
        while(count < vector_size_) {
            if(*cur) {
                byte = byte & (1 << count);
            }
            if(count % 8 == 7) {
                // 一个字节中的最后一个bit
                fout.write(reinterpret_cast<const char*>(&byte), 1);
                byte = 0;
            }
            ++count;
            ++cur;
        }
    }

}


