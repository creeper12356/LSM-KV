//
// Created by creeper on 24-3-12.
//
#include <hash_map>
#include <cstring>
#include <iostream>
#include "bloom_filter.h"
#include "MurmurHash3.h"
namespace bloom_filter {
    BloomFilter::BloomFilter(int vector_size)
            : vector_size_(vector_size) {
        hash_vector_ = new bool[vector_size];
        memset(hash_vector_, 0, vector_size);
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
        // 将bool数组转换为比特流
        bool* cur = hash_vector_;
        int count = 0;
        unsigned char byte = 0;
        while(count < vector_size_) {
            if(*cur) {
                byte = byte | (1 << (count % 8));
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

    void BloomFilter::ReadFromFile(std::ifstream &fin) {
        // 将比特流转换为bool数组
        bool* cur = hash_vector_;
        int count = 0;
        char byte = 0;

        while(count < vector_size_) {
            if(count % 8 == 0) {
                // 一个字节中的第一个bit
                fin.read(&byte, 1);
            }
            *cur = (byte >> (count % 8)) & 0x1;
            ++count;
            ++cur;
        }
    }

    bool BloomFilter::operator==(const BloomFilter &other) const {
        if(this->vector_size_ != other.vector_size_) {
            std::cerr << "difference vector size" << std::endl;
            return false;
        }
        for(int i = 0;i < vector_size_;++i) {
            if(this->hash_vector_[i] != other.hash_vector_[i]) {
                std::cerr << "two bloom filter differs at " << i << std::endl;
                return false;
            }
        }
        return true;
    }

}


