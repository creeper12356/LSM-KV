//
// Created by creeper on 24-3-12.
//
#include <hash_map>

#include "bloom_filter.h"
#include "MurmurHash3.h"
namespace bloom_filter {
    BloomFilter::BloomFilter(int vector_size, int hash_count)
            : vector_size_(vector_size),
              hash_count_(hash_count) {
        hash_vector_ = new bool[vector_size];
    }

    int BloomFilter::Hash(uint64_t key, int hash_index) {
        uint64_t hash[2] = {};
        MurmurHash3_x64_128(&key, sizeof(key), hash_index, hash);
        return hash[1] % vector_size_ ;
    }

    void BloomFilter::Insert(uint64_t key) {
        for(int i = 1;i <= hash_count_; ++i) {
            hash_vector_[Hash(key, i)] = true;
        }
    }

    bool BloomFilter::Search(uint64_t key) {
        for(int i = 1;i <= hash_count_; ++i) {
            if(!hash_vector_[Hash(key, i)]) {
                return false;
            }
        }
        return true;
    }

    BloomFilter::~BloomFilter() {
        delete [] hash_vector_;
    }

    void BloomFilter::WriteToFile(std::ofstream& fout) {
        fout.write((char*)(hash_vector_), vector_size_);
    }

}


