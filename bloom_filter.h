//
// Created by creeper on 24-3-12.
//

#ifndef HW2_BLOOM_FILTER_H
#define HW2_BLOOM_FILTER_H
#include <cstdint>
#include <fstream>
namespace bloom_filter {
    class BloomFilter {
    public:
        BloomFilter(int vector_size, int hash_count);
        ~BloomFilter();
    public:

        /**
         * Bloom 过滤器使用的Hash函数
         * @param key 查找和插入的键
         * @param hash_index Hash函数序号，从1-hash_count_
         * @return Hash后的值，从0-vector_size - 1
         */
        int Hash(uint64_t key, int hash_index);
        /**
         * 插入键
         * @param key
         */
        void Insert(uint64_t key);
        /**
         * 查找键
         * @param key
         * @return
         */
        bool Search(uint64_t key);

        /**
         * 将Bloom 过滤器的向量写入文件
         * @param fout 文件输出流
         */
        void WriteToFile(std::ofstream &fout);

    private:
        int vector_size_;
        int hash_count_;
        bool* hash_vector_;

    };
}

#endif //HW2_BLOOM_FILTER_H
