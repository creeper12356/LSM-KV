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
        explicit BloomFilter(int vector_size);
        ~BloomFilter();
    public:

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
        bool* hash_vector_;

    };
}

#endif //HW2_BLOOM_FILTER_H
