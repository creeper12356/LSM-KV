//
// Created by creeper on 24-3-31.
//

#ifndef LSMKV_HANDOUT_SKIP_LIST_H
#define LSMKV_HANDOUT_SKIP_LIST_H

#include <cstdint>
#include <vector>
#include <string>
#include <cstdlib>
#include <list>
namespace bloom_filter {
    class BloomFilter;
}
namespace ss_table {
    struct Header;
}
namespace skip_list {
    class SkipList
    {
        class Node;
    public:
        explicit SkipList(double p = 0.5);
        ~SkipList();

        std::string Get(uint64_t key) const;
        void Put(uint64_t key, const std::string &val);
        bool Del(uint64_t key);
        void Scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) const;
        void Reset();

        /**
         * @brief 将所有键插入Bloom过滤器
         * @param bloom_filter
         * @param ss_table_header 返回的SSTable的元数据
         */
        void InsertAllKeys(bloom_filter::BloomFilter* bloom_filter, ss_table::Header* ss_table_header) const;

        int size() const;

    private:

        /**
         * @brief 查找结点
         * @param key 查找的键
         * @param is_found 是否查找成功
         * @return 在跳表中查找key，若查找成功，返回查找过程中第一个键为key的结点指针，
         * 若查找失败，返回底层链表中键值最大且小于key的结点指针（如果不存在这样的结点，则返回底层链表头结点）。
         * 如果is_found不为空，通过is_found返回是否查找成功。
         */
        Node *GetNode(uint64_t key, bool* is_found) const;
        
        /**
         * @brief 清空跳表
         * @details 回收所有动态内存，并清空m_headers.
         */
        void Clear();


    private:
        class Node {
        public:
            uint64_t key;
            std::string* val_ptr;
            Node* prev;
            Node* succ;
            Node* above;
            Node* below;
            bool is_header; // 是否为链表头节点
            bool is_owner; // val_ptr的动态内存是否归属于该节点

            Node() ;
            explicit Node(const uint64_t& key, const std::string& val, Node* prev= nullptr, Node* succ= nullptr, Node* above= nullptr, Node* below= nullptr);
            Node(const uint64_t& key, std::string* val_ptr, Node* prev= nullptr, Node* succ= nullptr, Node* above= nullptr, Node* below= nullptr);
            ~Node();
            std::string& val() const {return *val_ptr;}
        };

    private:
        std::vector<Node*> headers_;
        double probability_;
        int size_;
    };

} // namespace skip_list

#endif //LSMKV_HANDOUT_SKIP_LIST_H
