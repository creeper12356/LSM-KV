//
// Created by creeper on 24-3-31.
//

#ifndef LSMKV_HANDOUT_SKIPLIST_H
#define LSMKV_HANDOUT_SKIPLIST_H

#include <cstdint>
#include <vector>
#include <string>
#include <cstdlib>
#include <list>

namespace skiplist {
    class Skiplist
    {
        class node;
    public:
        explicit Skiplist(double p = 0.5);
        ~Skiplist();

        std::string get(uint64_t key) const;
        void put(uint64_t key, const std::string &val);
        bool del(uint64_t key);
        void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);
        void reset();

    private:

        /**
         * @brief 查找结点
         * @param key 查找的键
         * @param is_found 是否查找成功
         * @return 在跳表中查找key，若查找成功，返回查找过程中第一个键为key的结点指针，
         * 若查找失败，返回底层链表中键值最大且小于key的结点指针（如果不存在这样的结点，则返回底层链表头结点）。
         * 如果is_found不为空，通过is_found返回是否查找成功。
         */
        node *get_node(uint64_t key, bool* is_found) const;
        
        /**
         * @brief 清空跳表
         * @details 回收所有动态内存，并清空m_headers.
         */
        void clear();


    private:
        class node {
        public:
            uint64_t key;
            std::string* val_ptr;
            node* prev;
            node* succ;
            node* above;
            node* below;
            bool is_header; // 是否为链表头节点
            bool is_owner; // val_ptr的动态内存是否归属于该节点

            node() ;
            explicit node(const uint64_t& key, const std::string& val, node* prev= nullptr, node* succ= nullptr, node* above= nullptr, node* below= nullptr);
            node(const uint64_t& key, std::string* val_ptr, node* prev= nullptr, node* succ= nullptr, node* above= nullptr, node* below= nullptr);
            ~node();
            std::string& val() const {return *val_ptr;}
        };

        std::vector<node*> m_headers;
        double m_probability;
    };

} // namespace skiplist

#endif //LSMKV_HANDOUT_SKIPLIST_H
