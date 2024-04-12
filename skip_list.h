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
#include <cassert>

namespace ss_table {
    struct Header;
    struct KeyOffsetVlenTuple;
}
namespace v_log {
    class VLog;
}
namespace skip_list {
    class SkipList
    {
    public:
        class Node;
        class Iterator;
    public:
        explicit SkipList(double p = 0.5);
        ~SkipList();

        std::string Get(uint64_t key) const;
        void Put(uint64_t key, const std::string &val);
        bool Del(uint64_t key);
        void Scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) const;
        void Reset();

        /**
         * 底层链表头节点指针
         * @return
         */
        Node *header() const;
        Iterator begin() const {
            return Iterator(headers_[0]->succ_);
        }
        Iterator end() const {
            return Iterator(nullptr);
        }


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
         * @brief 将结点插入水平链表
         * @param inserted 被插入结点
         * @param header 被插入链表的头结点
         */
        static void InsertNodeHorizonally(Node *inserted, Node* header);

        /**
         * @brief 将结点作为后继插入
         * @details 将结点inserted插入结点prev和prev的后继结点之间。
         * @param inserted 被插入结点
         * @param prev 被插入结点前驱
         */
        static void InsertSuccNode(Node *inserted, Node *prev);

        /**
         * @brief 依概率垂直向上生长
         * @param base_node 生长的基结点
         * @param growth_probability 生长概率
         * @return 基结点到顶部结点的路径高度
         */
        static int GrowUpVertically(Node *base_node, double growth_probability);

        /**
         * @brief 更新跳表的高度
         * @param height 更新后的跳表高度
         * @return 是否更新了高度
         */
        bool UpdateHeight(int height);
        
        /**
         * @brief 清空跳表
         * @details 回收所有动态内存，并清空m_headers.
         */
        void Clear();


    public:
        class Node {
            friend class SkipList;
        public:
            Node() ;
            explicit Node(const uint64_t& key, const std::string& val, Node* prev= nullptr, Node* succ= nullptr, Node* above= nullptr, Node* below= nullptr);
            Node(const uint64_t& key, std::string* val_ptr, Node* prev= nullptr, Node* succ= nullptr, Node* above= nullptr, Node* below= nullptr);
            ~Node();
            uint64_t key() const { return key_; }
            std::string& val() const {return *val_ptr_;}
            Node *prev() const { return prev_; }
            Node *succ() const { return succ_; }
            Node *above() const { return above_; }
            Node *below() const { return below_; }
            bool is_header() const { return is_header_; }
            bool is_owner() const { return is_owner_; }

        private:
            uint64_t key_;
            std::string* val_ptr_;
            Node* prev_;
            Node* succ_;
            Node* above_;
            Node* below_;
            bool is_header_; // 是否为链表头节点
            bool is_owner_; // val_ptr的动态内存是否归属于该节点
        };
        class Iterator {
        public:
            explicit Iterator(Node *cur_node): cur_node_(cur_node) {}
            Iterator& operator++() {
                assert(cur_node_);
                cur_node_ = cur_node_->succ_;
                return *this;
            }
            Iterator Previous() const {
                assert(cur_node_);
                return Iterator(cur_node_->prev_);
            }
            Iterator Next() const {
                assert(cur_node_);
                return Iterator(cur_node_->succ_);
            }
            Node &operator*() const {
                assert(cur_node_);
                return *cur_node_;
            }
            bool operator==(const Iterator& other) {
                return cur_node_ == other.cur_node_;
            }
            bool operator!=(const Iterator& other) {
                return cur_node_ != other.cur_node_;
            }

        private:
            Node *cur_node_;
        };

    private:
        std::vector<Node*> headers_;
        double probability_;
        int size_;
    };

} // namespace skip_list

#endif //LSMKV_HANDOUT_SKIP_LIST_H
