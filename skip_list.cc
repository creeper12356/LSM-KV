//
// Created by creeper on 24-3-31.
//
#include "skip_list.h"
#include "bloom_filter.h"
#include "ss_table.h"
#include "utils.h"
#include <cassert>

namespace skip_list {
    SkipList::SkipList(double p)
    : probability_(p)
    , size_(0)
    {
        // 加入头结点
        headers_.push_back(new Node);
    }
    void SkipList::Put(uint64_t key, const std::string &val) {
        assert(!headers_.empty());

        Node* cur_node = headers_.back();
        while(true) {
            if (cur_node->is_header || cur_node->key < key) {
                if (!cur_node->succ || cur_node->succ->key > key) {
                    if (cur_node->below) {
                        //go down
                        cur_node = cur_node->below;
                    } else {
                        // cur_node位于最底层
                        //新结点插入双链表
                        auto new_node = new Node(key, val, cur_node, cur_node->succ);
                        cur_node->succ = new_node;
                        if (new_node->succ) {
                            new_node->succ->prev = new_node;
                        }
                        auto val_ptr = new_node->val_ptr;


                        cur_node = new_node;
                        //按照概率垂直增高
                        int new_level = 1;
                        while (utils::random() < probability_) {
                            // val_ptr浅拷贝
                            cur_node->above = new Node(key, val_ptr, nullptr, nullptr, nullptr, cur_node);
                            cur_node = cur_node->above;
                            ++new_level;
                        }
                        //更新跳表高度
                        if (headers_.size() < new_level) {
                            int count = new_level - headers_.size();
                            while (count != 0) {
                                // m_headers剩余位置补齐头节点
                                auto new_header_node = new Node;
                                new_header_node->below = headers_.back();
                                headers_.back()->above = new_header_node;

                                headers_.push_back(new_header_node);
                                --count;
                            }
                        }

                        //逐层水平连接
                        //fix : bug
                        new_node = new_node->above;
                        for (int i = 1; i < new_level; ++i) {
                            cur_node = headers_[i];
                            while (true) {
                                if (cur_node->is_header || cur_node->key < key) {
                                    if (!cur_node->succ || cur_node->succ->key > key) {
                                        //新结点插入双链表
                                        new_node->prev = cur_node;
                                        new_node->succ = cur_node->succ;
                                        cur_node->succ = new_node;
                                        if (new_node->succ) {
                                            new_node->succ->prev = new_node;
                                        }
                                        break;
                                    }
                                }
                                cur_node = cur_node->succ;
                            }
                            //update new_node
                            new_node = new_node->above;
                        }
                        ++ size_;
                        return;
                    }
                } else {
                    cur_node = cur_node->succ;
                }
            } else if (cur_node->key == key) {
                // 查找成功，直接覆盖
                while (cur_node->below) {
                    cur_node = cur_node->below;
                }
                cur_node->val() = val;
                return;
            } else {
                //cur_node->key > key
                // control never reaches here.
                assert(false);
            }
        }
    }
    bool SkipList::Del(uint64_t key) {
        bool is_found;
        Node *res_node = GetNode(key, &is_found);
        if(!is_found) {
            return false;
        }

        // 移动到最底层
        while(res_node->below) {
            res_node = res_node->below;
        }
        // 向上逐层删除结点
        Node *deleted_node;
        while(res_node) {
            if(res_node->prev) {
                res_node->prev->succ = res_node->succ;
            }
            if(res_node->succ) {
                res_node->succ->prev = res_node->prev;
            }
            deleted_node = res_node;
            res_node = res_node->above;
            delete deleted_node;
        }
        -- size_;
        return true;
    }
    
    void SkipList::Scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) const
    {
        assert(key1 <= key2);
        list.clear();

        bool is_found;
        Node *node1 = GetNode(key1, &is_found);
        if(is_found) {
            // 移动到最底层
            while(node1->below) {
                node1 = node1->below;
            }
        }
        else {
            node1 = node1->succ;
        }
        // 扫描链表，将键值对加入返回列表中
        Node *cur_node = node1;
        while(cur_node && cur_node->key <= key2) {
            list.emplace_back(cur_node->key, cur_node->val());
            cur_node = cur_node->succ;
        }
    }
    
    void SkipList::Reset()
    {
        this->Clear();
        headers_.push_back(new Node);
        size_ = 0;
    }
    SkipList::Node* SkipList::GetNode(uint64_t key, bool* is_found) const
    {
        assert(!headers_.empty()) ;

        Node* cur_node = headers_.back();
        while(true) {
            if(cur_node->is_header || cur_node->key < key) {
                if(!cur_node->succ || cur_node->succ->key > key) {
                    if(!cur_node->below) {
                        // 已经到达最底层，查找失败
                        is_found && (*is_found = false);
                        break;
                    }
                    else {
                        cur_node = cur_node->below;
                    }
                }
                else {
                    cur_node = cur_node->succ;
                }
            }
            else if(cur_node->key == key) {
                // 查找成功
                is_found && (*is_found = true);
                break;
            }
        }
        return cur_node;
    }
    
    void SkipList::Clear() {
        // 逐层删除链表
        for(auto header: headers_) {
            Node* cur = header;
            Node* succ;
            while(cur) {
                succ = cur->succ;
                delete cur;
                cur = succ;
            }
        }
        headers_.clear();
    }
    std::string SkipList::Get(uint64_t key) const {
        bool is_found;
        Node *res_node = GetNode(key, &is_found);
        if(is_found) {
            return res_node->val();
        } else {
            return "";
        }
    }

    SkipList::~SkipList() {
        this->Clear();
    }

    int SkipList::size() const {
        return size_;
    }

    void SkipList::InsertAllKeys(bloom_filter::BloomFilter *bloom_filter, ss_table::Header* ss_table_header) const {
        assert(!headers_.empty());
        ss_table_header->key_count = 0;
        Node *cur_node = headers_[0]->succ;
        if(!cur_node) {
            // 跳表为空
            return ;
        }

        ss_table_header->min_key = cur_node->key;

        // 遍历除最后一个的所有结点
        while(cur_node->succ) {
            bloom_filter->Insert(cur_node->key);
            cur_node = cur_node->succ;
            ++ ss_table_header->key_count;
        }

        // 处理最后一个结点
        ss_table_header->max_key = cur_node->key;
        bloom_filter->Insert(cur_node->key);
        ++ ss_table_header->key_count;
    }


    SkipList::Node::Node(const uint64_t& arg_key,
                         const std::string& arg_val,
                         SkipList::Node *arg_prev,
                         SkipList::Node *arg_succ,
                         SkipList::Node *arg_above,
                         SkipList::Node *arg_below)
            : key(arg_key),
              val_ptr(new std::string(arg_val)),
              prev(arg_prev),
              succ(arg_succ),
              above(arg_above),
              below(arg_below),
              is_header(false),
              is_owner(true) { }

    SkipList::Node::Node(const uint64_t &arg_key,
                         std::string *arg_val_ptr,
                         SkipList::Node *arg_prev,
                         SkipList::Node *arg_succ,
                         SkipList::Node *arg_above,
                         SkipList::Node *arg_below)
            : key(arg_key),
              val_ptr(arg_val_ptr),
              prev(arg_prev),
              succ(arg_succ),
              above(arg_above),
              below(arg_below) ,
              is_header(false),
              is_owner(false) { }

    SkipList::Node::Node() :
            key(0),
            val_ptr(nullptr),
            prev(nullptr),
            succ(nullptr),
            above(nullptr),
            below(nullptr),
            is_header(true),
            is_owner(false) { }

    SkipList::Node::~Node() {
        if(is_owner) {
            delete val_ptr;
        }
    }

} // namespace skip_list
