//
// Created by creeper on 24-3-31.
//
#include "skip_list.h"
#include "ss_table.h"
#include "v_log.h"
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
        bool is_found;
        auto res_node = GetNode(key, &is_found);
        if(is_found) {
            // 查找成功，直接覆盖
            while (res_node->below_) {
                res_node = res_node->below_;
            }
            res_node->val() = val;
        }
        else {
            // 未查找成功，插入新结点
            auto new_node = new Node(key, val);
            InsertSuccNode(new_node, res_node);
            int height = GrowUpVertically(new_node, probability_);
            UpdateHeight(height);
            new_node = new_node->above_;
            for (int i = 1; i < height; ++i) {
                InsertNodeHorizonally(new_node, headers_[i]);
                new_node = new_node->above_;
            }
            ++ size_;
        }
    }
    bool SkipList::Del(uint64_t key) {
        bool is_found;
        Node *res_node = GetNode(key, &is_found);
        if(!is_found) {
            return false;
        }

        // 移动到最底层
        while(res_node->below_) {
            res_node = res_node->below_;
        }
        // 向上逐层删除结点
        Node *deleted_node;
        while(res_node) {
            if(res_node->prev_) {
                res_node->prev_->succ_ = res_node->succ_;
            }
            if(res_node->succ_) {
                res_node->succ_->prev_ = res_node->prev_;
            }
            deleted_node = res_node;
            res_node = res_node->above_;
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
            while(node1->below_) {
                node1 = node1->below_;
            }
        }
        else {
            node1 = node1->succ_;
        }
        // 扫描链表，将键值对加入返回列表中
        Node *cur_node = node1;
        while(cur_node && cur_node->key_ <= key2) {
            list.emplace_back(cur_node->key_, cur_node->val());
            cur_node = cur_node->succ_;
        }
    }
    
    void SkipList::Reset()
    {
        this->Clear();
        headers_.push_back(new Node);
    }
    SkipList::Node* SkipList::GetNode(uint64_t key, bool* is_found) const
    {
        assert(!headers_.empty()) ;

        Node* cur_node = headers_.back();
        while(true) {
            if(cur_node->is_header_ || cur_node->key_ < key) {
                if(!cur_node->succ_ || cur_node->succ_->key_ > key) {
                    if(!cur_node->below_) {
                        // 已经到达最底层，查找失败
                        is_found && (*is_found = false);
                        break;
                    }
                    else {
                        cur_node = cur_node->below_;
                    }
                }
                else {
                    cur_node = cur_node->succ_;
                }
            }
            else if(cur_node->key_ == key) {
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
                succ = cur->succ_;
                delete cur;
                cur = succ;
            }
        }
        headers_.clear();
        this->size_ = 0;
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

    SkipList::Node *SkipList::header() const {
        assert(!headers_.empty());
        return headers_[0];
    }

    void SkipList::InsertSuccNode(SkipList::Node *inserted, SkipList::Node *prev) {
        assert(inserted && prev);
        inserted->prev_ = prev;
        inserted->succ_ = prev->succ_;
        prev->succ_ = inserted;
        if(inserted->succ_) {
            inserted->succ_->prev_ = inserted;
        }
    }

    int SkipList::GrowUpVertically(SkipList::Node *base_node, double growth_probability) {
        Node *cur_node = base_node;
        int height = 1;
        while (utils::random() < growth_probability) {
            // 浅拷贝base_node.val_ptr
            cur_node->above_ = new Node(base_node->key_, base_node->val_ptr_, nullptr, nullptr, nullptr, cur_node);
            cur_node = cur_node->above_;
            ++ height;
        }
        return height;
    }

    bool SkipList::UpdateHeight(int height) {
        if(headers_.size() >= height) {
            return false;
        }

        int count = height - headers_.size();
        while (count != 0) {
            // m_headers剩余位置补齐头节点
            auto new_header_node = new Node;
            new_header_node->below_ = headers_.back();
            headers_.back()->above_ = new_header_node;

            headers_.push_back(new_header_node);
            --count;
        }
        return true;
    }

    void SkipList::InsertNodeHorizonally(SkipList::Node *inserted, SkipList::Node *header) {
        assert(inserted && header);
        Node *cur_node = header;
        while (true) {
            if (cur_node->is_header_ || cur_node->key_ < inserted->key_) {
                if (!cur_node->succ_ || cur_node->succ_->key_ > inserted->key_) {
                    InsertSuccNode(inserted, cur_node);
                    break;
                }
            }
            cur_node = cur_node->succ_;
        }
    }



    SkipList::Node::Node(const uint64_t& arg_key,
                         const std::string& arg_val,
                         SkipList::Node *arg_prev,
                         SkipList::Node *arg_succ,
                         SkipList::Node *arg_above,
                         SkipList::Node *arg_below)
            : key_(arg_key),
              val_ptr_(new std::string(arg_val)),
              prev_(arg_prev),
              succ_(arg_succ),
              above_(arg_above),
              below_(arg_below),
              is_header_(false),
              is_owner_(true) { }

    SkipList::Node::Node(const uint64_t &arg_key,
                         std::string *arg_val_ptr,
                         SkipList::Node *arg_prev,
                         SkipList::Node *arg_succ,
                         SkipList::Node *arg_above,
                         SkipList::Node *arg_below)
            : key_(arg_key),
              val_ptr_(arg_val_ptr),
              prev_(arg_prev),
              succ_(arg_succ),
              above_(arg_above),
              below_(arg_below) ,
              is_header_(false),
              is_owner_(false) { }

    SkipList::Node::Node() :
            key_(0),
            val_ptr_(nullptr),
            prev_(nullptr),
            succ_(nullptr),
            above_(nullptr),
            below_(nullptr),
            is_header_(true),
            is_owner_(false) { }

    SkipList::Node::~Node() {
        if(is_owner_) {
            delete val_ptr_;
        }
    }

} // namespace skip_list
