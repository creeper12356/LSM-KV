//
// Created by creeper on 24-3-31.
//
#include "skiplist.h"
#include "utils.h"
#include <cassert>

namespace skiplist {


    Skiplist::Skiplist(double p) : m_probability(p){
        // 加入头结点
        m_headers.push_back(new node);
    }
    void Skiplist::put(uint64_t key, const std::string &val) {
        assert(!m_headers.empty());

        node* cur_node = m_headers.back();
        while(true) {
            if(cur_node->is_header || cur_node->key < key) {
                if(!cur_node->succ || cur_node->succ->key > key) {
                    if(cur_node->below) {
                        //go down
                        cur_node = cur_node->below;
                    }
                    else {
                        // cur_node位于最底层
                        //新结点插入双链表
                        auto new_node = new node(key,val,cur_node,cur_node->succ);
                        cur_node->succ = new_node;
                        if(new_node->succ) {
                            new_node->succ->prev = new_node;
                        }
                        auto val_ptr = new_node->val_ptr;


                        cur_node = new_node;
                        //按照概率垂直增高
                        int new_level = 1;
                        while(utils::random() < m_probability ) {
                            // val_ptr浅拷贝
                            cur_node->above = new node(key, val_ptr, nullptr, nullptr,nullptr,cur_node);
                            cur_node = cur_node->above;
                            ++new_level;
                        }
                        //更新跳表高度
                        if(m_headers.size() < new_level) {
                            int count = new_level - m_headers.size();
                            while(count != 0) {
                                // m_headers剩余位置补齐头节点
                                auto new_header_node = new node;
                                new_header_node->below = m_headers.back();
                                m_headers.back()->above = new_header_node;

                                m_headers.push_back(new_header_node);
                                --count;
                            }
                        }

                        //逐层水平连接
                        //fix : bug
                        new_node = new_node->above;
                        for(int i = 1; i < new_level;++i) {
                            cur_node = m_headers[i];
                            while(true) {
                                if(cur_node->is_header || cur_node->key < key) {
                                    if(!cur_node->succ || cur_node->succ->key > key) {
                                        //新结点插入双链表
                                        new_node->prev = cur_node;
                                        new_node->succ = cur_node->succ;
                                        cur_node->succ = new_node;
                                        if(new_node->succ) {
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

                        return ;
                    }
                }
                else {
                    cur_node = cur_node->succ;
                }
            }
            else if(cur_node->key == key) {
                //find the key , then overwrite
                while(cur_node->below) {
                    cur_node = cur_node->below;
                }
                cur_node->val() = val;
                return ;
            }
            else {
                //cur_node->key > key
                // control never reaches here.
                assert(false);
            }
        }


    }
    bool Skiplist::del(uint64_t key) {
        bool is_found;
        node *res_node = get_node(key, &is_found);
        if(!is_found) {
            return false;
        }

        // 移动到最底层
        while(res_node->below) {
            res_node = res_node->below;
        }
        // 向上逐层删除结点
        node *deleted_node;
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
        return true;
    }
    
    void Skiplist::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
    {
        assert(key1 <= key2);
        list.clear();

        bool is_found;
        node *node1 = get_node(key1, &is_found);
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
        node *cur_node = node1;
        while(cur_node && cur_node->key <= key2) {
            list.emplace_back(cur_node->key, cur_node->val());
            cur_node = cur_node->succ;
        }
    }
    
    void Skiplist::reset()
    {
        this->clear();
        // 加入头结点
        m_headers.push_back(new node);
    }
    Skiplist::node* Skiplist::get_node(uint64_t key, bool* is_found) const
    {
        assert(!m_headers.empty()) ;

        node* cur_node = m_headers.back();
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
    
    void Skiplist::clear()
    {
        // 逐层删除链表
        for(auto header: m_headers) {
            node* cur = header;
            node* succ;
            while(cur) {
                succ = cur->succ;
                delete cur;
                cur = succ;
            }
        }
        m_headers.clear();
    }
    std::string Skiplist::get(uint64_t key) const {
        bool is_found;
        node *res_node = get_node(key, &is_found);
        if(is_found) {
            return res_node->val();
        } else {
            return "";
        }
    }

    Skiplist::~Skiplist() {
        this->clear();
    }


    Skiplist::node::node(const uint64_t& arg_key,
                              const std::string& arg_val,
                              Skiplist::node *arg_prev,
                              Skiplist::node *arg_succ,
                              Skiplist::node *arg_above,
                              Skiplist::node *arg_below)
            : key(arg_key),
              val_ptr(new std::string(arg_val)),
              prev(arg_prev),
              succ(arg_succ),
              above(arg_above),
              below(arg_below),
              is_header(false),
              is_owner(true) { }

    Skiplist::node::node(const uint64_t &arg_key,
                              std::string *arg_val_ptr,
                              Skiplist::node *arg_prev,
                              Skiplist::node *arg_succ,
                              Skiplist::node *arg_above,
                              Skiplist::node *arg_below)
            : key(arg_key),
              val_ptr(arg_val_ptr),
              prev(arg_prev),
              succ(arg_succ),
              above(arg_above),
              below(arg_below) ,
              is_header(false),
              is_owner(false) { }

    Skiplist::node::node() :
            key(0),
            val_ptr(nullptr),
            prev(nullptr),
            succ(nullptr),
            above(nullptr),
            below(nullptr),
            is_header(true),
            is_owner(false) { }

    Skiplist::node::~node() {
        if(is_owner) {
            delete val_ptr;
        }
    }

} // namespace skiplist
