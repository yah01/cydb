#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include <filesystem>
#include <iostream>
#include <cstring>
#include <unordered_map>
#include <list>
#include <cassert>
#include <algorithm>
#include <memory>
#include <cstdio>
#include <unistd.h>
#include <fcntl.h>

#include "../kv_engine.hpp"

#include "page.hpp"
#include "buffer_manager.hpp"

namespace cyber
{
    class BTree : public KvEngine
    {
    public:
        virtual ~BTree()
        {
        }

        virtual OpStatus open(const char *path)
        {
            return buffer_manager.open(path);
        };

        virtual OpStatus get(const std::string &key)
        {
            BTreeNode *node = go_to_leaf(key);

            size_t index = node->find_index(key);
            if (index > node->data_num()) // no data in the node
            {
                return OpStatus(OpError::KeyNotFound);
            }
            
            KeyValueCell kv_cell(node->key_value_cell(index));
            if (kv_cell.compare_by_key(key) != 0) // key not found
            {
                return OpStatus(OpError::KeyNotFound);
            }
            return OpStatus(OpError::Ok, kv_cell.build_value_string());
        };

        virtual OpStatus set(const std::string &key, std::string value)
        {
            BTreeNode *node = go_to_leaf(key);

            // there is still enough space
            size_t index = node->find_index(key);
            if (index < node->data_num() && node->key_value_cell(index).compare_by_key(key) == 0) // exist, update value
                node->update_value(value, index);
            else
                node->insert_value(key, value);

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus remove(const std::string &key)
        {
            BTreeNode *node = go_to_leaf(key);

            size_t index = node->find_index(key);
            if (index < node->data_num() && node->key_value_cell(index).compare_by_key(key) == 0)
                node->remove(index);
            else
                return OpStatus(OpError::KeyNotFound);

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus scan(const std::string &start_key, const std::string &end_key){
            
        };

    private:
        void split(BTreeNode *node)
        {
            uint32_t sibling_id = buffer_manager.allocate_page(node->cell_type());
            BTreeNode *sibling = buffer_manager.get(sibling_id);

            for (int i = 0; i < node->data_num(); i++)
            {
            }

            if (is_root(node))
            {
                uint32_t new_root = buffer_manager.allocate_page(CellType::KeyCell);
            }
        }

        bool is_root(BTreeNode *node) const { return buffer_manager.ask_parent(node->page_id) == -1; }

        BTreeNode *go_to_leaf(const std::string &key)
        {
            BTreeNode *node = buffer_manager.get(0);

            // node is not a leaf
            while (node->cell_type() == CellType::KeyCell)
                node = buffer_manager.get(node->find_child(key));

            return node;
        }
        BufferManager buffer_manager;
    };
} // namespace cyber