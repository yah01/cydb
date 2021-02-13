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
    struct Metadata
    {
        uint32_t root_id;
        uint32_t node_num;
        uint32_t data_num;
    };

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
            BTreeNode *node = go_to_leaf(0, key);

            size_t index = node->find_value_index(key);
            if (index >= node->data_num()) // no data in the node
            {
                return OpStatus(OpError::KeyNotFound);
            }

            KeyValueCell kvcell(node->key_value_cell(index));
            if (kvcell.compare_by_key(key) != 0) // key not found
            {
                return OpStatus(OpError::KeyNotFound);
            }
            return OpStatus(OpError::Ok, kvcell.build_value_string());
        };

        virtual OpStatus set(const std::string &key, std::string value)
        {
            BTreeNode *node = go_to_leaf(0, key);

            // there is still enough space
            size_t index = node->find_value_index(key);
            if (index < node->data_num() && node->key_value_cell(index).compare_by_key(key) == 0) // exist, update value
                node->update_value(index, value);
            else
            {
                if (node->insert_value(key, value) == 0)
                {
                    split(node);
                }
            }

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus remove(const std::string &key)
        {
            BTreeNode *node = go_to_leaf(0, key);

            size_t index = node->find_value_index(key);
            if (index < node->data_num() && node->key_value_cell(index).compare_by_key(key) == 0)
                node->remove(index);
            else
                return OpStatus(OpError::KeyNotFound);

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus scan(const std::string &start_key, const std::string &end_key){

        };

    private:
        // return the highest effected node id
        uint32_t split(BTreeNode *node)
        {
            buffer_manager.pin(node->page_id);

            uint32_t sibling_id = buffer_manager.allocate_page(node->cell_type());
            uint32_t parent_id = buffer_manager.ask_parent(node->page_id);

            buffer_manager.pin(sibling_id);
            BTreeNode *sibling = buffer_manager.get(sibling_id);
            size_t n = node->data_num();
            std::string key;
            for (int i = n / 2 + 1; i < n; i++)
            {
                if (node->cell_type() == CellType::KeyCell)
                {
                    KeyCell kcell(node->key_cell(i));
                    sibling->insert_child(kcell.build_key_string(), kcell.child());
                    if (i == n / 2 + 1)
                        key = kcell.build_key_string();
                }
                else
                {
                    KeyValueCell kvcell(node->key_value_cell(i));
                    sibling->insert_value(kvcell.build_key_string(), kvcell.build_value_string());
                    if (i == n / 2 + 1)
                        key = kvcell.build_key_string();
                }
                node->remove(i);
            }

            if (parent_id == -1)
                parent_id = buffer_manager.allocate_page(CellType::KeyCell);
            buffer_manager.pin(parent_id);
            BTreeNode *parent = buffer_manager.get(parent_id);
            parent->update_child(parent->find_child_index(key), sibling_id);
            buffer_manager.unpin(sibling_id);
            buffer_manager.unpin(node->page_id);

            if (parent->insert_child(key, node->page_id) == WriteError::Failed)
            {
                return split(parent);
            }
            buffer_manager.unpin(parent_id);
            return parent_id;
        }

        bool is_root(BTreeNode *node) const { return buffer_manager.ask_parent(node->page_id) == -1; }

        BTreeNode *go_to_leaf(uint32_t &&page_id, const std::string &key)
        {
            BTreeNode *node = buffer_manager.get(page_id);

            // node is not a leaf
            while (node->cell_type() == CellType::KeyCell)
                node = buffer_manager.get(node->find_child(key));

            return node;
        }

        Metadata metadata;
        BufferManager buffer_manager;
    };
} // namespace cyber