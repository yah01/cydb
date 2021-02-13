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
#include <tuple>
#include <unistd.h>
#include <fcntl.h>

#include "engines/kv_engine.hpp"
#include "context.hpp"

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
            BTreeNode *node;
            std::tie(node, std::ignore) = go_to_leaf(key);

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
            auto [node, parent_map] = go_to_leaf(key);
            // there is still enough space
            size_t index = node->find_value_index(key);
            if (index < node->data_num() && node->key_value_cell(index).compare_by_key(key) == 0) // exist, update value
            {
                node->update_value(index, value); // todo: handle split
            }
            else
            {
                if (node->insert_value(key, value) == 0)
                {
                    split(node, parent_map);
                }
                buffer_manager.metadata.data_num++;
            }

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus remove(const std::string &key)
        {
            BTreeNode *node;
            std::tie(node, std::ignore) = go_to_leaf(key);

            size_t index = node->find_value_index(key);
            if (index < node->data_num() && node->key_value_cell(index).compare_by_key(key) == 0)
                node->remove(index);
            else
                return OpStatus(OpError::KeyNotFound);

            buffer_manager.metadata.data_num--;
            return OpStatus(OpError::Ok);
        };

        virtual OpStatus scan(const std::string &start_key, const std::string &end_key){

        };

    private:
        // return the highest effected node id
        uint32_t split(BTreeNode *node, std::unordered_map<uint32_t, uint32_t> &parent_map)
        {
            buffer_manager.pin(node->page_id);

            uint32_t sibling_id = buffer_manager.allocate_page(node->cell_type());
            int32_t parent_id = -1;
            if (auto it = parent_map.find(node->page_id); it != parent_map.end())
                parent_id = it->second;

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
                return split(parent, parent_map);
            }
            buffer_manager.unpin(parent_id);
            return parent_id;
        }

        // bool is_root(BTreeNode *node) const { return buffer_manager.ask_parent(node->page_id) == -1; }

        std::tuple<BTreeNode *, std::unordered_map<uint32_t, uint32_t>> go_to_leaf(const std::string &key)
        {
            BTreeNode *node = buffer_manager.get_root();
            return go_to_leaf(node, key);
        }

        std::tuple<BTreeNode *, std::unordered_map<uint32_t, uint32_t>> go_to_leaf(BTreeNode *node, const std::string &key)
        {
            std::unordered_map<uint32_t, uint32_t> parent_map;

            // node is not a leaf
            while (node->cell_type() == CellType::KeyCell)
            {
                int32_t child = node->find_child(key);
                parent_map[child] = node->page_id;

                node = buffer_manager.get(child);
            }

            return std::make_tuple(node, std::move(parent_map));
        }

        Metadata metadata;
        BufferManager buffer_manager;
    };
} // namespace cyber