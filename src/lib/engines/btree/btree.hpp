#pragma once

#include <list>
#include <algorithm>
#include <tuple>

#include "engines/kv_engine.hpp"

#include "page.hpp"
#include "buffer_manager.hpp"

namespace cyber
{
    class BTree : public KvEngine
    {
    public:
        virtual OpStatus open(const char *dir_path)
        {
            return buffer_manager.open(dir_path);
        };

        virtual OpStatus get(std::string_view key)
        {
            BTreeNode *node;
            std::tie(node, std::ignore) = go_to_leaf(key);

            num_t index = node->find_value_index(key);
            if (index >= node->data_num()) // no data in the node
            {
                return OpStatus(OpError::KeyNotFound);
            }

            KeyValueCell kvcell(node->key_value_cell(index));
            if (kvcell != key) // key not found
            {
                return OpStatus(OpError::KeyNotFound);
            }
            return OpStatus(OpError::Ok, kvcell.value_string());
        };

        virtual OpStatus set(std::string_view key, std::string value)
        {
            auto [node, parent_map] = go_to_leaf(key);
            // there is still enough space
            num_t index = node->find_value_index(key);
            if (index < node->data_num() && node->key_value_cell(index) == key)
            {
                // new value length is greater than the old value's
                // and the node has no enough free space
                while (node->update_value(index, value) == std::nullopt)
                {
                    id_t node_id = split(node, parent_map);
                    std::tie(node, parent_map) = go_to_leaf(buffer_manager.get(node_id), key);
                }
            }
            else
            {
                while (node->insert_value(key, value) == std::nullopt)
                {
                    id_t node_id = split(node, parent_map);
                    std::tie(node, parent_map) = go_to_leaf(buffer_manager.get(node_id), key);
                }
                buffer_manager.metadata.data_num++;
            }

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus remove(std::string_view key)
        {
            BTreeNode *node;
            std::tie(node, std::ignore) = go_to_leaf(key);

            num_t index = node->find_value_index(key);
            if (index < node->data_num() && key == node->key_value_cell(index))
                node->remove(index);
            else
                return OpStatus(OpError::KeyNotFound);

            buffer_manager.metadata.data_num--;
            return OpStatus(OpError::Ok);
        };

        virtual OpStatus scan(std::string_view start_key, std::string_view end_key)
        {
            return OpStatus(OpError::Internal);
        };

        Metadata &metadata() { return buffer_manager.metadata; }

    private:
        // BTree operations
        // return the highest effected node id
        id_t split(BTreeNode *node, std::unordered_map<uint32_t, uint32_t> &parent_map)
        {
            buffer_manager.pin(node->page_id);

            id_t node_id = node->page_id;
            id_t sibling_id = buffer_manager.allocate_page(node->type());

            buffer_manager.pin(sibling_id);
            BTreeNode *sibling = buffer_manager.get(sibling_id);

            num_t n = node->data_num();
            std::string key;
            uint32_t index = n / 2 + 1;
            for (auto i : iota(index, n))
            {
                if (node->type() == CellType::KeyCell)
                {
                    KeyCell kcell(node->key_cell(index));
                    if (i == index)
                        key = kcell.key_string();
                    else
                        sibling->insert_child(kcell.key_string(), kcell.child());
                }
                else
                {
                    KeyValueCell kvcell(node->key_value_cell(index));
                    sibling->insert_value(kvcell.key_string(), kvcell.value_string());
                    if (i == index)
                        key = kvcell.key_string();
                }
                node->remove(index);
            }

            bool is_root = false;
            id_t parent_id = 0;
            if (auto it = parent_map.find(node_id); it != parent_map.end())
                parent_id = it->second;
            else
            {
                is_root = true;
                parent_id = buffer_manager.allocate_page(CellType::KeyCell);
                buffer_manager.metadata.root_id = parent_id;
            }

            buffer_manager.pin(parent_id);
            BTreeNode *parent = buffer_manager.get(parent_id);
            if (is_root)
                parent->rightmost_child() = sibling_id;
            else
            {
                if (node_id == parent->rightmost_child())
                    parent->rightmost_child() = sibling_id;
                else
                    parent->update_child(parent->find_child_index(key), sibling_id);
            }

            buffer_manager.unpin(node_id);
            buffer_manager.unpin(sibling_id);

            if (parent->insert_child(key, node_id) == std::nullopt)
            {
                uint32_t anc = split(parent, parent_map);
                buffer_manager.get(parent_id)->insert_child(key, node_id);
                parent->insert_child(key, node_id);
                return anc;
            }
            buffer_manager.unpin(parent_id);
            return parent_id;
        }
        // utils
        std::tuple<BTreeNode *, std::unordered_map<uint32_t, uint32_t>> go_to_leaf(std::string_view key)
        {
            BTreeNode *node = buffer_manager.get_root();
            return go_to_leaf(node, key);
        }
        std::tuple<BTreeNode *, std::unordered_map<uint32_t, uint32_t>> go_to_leaf(BTreeNode *node, std::string_view key)
        {
            std::unordered_map<uint32_t, uint32_t> parent_map;

            // node is not a leaf
            while (node->type() == CellType::KeyCell)
            {
                int32_t child = node->find_child(key);
                parent_map[child] = node->page_id;

                node = buffer_manager.get(child);
            }

            return std::make_tuple(node, std::move(parent_map));
        }

        BufferManager buffer_manager;
    };
} // namespace cyber