#pragma once

#include <cstdint>
#include <string>
#include <cstring>
#include <vector>
#include <filesystem>
#include <iostream>
#include <cstring>
#include <unordered_map>
#include <cassert>
#include <algorithm>
#include <memory>
#include <cstdio>
#include <unistd.h>
#include <fcntl.h>

#include "kv_engine.hpp"

namespace cyber
{
    constexpr uint64_t PAGE_SIZE = 16 << 10; // 16KiB

    enum class CellType : uint8_t
    {
        KeyCell = 1,
        KeyValueCell = 2,
    };

    struct KeyCellHeader
    {
        uint16_t key_size;
        uint32_t child_id;

        KeyCellHeader(const std::string &key, const uint32_t &child_id) : key_size(key.length()),
                                                                          child_id(child_id) {}

        KeyCellHeader(char *cell)
        {
            std::memcpy(this, cell, sizeof(KeyCellHeader));
        }
    };

    struct KeyValueCellHeader
    {
        uint16_t key_size;
        uint32_t value_size;

        KeyValueCellHeader(const std::string &key, const std::string &value) : key_size(key.length()),
                                                                               value_size(value.length()) {}

        KeyValueCellHeader(char *cell)
        {
            memcpy(this, cell, sizeof(KeyValueCellHeader));
        }
    };

    struct PageHeader
    {
        uint64_t checksum;
        CellType type;

        uint16_t data_num;
        uint64_t cell_end; // Cells grow left
        uint32_t rightmost_child;

        uint64_t header_checksum()
        {
            uint64_t *header = (uint64_t *)this;
            uint64_t checksum = 0;
            int n = sizeof(PageHeader) / 8;
            for (int i = 1; i < n; i++)
                checksum ^= header[i];
            return checksum;
        }

        inline uint32_t last_pointer_pos() const
        {
            return sizeof(PageHeader) + data_num * sizeof(uint32_t);
        }

        inline uint64_t free_space() const
        {
            return cell_end - last_pointer_pos();
        }
    };

    constexpr uint64_t KEY_CELL_HEADER_SIZE = sizeof(KeyCellHeader),
                       KEY_VALUE_CELL_HEADER_SIZE = sizeof(KeyValueCellHeader),
                       PAGE_HEADER_SIZE = sizeof(PageHeader); // Must be multiple of 8

    inline char *page_cell(char *page, uint32_t i)
    {
        return page + ((uint32_t *)(page + PAGE_HEADER_SIZE))[i];
    }

    class BTreeNode
    {
    public:
        uint32_t page_id;
        PageHeader header;

        std::vector<std::string> keys;
        std::vector<std::string> values;
        std::vector<uint32_t> children;

        // BTreeNode(uint32_t page_id) : page_id(page_id) {}
        BTreeNode(uint32_t page_id, char *buf) : page_id(page_id)
        {
            memcpy(&header, buf, PAGE_HEADER_SIZE);
            init_check(buf);
            init_read_data(buf);
        }

        // ~BTreeNode()
        // {
        //     delete[] page;
        // }

        uint32_t find_child(const std::string &key) const
        {
            // auto pointers = buffer_manager.load_pointers(page_id, header.data_num);
            int index = std::lower_bound(keys.begin(), keys.end(), key) - keys.begin();
            if (index >= keys.size())
                return header.rightmost_child;

            return children[index];
        }

        size_t find_value_index(const std::string &key) const
        {
            size_t index = std::lower_bound(keys.begin(), keys.end(), key) - keys.begin();
            return index;
        }

        void update_with_index(std::string &&value, size_t index)
        {
            values[index] = std::move(value);
            // todo modify this->page
        }

        void insert_with_index(const std::string &key, std::string &&value, size_t index)
        {
            keys.insert(keys.begin() + index, key);
            values.insert(values.begin() + index, std::move(value));
            header.data_num++;
            // todo modify this->page
        }

        char *serialize()
        {
            static char buf[PAGE_SIZE];
            memset(buf, 0, PAGE_SIZE);

            uint32_t *pointers = (uint32_t *)(buf + PAGE_HEADER_SIZE);

            char *cell_end = buf + PAGE_SIZE;
            for (int i = 0; i < header.data_num; i++)
            {
                if (header.type == CellType::KeyCell)
                {
                    KeyCellHeader key_cell(keys[i], children[i]);
                    std::memcpy(cell_end - KEY_CELL_HEADER_SIZE - keys[i].length(),
                                &key_cell,
                                KEY_CELL_HEADER_SIZE);
                    std::memcpy(cell_end - keys[i].length(),
                                keys[i].c_str(),
                                keys[i].length());
                    cell_end -= KEY_CELL_HEADER_SIZE + keys[i].length();
                    pointers[i] = cell_end - buf;
                }
                else
                {
                    KeyValueCellHeader kv_cell(keys[i], values[i]);
                    std::memcpy(cell_end - KEY_VALUE_CELL_HEADER_SIZE - keys[i].length() - values[i].length(),
                                &kv_cell,
                                KEY_VALUE_CELL_HEADER_SIZE);
                    std::memcpy(cell_end - keys[i].length() - values[i].length(),
                                keys[i].c_str(),
                                keys[i].length());
                    std::memcpy(cell_end - values[i].length(),
                                values[i].c_str(),
                                values[i].length());
                    cell_end -= KEY_VALUE_CELL_HEADER_SIZE + keys[i].length() + values[i].length();
                    pointers[i] = cell_end - buf;
                }
            }
            header.cell_end = cell_end - buf;

            uint64_t checksum = header.header_checksum();
            int n = PAGE_SIZE / 8;
            uint64_t *data = (uint64_t *)buf;
            for (int i = PAGE_HEADER_SIZE / 8; i < n; i++)
                checksum ^= data[i];

            header.checksum = checksum;
            std::memcpy(buf, &header, PAGE_HEADER_SIZE);

            return buf;
            // return std::unique_ptr<char[]>(buf);
        }

    private:
        bool init_check(char *page)
        {
            uint64_t checksum = header.header_checksum();
            int n = PAGE_SIZE / 8;
            uint64_t *data = (uint64_t *)page;
            for (int i = PAGE_HEADER_SIZE / 8; i < n; i++)
                checksum ^= data[i];

            if (checksum != header.checksum)
            {
                valid = false;
                std::cerr << "page checksum fails:\n"
                          << "checksum = " << checksum << "\n"
                          << "header.checksum = " << header.checksum << "\n";
                exit(-1);
            }
        }

        void init_read_data(char *page)
        {
            keys.resize(header.data_num);

            if (header.type == CellType::KeyCell)
                children.resize(header.data_num);
            else
                values.resize(header.data_num);

            for (int i = 0; i < header.data_num; i++)
            {
                char *cell_data = page_cell(page, i);

                if (header.type == CellType::KeyCell)
                {
                    KeyCellHeader cell(cell_data);
                    keys[i] = std::string(cell_data + KEY_CELL_HEADER_SIZE, cell.key_size);
                    children[i] = cell.child_id;
                }
                else
                {
                    KeyValueCellHeader cell(cell_data);
                    keys[i] = std::string(cell_data + KEY_VALUE_CELL_HEADER_SIZE, cell.key_size);
                    values[i] = std::string(cell_data + KEY_VALUE_CELL_HEADER_SIZE + cell.key_size, cell.value_size);
                }
            }
        }

        bool valid = true; // true iff the checksum is correct
    };

    using Page = BTreeNode;

    class BTree : public KvEngine
    {
    public:
        virtual OpStatus open(const char *path)
        {
            if (!std::filesystem::exists(path))
                std::filesystem::create_directory(path);
            dir = std::filesystem::path(path);

            std::filesystem::path data_file_path;
            data_file_path = std::filesystem::path(dir).append("data");

            data_file = open64(data_file_path.c_str(), O_CREAT | O_RDWR | O_SYNC, S_IRUSR | S_IWUSR);
            if (data_file == -1)
            {
                std::cout << "failed to open file\n";
                exit(-1);
            }

            // Allocate root page
            if (file_size(data_file) == 0)
            {
                // truncate64(data_file_path.c_str(), PAGE_SIZE * 10);
                PageHeader header;
                header.type = CellType::KeyValueCell;
                header.data_num = 0;
                header.cell_end = PAGE_SIZE;
                header.checksum = header.header_checksum();
                std::cout << "root checksum: " << header.checksum << "\n";

                // char buf[PAGE_SIZE];
                // memset(buf, 0, PAGE_SIZE);
                // memcpy(buf, &header, PAGE_HEADER_SIZE);
                ssize_t n = pwrite64(data_file, &header, PAGE_HEADER_SIZE, 0);
                if (n != PAGE_HEADER_SIZE)
                    puts(strerror(errno));
            }

            // Force buffering root page
            buffer_manager.get(0);

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus get(const std::string &key)
        {
            BTreeNode *node = buffer_manager.get(0);

            // node is not a leaf
            while (node->header.type == CellType::KeyCell)
                node = buffer_manager.get(node->find_child(key));

            size_t index = node->find_value_index(key);
            if (index >= node->values.size() || node->keys[index] != key) // key not found
            {
                return OpStatus(OpError::KeyNotFound);
            }

            return OpStatus(OpError::Ok, node->values[index]);
        };

        virtual OpStatus set(const std::string &key, std::string value)
        {
            BTreeNode *node = buffer_manager.get(0);

            while (node->header.type == CellType::KeyCell)
                node = buffer_manager.get(node->find_child(key));

            // there is still enough space
            if (key.size() + value.size() + sizeof(uint32_t) + KEY_VALUE_CELL_HEADER_SIZE <= node->header.free_space())
            {
                size_t index = std::lower_bound(node->keys.begin(), node->keys.end(), key) - node->keys.begin();
                if (index < node->keys.size() && node->keys[index] == key) // exist, update value
                    node->update_with_index(std::move(value), index);
                else
                    node->insert_with_index(key, std::move(value), index);
            }
            else
            {
                // handle split
            }

            return OpStatus(OpError::Ok);
        };

        virtual OpStatus remove(const std::string &key){

        };

        virtual OpStatus scan(const std::string &start_key, const std::string &end_key){

        };

        BTree() : buffer_manager(*this)
        {
        }

        virtual ~BTree()
        {
        }

        char *load_page(uint32_t page_id)
        {
            static char page[PAGE_SIZE];

            uint64_t page_pos = (uint64_t)page_id * PAGE_SIZE;
            ssize_t n = pread64(data_file, page, PAGE_SIZE, page_pos);
            if (n == -1)
                puts(strerror(errno));

            // std::cout << "cur read offset: " << data_file.tellg() << "\n";

            std::cout << "read header bytes:\n";
            for (int i = 0; i < PAGE_HEADER_SIZE; i++)
            {
                printf("%x ", page[i] & 0xff);
            }
            std::cout << "\n";

            return page;
        }

    private:
        static uint64_t file_size(int fd)
        {
            uint64_t offset = lseek64(fd, 0, SEEK_SET);
            uint64_t size = lseek(fd, 0, SEEK_END);
            lseek(fd, offset, SEEK_SET);
            return size;
        }

        class BufferManager
        {
        public:
            // todo: modify the default buffer size
            BufferManager(BTree &btree, uint64_t size = PAGE_SIZE) : btree(btree), buffer_size(size) {}

            ~BufferManager()
            {
                // std::cout << "deconstruct buffer manager\n";
                for (auto pair : buffer_map)
                {
                    store(pair.second);
                }
                close(btree.data_file);
            }

            BTreeNode *get(const uint32_t &page_id)
            {
                BTreeNode *res = nullptr;

                if (buffer_map.find(page_id) == buffer_map.end())
                {
                    char *page = load(page_id);
                    if (page == nullptr) // No such page
                        return nullptr;

                    res = new BTreeNode(page_id, page);
                    buffer_map[page_id] = res;
                }

                if (res == nullptr)
                    res = buffer_map[page_id];

                return res;
            }

            std::vector<uint32_t> load_pointers(const uint32_t &page_id, const uint32_t &n)
            {
                static uint32_t buf[1024];
                uint64_t page_pos = page_id * PAGE_SIZE;
                pread64(btree.data_file, buf, n * sizeof(uint32_t), page_pos);
                std::vector<uint32_t> pointers(n);
                for (int i = 0; i < n; i++)
                    pointers[i] = buf[i];
                return pointers;
            }

        private:
            char *load(const uint32_t &page_id)
            {
                // need to evict a page
                if (current_size + PAGE_SIZE > buffer_size)
                {
                    evict();
                }

                current_size += PAGE_SIZE;

                return btree.load_page(page_id);
            }

            // choose a buffered page, and evict
            bool evict()
            {
                uint32_t id = 0;
                for (auto it = buffer_map.begin(); it != buffer_map.end(); it++)
                {
                    if (it->second->page_id != 0) // not root
                    {
                        if (!store(it->second))
                            return false;

                        delete it->second;
                        current_size -= PAGE_SIZE;
                        buffer_map.erase(it);
                        return true;
                    }
                }

                return false;
            }

            bool store(BTreeNode *node)
            {
                uint64_t page_pos = (uint64_t)(node->page_id) * PAGE_SIZE;
                char *buf = node->serialize();
                ssize_t n = pwrite64(btree.data_file, buf, PAGE_SIZE, page_pos);
                if (n == -1)
                    puts(strerror(errno));

                return true;
            }

            BTree &btree;
            uint64_t buffer_size;
            uint64_t current_size;
            std::unordered_map<uint32_t, BTreeNode *> buffer_map;
        };

        BufferManager buffer_manager;
        std::filesystem::path dir;
        int data_file;
    };
} // namespace cyber