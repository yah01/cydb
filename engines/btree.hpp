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

#include "kv_engine.hpp"

/*
All structs are POD types.
Should always use them by row pointer, except you try to create a new region.
*/

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
        uint32_t cell_end; // cells grow left, cell_end is the offset of the last cell.
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

    class KeyValueCell
    {
        KeyValueCellHeader *header;
        char *key;
        char *value;

    public:
        KeyValueCell(char *cell)
        {
            header = (KeyValueCellHeader *)cell;
            key = cell + KEY_VALUE_CELL_HEADER_SIZE;
            value = cell + KEY_VALUE_CELL_HEADER_SIZE + header->key_size;
        }

        // only for create new cell, the pointer *cell* may contains invalid data
        KeyValueCell(char *cell, uint16_t key_size)
        {
            header = (KeyValueCellHeader *)cell;
            key = cell + KEY_VALUE_CELL_HEADER_SIZE;
            value = cell + KEY_VALUE_CELL_HEADER_SIZE + key_size;
        }

        std::string build_value_string()
        {
            return std::string(value, header->value_size);
        }

        inline uint32_t value_size() { return header->value_size; }

        inline bool operator<(const KeyValueCell &rhs) const
        {
            for (int i = 0; i < header->key_size && i < rhs.header->key_size; i++)
            {
                if (key[i] < rhs.key[i])
                    return true;
                else if (key[i] > rhs.key[i])
                    return false;
            }

            return (header->key_size < rhs.header->key_size) ? true : false;
        }

        // -1 -> less
        // 0  -> equal
        // 1  -> greater
        inline int compare_by_key(const std::string &key) const
        {
            for (int i = 0; i < header->key_size && i < key.length(); i++)
            {
                if (this->key[i] < key[i])
                    return -1;
                else if (this->key[i] > key[i])
                    return 1;
            }

            if (header->key_size == key.length())
                return 0;

            return (header->key_size < key.length()) ? -1 : 1;
        }
        inline void write_key(const char *key, const size_t n)
        {
            header->key_size = n;
            memcpy(this->key, key, n);
        }
        inline void write_key(const std::string &key)
        {
            write_key(key.c_str(), key.length());
        }
        inline void write_value(const char *value, const size_t n)
        {
            header->value_size = n;
            memcpy(this->value, value, n);
        }
        inline void write_value(const std::string &value)
        {
            write_value(value.c_str(), value.length());
        }
    };

    class BTreeNode
    {
    public:
        const uint32_t page_id;

        // BTreeNode(uint32_t page_id) : page_id(page_id) {}
        BTreeNode(uint32_t page_id, char *buf) : page_id(page_id), page(buf)
        {
            header = (PageHeader *)buf;
            pointers = (uint32_t *)(buf + PAGE_HEADER_SIZE);

            init_check();
            // init_read_data();
        }

        CellType cell_type() const
        {
            return header->type;
        }

        size_t entry_num() const { return header->data_num; }

        inline KeyValueCell key_value_cell(uint32_t i) { return KeyValueCell(row_cell(i)); }

        uint32_t find_child(const std::string &key) const
        {
        }

        // equal to lower_bound
        // return value -1 means there is no entry.
        size_t find_index(const std::string &key)
        {
            size_t l = 0, r = header->data_num, mid;
            while (l < r)
            {
                mid = (l + r) >> 1;
                KeyValueCell kv_cell(key_value_cell(mid));
                int cmp_res = kv_cell.compare_by_key(key);

                if (cmp_res == -1)
                {
                    l = mid + 1;
                }
                else if (cmp_res == 0)
                {
                    return mid;
                }
                else
                    r = mid;
            }

            return r;
        }

        void update_with_index(const std::string &value, size_t index)
        {
            KeyValueCell kv_cell(row_cell(index));

            // append the new value, and mark the old cell as removed
            if (value.length() > kv_cell.value_size())
            {
                // todo
            }
            else
            {
                kv_cell.write_value(value);
            }
        }

        void insert(const std::string &key, const std::string &value)
        {
            size_t kv_cell_size = KEY_VALUE_CELL_HEADER_SIZE + key.length() + value.length();
            pointers[header->data_num] = header->cell_end - kv_cell_size;
            KeyValueCell kv_cell(row_cell(header->data_num), key.length());
            kv_cell.write_key(key);
            kv_cell.write_value(value);
            header->data_num++;
            header->cell_end -= kv_cell_size;

            // todo: use binary search to find the final index, avoid to compare keys too many times
            for (int i = header->data_num - 1; i > 0; i--)
            {
                if (kv_cell < key_value_cell(i - 1))
                {
                    std::swap(pointers[i - 1], pointers[i]);
                }
                else
                {
                    break;
                }
            }
        }

        const char *raw_page() { return this->page; }

        // re-calculate the checksum
        // if you try to check, save the header->checksum first
        uint64_t cal_checksum()
        {
            header->checksum = header->header_checksum();
            int n = PAGE_SIZE / sizeof(header->checksum);
            uint64_t *data = (uint64_t *)page;
            for (int i = PAGE_HEADER_SIZE / sizeof(header->checksum); i < n; i++)
                header->checksum ^= data[i];

            return header->checksum;
        }

        inline size_t free_space() const
        {
            return header->free_space();
        }

    private:
        bool init_check()
        {
            uint64_t old_checksum = header->checksum;
            if (old_checksum != cal_checksum())
            {
                valid = false;
                std::cerr << "page checksum fails:\n"
                          << "checksum = " << header->checksum << "\n"
                          << "header.checksum = " << old_checksum << "\n";
                exit(-1);
            }
        }

        inline char *row_cell(uint32_t i) { return page + pointers[i]; }

        bool valid = true; // true iff the checksum is correct
        char *page;
        PageHeader *header;
        uint32_t *pointers; // point to the offset of cells.
    };

    class BTree : public KvEngine
    {
    public:
        virtual OpStatus open(const char *path)
        {
            return buffer_manager.open(path);
        };

        virtual OpStatus get(const std::string &key)
        {
            BTreeNode *node = buffer_manager.get(0);

            // node is not a leaf
            while (node->cell_type() == CellType::KeyCell)
                node = buffer_manager.get(node->find_child(key));

            size_t index = node->find_index(key);
            if (index > node->entry_num()) // no data in the node
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
            BTreeNode *node = buffer_manager.get(0);

            while (node->cell_type() == CellType::KeyCell)
                node = buffer_manager.get(node->find_child(key));

            // there is still enough space
            if (key.size() + value.size() + sizeof(uint32_t) + KEY_VALUE_CELL_HEADER_SIZE <= node->free_space())
            {
                size_t index = node->find_index(key);
                if (index < node->entry_num() && node->key_value_cell(index).compare_by_key(key) == 0) // exist, update value
                    node->update_with_index(value, index);
                else
                    node->insert(key, value);
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

        virtual ~BTree()
        {
        }

    private:
        class BufferManager
        {
        public:
            // todo: modify the default buffer size
            BufferManager(uint64_t size = PAGE_SIZE) : buffer_size(size) {}

            ~BufferManager()
            {
                // std::cout << "deconstruct buffer manager\n";
                for (auto pair : buffer_map)
                {
                    store(pair.second);
                }
                close(data_file);
            }

            static uint64_t file_size(int fd)
            {
                uint64_t offset = lseek64(fd, 0, SEEK_SET);
                uint64_t size = lseek(fd, 0, SEEK_END);
                lseek(fd, offset, SEEK_SET);
                return size;
            }

            OpStatus open(const char *path)
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
                get(0);

                return OpStatus(OpError::Ok);
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
                pread64(data_file, buf, n * sizeof(uint32_t), page_pos);
                std::vector<uint32_t> pointers(n);
                for (int i = 0; i < n; i++)
                    pointers[i] = buf[i];
                return pointers;
            }

        private:
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

            char *load(const uint32_t &page_id)
            {
                // need to evict a page
                if (current_size + PAGE_SIZE > buffer_size)
                {
                    evict();
                }

                current_size += PAGE_SIZE;

                return load_page(page_id);
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
                node->cal_checksum();
                ssize_t n = pwrite64(data_file, node->raw_page(), PAGE_SIZE, page_pos);
                if (n == -1)
                    puts(strerror(errno));

                return true;
            }

            int data_file;
            std::filesystem::path dir;
            uint64_t buffer_size;
            uint64_t current_size;
            std::unordered_map<uint32_t, BTreeNode *> buffer_map;
        };

        BufferManager buffer_manager;
    };
} // namespace cyber