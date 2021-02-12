#pragma once

#include <cstdint>
#include <unordered_map>
#include <filesystem>
#include <iostream>
#include <vector>
#include <unordered_set>
#include <fcntl.h>
#include <unistd.h>

#include "../kv_engine.hpp"

#include "page.hpp"

namespace cyber
{
    class BufferManager
    {
    public:
        uint32_t page_num = 0;
        // todo: modify the default buffer size
        BufferManager(uint64_t size = PAGE_SIZE) : buffer_size(size) {}

        ~BufferManager()
        {
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
                allocate_page(CellType::KeyValueCell);
            }

            return OpStatus(OpError::Ok);
        }

        BTreeNode *get(const uint32_t &page_id)
        {
            BTreeNode *res = nullptr;

            if (buffer_map.find(page_id) == buffer_map.end())
            {
                char *page = load(page_id);
                if (page == nullptr) // can't load
                {
                    return nullptr;
                }

                res = new BTreeNode(page_id, page);
                buffer_map[page_id] = res;
            }

            if (res == nullptr)
                res = buffer_map[page_id];

            return res;
        }

        inline void pin(const uint32_t &page_id) { pinned_page.insert(page_id); }
        inline void unpin(const uint32_t &page_id) { pinned_page.erase(page_id); }

        uint32_t allocate_page(CellType cell_type, int parent = -1)
        {
            PageHeader header;
            header.type = cell_type;
            header.data_num = 0;
            header.cell_end = PAGE_SIZE;
            header.rightmost_child = -1;
            header.checksum = header.header_checksum();

            ssize_t n = pwrite64(data_file, &header, PAGE_HEADER_SIZE, page_num * PAGE_SIZE);
            if (n == -1)
                puts(strerror(errno));

            this->parent.push_back(parent);
            page_num++;

            return page_num - 1;
        }

        const int &ask_parent(uint32_t node_id) const { return parent[node_id]; }

    private:
        char *load_page(const uint32_t &page_id)
        {
            char *page = new char[PAGE_SIZE];

            uint64_t page_pos = (uint64_t)page_id * PAGE_SIZE;
            ssize_t n = pread64(data_file, page, PAGE_SIZE, page_pos);
            if (n == -1)
                puts(strerror(errno));

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
                if (!evict()) // failed to evict
                {
                    return nullptr;
                }
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
                if (const uint32_t &id = it->second->page_id;
                    pinned_page.find(id) == pinned_page.end())
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
        std::unordered_set<uint32_t> pinned_page;
        std::vector<int> parent;
    };
} // namespace cyber