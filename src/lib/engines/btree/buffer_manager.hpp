#pragma once

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

#include "engines/kv_engine.hpp"

#include "page.hpp"

namespace cyber
{
    uint64_t file_size(int fd)
    {
        uint64_t offset = lseek64(fd, 0, SEEK_SET);
        uint64_t size = lseek(fd, 0, SEEK_END);
        lseek(fd, offset, SEEK_SET);
        return size;
    }

    struct Metadata
    {
        uint32_t root_id;
        uint32_t node_num;
        uint64_t data_num;
    };

    constexpr size_t METADATA_SIZE = sizeof(Metadata);

    class BufferManager
    {
    public:
        Metadata metadata;

        // TODO: modify the default buffer size
        BufferManager(uint64_t size = PAGE_SIZE) : buffer_size(size) {}

        ~BufferManager()
        {
            for (auto pair : buffer_map)
            {
                store_page(pair.second);
            }
            close(data_file);

            std::filesystem::path metadata_path = dir / "metadata";
            int metadata_file = open64(metadata_path.c_str(), O_CREAT | O_WRONLY | O_SYNC, S_IRUSR | S_IWUSR);
            if (metadata_file == -1)
                exit(-1);
            if (pwrite64(metadata_file, &metadata, METADATA_SIZE, 0) == -1)
            {
                exit(-1);
            }
            close(metadata_file);
        }

        OpStatus open(const char *path)
        {
            if (!std::filesystem::exists(path))
                std::filesystem::create_directory(path);
            dir = std::filesystem::path(path);

            std::filesystem::path data_file_path, metadata_path;
            data_file_path = dir / "data";
            metadata_path = dir / "metadata";

            data_file = open64(data_file_path.c_str(), O_CREAT | O_RDWR | O_SYNC, S_IRUSR | S_IWUSR);
            if (data_file == -1)
            {
                exit(-1);
            }

            // Allocate root page
            if (file_size(data_file) == 0)
            {
                allocate_page(CellType::KeyValueCell);
            }

            int metadata_file = open64(metadata_path.c_str(), O_CREAT | O_RDONLY | O_SYNC, S_IRUSR | S_IWUSR);
            if (metadata_file == -1)
                exit(-1);
            if (pread64(metadata_file, &metadata, METADATA_SIZE, 0) == -1)
            {
                exit(-1);
            }
            close(metadata_file);

            return OpStatus(OpError::Ok);
        }

        // node methods
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
        BTreeNode *get_root()
        {
            return get(metadata.root_id);
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

            ssize_t n = pwrite64(data_file, &header, PAGE_HEADER_SIZE, metadata.node_num * PAGE_SIZE);
            if (n == -1)
                puts(strerror(errno));

            metadata.node_num++;

            return metadata.node_num - 1;
        }

    private:
        // read page from disk
        char *load_page(const uint32_t &page_id)
        {
            char *page;
            try
            {
                page = new char[PAGE_SIZE];
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
                exit(-1);
            }

            uint64_t page_pos = (uint64_t)page_id * PAGE_SIZE;
            ssize_t n = pread64(data_file, page, PAGE_SIZE, page_pos);
            if (n == -1)
                puts(strerror(errno));

            return page;
        }
        // write a page to disk
        bool store_page(BTreeNode *node)
        {
            uint64_t page_pos = (uint64_t)(node->page_id) * PAGE_SIZE;
            node->cal_checksum();
            ssize_t n = pwrite64(data_file, node->raw_page(), PAGE_SIZE, page_pos);
            if (n == -1)
                puts(strerror(errno));

            delete node;
            return true;
        }

        // load page to buffer pool
        char *load(const uint32_t &page_id)
        {
            // need to evict a page
            if (current_size + PAGE_SIZE > buffer_size)
            {
                if (!evict()) // failed to evict
                {
                    // TODO: now, do nothing
                    // return nullptr;
                }
            }

            current_size += PAGE_SIZE;

            return load_page(page_id);
        }
        bool evict()
        {
            uint32_t id = 0;
            for (auto it = buffer_map.begin(); it != buffer_map.end(); it++)
            {
                if (const uint32_t &id = it->second->page_id;
                    pinned_page.find(id) == pinned_page.end())
                {
                    if (!store_page(it->second))
                        return false;

                    current_size -= PAGE_SIZE;
                    buffer_map.erase(it);
                    return true;
                }
            }

            return false;
        }

        // data members
        int data_file;
        std::filesystem::path dir;
        uint64_t buffer_size;
        uint64_t current_size;
        std::unordered_map<uint32_t, BTreeNode *> buffer_map;
        std::unordered_set<uint32_t> pinned_page;
    };
} // namespace cyber