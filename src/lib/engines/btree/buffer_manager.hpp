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
    namespace fs = std::filesystem;

    uint64_t file_size(int fd)
    {
        uint64_t offset = lseek64(fd, 0, SEEK_SET);
        uint64_t size = lseek(fd, 0, SEEK_END);
        lseek(fd, offset, SEEK_SET);
        return size;
    }

    struct Metadata
    {
        page_id_t root_id;
        uint32_t node_num;
        uint64_t data_num;
    };

    constexpr size_t METADATA_SIZE = sizeof(Metadata);

    class BufferManager
    {
    public:
        Metadata metadata;

        // TODO: modify the default buffer size
        BufferManager(size_t size = PAGE_SIZE) : buffer_size(size) {}

        ~BufferManager()
        {
            for (auto pair : buffer_map)
            {
                store_page(pair.second);
            }
            close(data_file);

            fs::path metadata_path = dir / "metadata";
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
            if (!fs::exists(path))
                fs::create_directory(path);
            dir = fs::path(path);

            fs::path data_file_path, metadata_path;
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
        BTreeNode *get(const page_id_t &page_id)
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
        inline BTreeNode *get_root() { return get(metadata.root_id); }
        inline void pin(const page_id_t &page_id) { pinned_page.insert(page_id); }
        inline void unpin(const page_id_t &page_id) { pinned_page.erase(page_id); }
        page_id_t allocate_page(CellType cell_type)
        {
            PageHeader header;
            header.type = cell_type;
            header.cell_end = PAGE_SIZE;
            header.rightmost_child = metadata.node_num;
            header.checksum = header.header_checksum();

            ssize_t n = pwrite64(data_file, &header, PAGE_HEADER_SIZE, page_off(metadata.node_num));
            if (n == -1)
                puts(strerror(errno));

            return metadata.node_num++;
        }

    private:
        // read page from disk
        char *load_page(const page_id_t &page_id)
        {
            char *page;
            try
            {
                page = new char[PAGE_SIZE]();
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
                exit(-1);
            }

            ssize_t n = pread64(data_file, page, PAGE_SIZE, page_off(page_id));
            if (n == -1)
                puts(strerror(errno));

            return page;
        }
        // write a page to disk
        bool store_page(BTreeNode *node)
        {
            node->cal_checksum();
            ssize_t n = pwrite64(data_file, node->raw_page(), PAGE_SIZE, page_off(node->page_id));
            if (n == -1)
                puts(strerror(errno));

            delete node;
            return true;
        }

        // load page to buffer pool
        char *load(const page_id_t &page_id)
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
            for (auto it : iota(buffer_map.begin(), buffer_map.end()))
            {
                if (const page_id_t &id = it->second->page_id;
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
        fs::path dir;
        size_t buffer_size;
        size_t current_size;
        std::unordered_map<uint32_t, BTreeNode *> buffer_map;
        std::unordered_set<uint32_t> pinned_page;
    };
} // namespace cyber