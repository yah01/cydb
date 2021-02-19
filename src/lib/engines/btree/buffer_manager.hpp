#pragma once

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

#include "engines/type.h"
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
        id_t root_id;
        uint32_t node_num;
        uint64_t data_num;
    };

    constexpr size_t METADATA_SIZE = sizeof(Metadata);

    class BufferManager
    {
    public:
        Metadata metadata;

        // TODO: modify the default buffer size
        BufferManager(size_t size = 2 * gb) : buffer_size(size) {}

        ~BufferManager()
        {
            offset_t max_wal_end_off = 0;
            for (auto pair : buffer_map)
            {
                if (pair.second->wal_end_off() > max_wal_end_off)
                    max_wal_end_off = pair.second->wal_end_off();

                store_page(pair.second);
            }
            wal.set_trim_off(max_wal_end_off);

            close(data_file);

            fs::path metadata_path = dir / "metadata";
            int metadata_file = open64(metadata_path.c_str(), O_CREAT | O_WRONLY | O_SYNC, S_IRUSR | S_IWUSR);
            if (metadata_file == -1)
                exit(-1);
            if (pwrite64(metadata_file, &metadata, METADATA_SIZE, 0) == -1)
            {
                std::cerr << "write metadata_file: " << strerror(errno);
                exit(-1);
            }
            close(metadata_file);
        }

        OpStatus open(const char *dir_path)
        {
            if (!fs::exists(dir_path))
                fs::create_directory(dir_path);
            dir = fs::path(dir_path);

            wal.open(dir.c_str());
            posix_fallocate64()

            fs::path data_file_path, metadata_path;
            data_file_path = dir / "data";
            metadata_path = dir / "metadata";

            data_file = open64(data_file_path.c_str(), O_CREAT | O_RDWR | O_SYNC | O_DIRECT, S_IRUSR | S_IWUSR);
            if (data_file == -1)
            {
                std::cerr << "open data_file: " << strerror(errno);
                exit(-1);
            }

            // Allocate root page
            if (file_size(data_file) == 0)
            {
                allocate_page(CellType::KeyValueCell);
            }

            int metadata_file = open64(metadata_path.c_str(), O_CREAT | O_RDONLY | O_SYNC, S_IRUSR | S_IWUSR);
            if (metadata_file == -1)
            {
                std::cerr << "open metadata_file: " << strerror(errno);
                exit(-1);
            }
            if (pread64(metadata_file, &metadata, METADATA_SIZE, 0) == -1)
            {
                std::cerr << "read metadata_file: " << strerror(errno);
                exit(-1);
            }
            close(metadata_file);

            wal.for_each_record([&](const Record &rec) {
                LogicalRecord *record = (LogicalRecord *)rec.redo;
                BTreeNode *node = get(rec.page_id);

                if (record->type == RecordType::Insert)
                {
                    if (node->type() == CellType::KeyCell)
                    {
                        id_t *child_id = (id_t *)(record->record + record->key_len);
                        node->insert_child(record->key_string(), *child_id);
                    }
                    else
                    {
                        len_t value_len = rec.redo_len - record->key_len;
                        node->insert_value(record->key_string(),
                                           record->value_string(value_len));
                    }
                }
                else if (record->type == RecordType::Update)
                {
                    num_t *index = (num_t *)(record->record);
                    if (node->type() == CellType::KeyCell)
                    {
                        id_t *child_id = (id_t *)(record->record + record->key_len);
                        node->update_child(*index, *child_id);
                    }
                    else
                    {
                        len_t value_len = rec.redo_len - record->key_len;
                        node->update_value(*index, record->value_string(value_len));
                    }
                }
                else if (record->type == RecordType::Remove)
                {
                    num_t *index = (num_t *)(record->record);
                    node->remove(*index);
                }
            });

            return OpStatus(OpError::Ok);
        }

        // node methods
        BTreeNode *get(const id_t &page_id)
        {
            BTreeNode *res = nullptr;

            if (buffer_map.find(page_id) == buffer_map.end())
            {
                char *page = load(page_id);
                if (page == nullptr) // can't load
                {
                    return nullptr;
                }

                res = new BTreeNode(page_id, page, &wal);
                buffer_map[page_id] = res;
            }

            if (res == nullptr)
                res = buffer_map[page_id];

            return res;
        }
        inline BTreeNode *get_root() { return get(metadata.root_id); }
        inline void pin(const id_t &page_id) { pinned_page.insert(page_id); }
        inline void unpin(const id_t &page_id) { pinned_page.erase(page_id); }
        id_t allocate_page(CellType cell_type)
        {
            static char *buf = (char *)operator new(BLOCK_SIZE, (std::align_val_t)BLOCK_SIZE);
            PageHeader *header = (PageHeader *)buf;
            header->type = cell_type;
            header->cell_end = PAGE_SIZE;
            header->rightmost_child = metadata.node_num;
            header->checksum = header->header_checksum();

            fallocate64(data_file, 0, page_off(metadata.node_num), PAGE_SIZE);

            ssize_t n = pwrite64(data_file, header, BLOCK_SIZE, page_off(metadata.node_num));
            if (n == -1)
            {
                std::cerr << "write data_file: " << strerror(errno);
                exit(-1);
            }

            return metadata.node_num++;
        }
        void deallocate_page(id_t page_id)
        {
            fallocate64(data_file, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, page_off(page_id), PAGE_SIZE);
        }

    private:
        // read page from disk
        char *load_page(const id_t &page_id)
        {
            char *page;
            try
            {
                page = (char *)operator new(PAGE_SIZE, (std::align_val_t)BLOCK_SIZE);
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
            {
                std::cerr << "write data_file: " << strerror(errno);
                exit(-1);
            }

            delete node;
            return true;
        }

        // load page to buffer pool
        char *load(const id_t &page_id)
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
                if (const id_t &id = it->second->page_id;
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
        WriteAheadLog wal;
        size_t buffer_size;
        size_t current_size;
        std::unordered_map<uint32_t, BTreeNode *> buffer_map;
        std::unordered_set<uint32_t> pinned_page;
    };
} // namespace cyber