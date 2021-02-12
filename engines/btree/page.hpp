#pragma once

/*
All structs are POD types.
Should always use them by row pointer, except you try to create a new region.
*/

#include <cstdint>
#include <string>
#include <cstring>
#include <iostream>

namespace cyber
{
    constexpr uint64_t PAGE_SIZE = 16 << 10; // 16KiB

    enum struct CellType : uint8_t
    {
        KeyCell = 1,
        KeyValueCell = 2,
    };

    struct KeyCellHeader
    {
        uint32_t key_size;
        int32_t child_id;

        KeyCellHeader(const std::string &key, const uint32_t &child_id) : key_size(key.length()),
                                                                          child_id(child_id) {}
    };

    struct KeyValueCellHeader
    {
        uint32_t key_size;
        uint32_t value_size;

        KeyValueCellHeader(const std::string &key, const std::string &value) : key_size(key.length()),
                                                                               value_size(value.length()) {}
    };

    struct PageHeader
    {
        uint64_t checksum;
        CellType type;

        uint16_t data_num;
        uint32_t cell_end; // cells grow left, cell_end is the offset of the last cell.
        int32_t rightmost_child;

        uint64_t header_checksum()
        {
            uint64_t *header = (uint64_t *)this;
            uint64_t checksum = 0;
            int n = sizeof(PageHeader) / 8;
            for (int i = 1; i < n; i++)
                checksum ^= header[i];
            return checksum;
        }
    };

    constexpr uint64_t KEY_CELL_HEADER_SIZE = sizeof(KeyCellHeader),
                       KEY_VALUE_CELL_HEADER_SIZE = sizeof(KeyValueCellHeader),
                       PAGE_HEADER_SIZE = sizeof(PageHeader); // Must be multiple of 8

    class Cell
    {
    protected:
        char *key = nullptr;

    public:
        virtual ~Cell() {}

        virtual size_t key_size() const = 0;
        virtual size_t size() const = 0;

        // -1 -> less
        // 0  -> equal
        // 1  -> greater
        virtual int compare_by_key(const std::string &key) const
        {
            for (int i = 0; i < key_size() && i < key.length(); i++)
            {
                if (this->key[i] < key[i])
                    return -1;
                else if (this->key[i] > key[i])
                    return 1;
            }

            if (key_size() == key.length())
                return 0;

            return (key_size() < key.length()) ? -1 : 1;
        }
    };
    class KeyCell : public Cell
    {
        KeyCellHeader *header;

    public:
        KeyCell(char *cell)
        {
            header = (KeyCellHeader *)cell;
            key = cell + KEY_CELL_HEADER_SIZE;
        }
        virtual ~KeyCell() {}

        int32_t child() const { return header->child_id; }

        virtual size_t key_size() const { return header->key_size; }
        virtual size_t size() const { return KEY_CELL_HEADER_SIZE + header->key_size; }
    };
    class KeyValueCell : public Cell
    {
        KeyValueCellHeader *header = nullptr;
        char *value = nullptr;

    public:
        KeyValueCell(char *cell)
        {
            header = (KeyValueCellHeader *)cell;
            key = cell + KEY_VALUE_CELL_HEADER_SIZE;
            value = cell + KEY_VALUE_CELL_HEADER_SIZE + header->key_size;
        }

        // only for create new cell, the pointer *cell* may contains invalid data
        KeyValueCell(char *cell, uint32_t key_size)
        {
            header = (KeyValueCellHeader *)cell;
            key = cell + KEY_VALUE_CELL_HEADER_SIZE;
            value = cell + KEY_VALUE_CELL_HEADER_SIZE + key_size;
        }

        virtual ~KeyValueCell() {}

        virtual size_t key_size() const { return header->key_size; }
        virtual size_t size() const { return KEY_CELL_HEADER_SIZE + header->key_size + header->value_size; }
        inline uint32_t value_size() { return header->value_size; }

        std::string build_key_string() { return std::string(key, header->key_size); }

        std::string build_value_string() { return std::string(value, header->value_size); }

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
            init_available_list();
            // init_read_data();
        }

        inline CellType cell_type() const { return header->type; }

        inline size_t data_num() const { return header->data_num; }

        inline KeyCell key_cell(uint32_t i) { return KeyCell(raw_cell(i)); }
        inline KeyValueCell key_value_cell(uint32_t i) { return KeyValueCell(raw_cell(i)); }

        int32_t find_child(const std::string &key)
        {
            size_t index = find_index(key);
            if (index < header->data_num)
                return key_cell(index).child();
            return header->rightmost_child;
        }

        // equal to lower_bound
        // return value -1 means there is no entry.
        size_t find_index(const std::string &key)
        {
            return std::lower_bound(pointers, pointers + header->data_num, key, [&](const uint32_t &offset, const std::string &key) {
                       if (header->type == CellType::KeyCell)
                           return KeyCell(page + offset).compare_by_key(key) < 0;
                       else
                           return KeyValueCell(page + offset).compare_by_key(key) < 0;
                   }) -
                   pointers;

            // size_t l = 0,
            //        r = header->data_num, mid;
            // while (l < r)
            // {
            //     mid = (l + r) >> 1;
            //     KeyValueCell kv_cell(key_value_cell(mid));
            //     int cmp_res = kv_cell.compare_by_key(key);

            //     if (cmp_res == -1)
            //     {
            //         l = mid + 1;
            //     }
            //     else if (cmp_res == 0)
            //     {
            //         return mid;
            //     }
            //     else
            //         r = mid;
            // }

            // return r;
        }

        uint32_t update_value(const std::string &value, size_t index)
        {
            KeyValueCell kv_cell(key_value_cell(index));

            // append the new value, and mark the old cell as removed
            if (value.length() > kv_cell.value_size())
            {
                if (free_space() >= kv_cell.size() - kv_cell.value_size() + value.length())
                {
                    remove_cell(index);
                    uint32_t cell_offset = insert_cell(kv_cell.build_key_string(), value);
                    if (cell_offset == 0)
                        return 0;

                    pointers[index] = cell_offset;
                }
                else
                    return 0;
            }
            else
            {
                size_t len = kv_cell.value_size() - value.length();
                kv_cell.write_value(value);
                if (len > 0)
                    insert_free_cell(AvailableEntry(pointers[index] + kv_cell.size(), len));
                return pointers[index];
            }
        }

        // return value: the offset of the new cell
        // return 0 when there is no enough free space
        uint32_t insert_value(const std::string &key, const std::string &value)
        {
            uint32_t cell_offset = insert_cell(key, value);
            if (cell_offset == 0)
                return 0;

            if (header->cell_end > cell_offset)
                header->cell_end = cell_offset;

            pointers[header->data_num] = cell_offset;
            header->data_num++;

            // todo optimization: use binary search to find the final index, avoid to compare keys too many times
            KeyValueCell kv_cell(page + cell_offset);
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

            return cell_offset;
        }

        void remove(uint32_t index)
        {
            remove_cell(index);
            std::memmove(pointers + index, pointers + index + 1, (header->data_num - index - 1) * sizeof(uint32_t));
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
            return header->cell_end - PAGE_HEADER_SIZE - header->data_num * sizeof(uint32_t);
        }

    private:
        struct AvailableEntry
        {
            uint32_t offset;
            uint32_t len;
            AvailableEntry(uint32_t offset, uint32_t len) : offset(offset), len(len) {}
        };

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

        bool init_available_list()
        {
            uint32_t tmp_pointers[header->data_num];
            memcpy(tmp_pointers, pointers, sizeof(tmp_pointers));
            std::sort(tmp_pointers, tmp_pointers + header->data_num);
            std::reverse(tmp_pointers, tmp_pointers + header->data_num);

            uint32_t boundary = PAGE_SIZE;
            for (int i = 0; i < header->data_num; i++)
            {
                uint32_t l = tmp_pointers[i], r = tmp_pointers[i];
                if (header->type == CellType::KeyCell)
                    r += KeyCell(page + tmp_pointers[i]).size();
                else
                    r += KeyValueCell(page + tmp_pointers[i]).size();

                if (boundary > r)
                    available_list.push_back(AvailableEntry(r, boundary - r));
                boundary = l;
            }
        }

        inline char *raw_cell(uint32_t i) { return page + pointers[i]; }

        inline size_t cell_size(uint32_t i)
        {
            if (header->type == CellType::KeyCell)
            {
                return key_cell(i).size();
            }
            else
            {
                return key_value_cell(i).size();
            }
        }

        void insert_free_cell(const AvailableEntry &entry)
        {
            auto it = std::find_if(available_list.begin(), available_list.end(), [&entry](const AvailableEntry &in_list_entry) {
                return entry.offset > in_list_entry.len;
            });
            it = available_list.insert(it, entry);

            while (it != available_list.end())
            {
                auto next = std::next(it);
                if (it->offset + it->len == next->offset)
                {
                    next->offset = it->offset;
                    next->len += it->len;
                    it = available_list.erase(next);
                }
                else
                {
                    break;
                }
            }

            while (it != available_list.begin())
            {
                auto prev = std::prev(it);
                if (prev->offset + prev->len == it->offset)
                {
                    it->offset = prev->offset;
                    it->len += prev->len;
                    it = available_list.erase(prev);
                }
            }
        }

        // no side effects remove
        // pointers will not be modified
        void remove_cell(uint32_t index)
        {
            insert_free_cell(AvailableEntry(pointers[index], cell_size(index)));

            while (!available_list.empty() && available_list.back().offset == header->cell_end)
            {
                header->cell_end += available_list.back().len;
                available_list.pop_back();
            }
        }

        // no side effects insert
        // header and pointers will not be modified
        uint32_t insert_cell(const std::string &key, const std::string &value)
        {
            size_t kv_cell_size = KEY_VALUE_CELL_HEADER_SIZE + key.length() + value.length();
            auto it = std::find_if(available_list.begin(), available_list.end(), [&kv_cell_size](const AvailableEntry &entry) {
                return entry.len >= kv_cell_size;
            });

            uint32_t cell_offset;
            if (it != available_list.end() && free_space() >= sizeof(uint32_t))
            {
                cell_offset = it->offset;
                if (it->len > kv_cell_size)
                    it->offset += kv_cell_size;
                else
                    available_list.erase(it);
            }
            else if (free_space() >= kv_cell_size + sizeof(uint32_t))
            {
                cell_offset = header->cell_end - kv_cell_size;
            }
            else
            {
                return 0;
            }

            KeyValueCell kv_cell(page + cell_offset, key.length());
            kv_cell.write_key(key);
            kv_cell.write_value(value);

            return cell_offset;
        }

        bool valid = true; // true iff the checksum is correct
        char *page;
        PageHeader *header;
        uint32_t *pointers; // point to the offset of cells.

        std::list<AvailableEntry> available_list;
    };
} // namespace cyber