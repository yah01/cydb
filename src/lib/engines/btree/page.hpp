#pragma once

/*
All structs are POD types.
Should always use them by row pointer, except you try to create a new region.
*/

#include <string>
#include <cstring>
#include <ranges>
#include <cmath>

namespace cyber
{
    namespace ranges = std::ranges;
    namespace views = std::views;
    using views::iota;

    constexpr size_t PAGE_SIZE = 16 << 10; // 16KiB

    using page_id_t = uint32_t;
    using len_t = uint32_t;
    using checksum_t = uint64_t;
    using num_t = uint32_t;
    using offset_t = uint32_t;

    static_assert(std::numeric_limits<num_t>::max() >= PAGE_SIZE);
    static_assert(std::numeric_limits<offset_t>::max() >= PAGE_SIZE);

    // utils
    inline uint64_t page_off(const page_id_t id) { return id * PAGE_SIZE; }

    enum struct CellType : uint8_t
    {
        KeyCell = 1,
        KeyValueCell = 2,
    };

    struct KeyCellHeader
    {
        len_t key_size;
        page_id_t child_id;

        KeyCellHeader(const std::string &key, const page_id_t &child_id) : key_size(key.length()),
                                                                           child_id(child_id) {}
    };

    struct KeyValueCellHeader
    {
        len_t key_size;
        len_t value_size;

        KeyValueCellHeader(const std::string &key, const std::string &value) : key_size(key.length()),
                                                                               value_size(value.length()) {}
    };

    struct PageHeader
    {
        checksum_t checksum = 0;
        CellType type;
        num_t data_num = 0;
        offset_t cell_end;         // cells grow left, cell_end is the offset of the last cell.
        page_id_t rightmost_child; // equal to the own id if no rightmost_child

        checksum_t header_checksum()
        {
            checksum_t checksum = 0;
            checksum_t *data = (checksum_t *)this;
            int n = sizeof(PageHeader) / sizeof(checksum);
            for (auto i : iota(1, n))
                checksum ^= data[i];

            return checksum;
        }
    };

    constexpr size_t KEY_CELL_HEADER_SIZE = sizeof(KeyCellHeader),
                     KEY_VALUE_CELL_HEADER_SIZE = sizeof(KeyValueCellHeader),
                     PAGE_HEADER_SIZE = sizeof(PageHeader); // Must be multiple of 8
    static_assert(PAGE_HEADER_SIZE % 8 == 0, "PAGE_HEADER_SIZE can't be divided by 8");

    class Cell
    {
    protected:
        char *key = nullptr;

    public:
        virtual ~Cell() {}

        virtual len_t key_len() const = 0;
        virtual size_t size() const = 0;
        virtual std::string key_string() = 0;
        virtual void write_key(const char *key, const len_t n) = 0;
        virtual void write_key(const std::string &key) { write_key(key.c_str(), key.length()); }
        friend auto operator<=>(const Cell &lhs, const std::string &rhs)
        {
            for (auto i : iota(0ul, lhs.key_len() < rhs.length() ? lhs.key_len()
                                                                 : rhs.length()))
            {
                if (lhs.key[i] < rhs[i])
                    return std::partial_ordering::less;
                else if (lhs.key[i] > rhs[i])
                    return std::partial_ordering::greater;
            }

            if (lhs.key_len() == rhs.length())
                return std::partial_ordering::equivalent;

            return lhs.key_len() < rhs.length() ? std::partial_ordering::less
                                                : std::partial_ordering::greater;
        }
        friend auto operator==(const Cell &lhs, const std::string &rhs) { return lhs <=> rhs == 0; }
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

        ~KeyCell() {}
        len_t key_len() const override { return header->key_size; }
        size_t size() const override { return KEY_CELL_HEADER_SIZE + header->key_size; }
        std::string key_string() override { return std::string(key, header->key_size); }
        void write_key(const char *key, const len_t n) override { memcpy(this->key, key, header->key_size = n); }
        using Cell::write_key;
        // void write_key(const std::string &key) override { Cell::write_key(key); }

        page_id_t child() const { return header->child_id; }
        void write_child(const int32_t child_id) { header->child_id = child_id; }
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

        len_t key_len() const override { return header->key_size; }
        size_t size() const override { return KEY_CELL_HEADER_SIZE + header->key_size + header->value_size; }
        std::string key_string() override { return std::string(key, header->key_size); }
        void write_key(const char *key, const len_t n) override { memcpy(this->key, key, header->key_size = n); }
        // void write_key(const std::string &key) override { Cell::write_key(key); }
        using Cell::write_key;

        inline len_t value_len() const { return header->value_size; }
        std::string value_string() const { return std::string(value, header->value_size); }
        inline void write_value(const char *value, const len_t n) { memcpy(this->value, value, header->value_size = n); }
        inline void write_value(const std::string &value) { write_value(value.c_str(), value.length()); }
    };

    class BTreeNode
    {
    public:
        const page_id_t page_id;

        // BTreeNode(uint32_t page_id) : page_id(page_id) {}
        BTreeNode(page_id_t page_id, char *buf) : page_id(page_id), page(buf)
        {
            header = (PageHeader *)buf;
            pointers = (offset_t *)(buf + PAGE_HEADER_SIZE);

            init_check();
            init_available_list();
        }
        ~BTreeNode() { delete[] page; }

        inline const char *raw_page() { return this->page; }

        // header methods
        inline CellType type() const { return header->type; }
        inline num_t data_num() const { return header->data_num; }
        inline page_id_t &rightmost_child() { return header->rightmost_child; }
        // re-calculate the checksum
        // if you try to check, save the header->checksum first
        checksum_t cal_checksum()
        {
            header->checksum = header->header_checksum();
            auto n = PAGE_SIZE / sizeof(header->checksum);
            checksum_t *data = (checksum_t *)page;
            for (auto i : iota(PAGE_HEADER_SIZE / sizeof(checksum_t), n))
                header->checksum ^= data[i];

            return header->checksum;
        }
        inline size_t free_space() const { return header->cell_end - PAGE_HEADER_SIZE - header->data_num * sizeof(uint32_t); }

        // cell methods
        inline KeyCell key_cell(size_t i) { return KeyCell(raw_cell(i)); }
        inline KeyValueCell key_value_cell(size_t i) { return KeyValueCell(raw_cell(i)); }
        void remove(size_t index)
        {
            remove_cell(index);
            std::memmove(pointers + index, pointers + index + 1, (header->data_num - index - 1) * sizeof(uint32_t));
            header->data_num--;
        }

        // KeyCell methods
        size_t find_child_index(const std::string &key)
        {
            return std::upper_bound(pointers, pointers + header->data_num, key, [&](const std::string &key, const offset_t &offset) {
                       return KeyCell(page + offset) > key;
                   }) -
                   pointers;
        }
        page_id_t find_child(const std::string &key)
        {
            size_t index = find_child_index(key);
            if (index < header->data_num)
                return key_cell(index).child();
            return header->rightmost_child;
        }
        std::optional<offset_t> update_child(size_t index, const page_id_t &child)
        {
            if (index >= header->data_num)
            {
                header->rightmost_child = child;
                return 1;
            }

            key_cell(index).write_child(child);
            return pointers[index];
        }
        std::optional<offset_t> insert_child(const std::string &key, const page_id_t child)
        {
            size_t index = find_child_index(key);
            if (index >= header->data_num && header->data_num > 0)
                return std::nullopt;

            offset_t cell_offset = insert_kcell(key, child);
            if (cell_offset == 0)
                return std::nullopt;

            if (header->cell_end > cell_offset)
                header->cell_end = cell_offset;

            pointers[header->data_num] = cell_offset;
            header->data_num++;

            KeyCell kcell(page + cell_offset);
            for (auto i : views::iota(index + 1, header->data_num) | views::reverse)
                std::swap(pointers[i - 1], pointers[i]);

            return cell_offset;
        }

        // KeyValueCell methods

        // equal to lower_bound
        // return value -1 means there is no entry.
        size_t find_value_index(const std::string &key)
        {
            return std::lower_bound(pointers, pointers + header->data_num, key, [&](const offset_t &offset, const std::string &key) {
                       return KeyValueCell(page + offset) < key;
                   }) -
                   pointers;
        }
        std::optional<offset_t> update_value(size_t index, const std::string &value)
        {
            KeyValueCell kvcell(key_value_cell(index));

            // append the new value, and mark the old cell as removed
            if (value.length() > kvcell.value_len())
            {
                if (free_space() >= kvcell.size() - kvcell.value_len() + value.length())
                {
                    remove_cell(index);
                    offset_t cell_offset = insert_kvcell(kvcell.key_string(), value);
                    if (cell_offset == 0)
                        return std::nullopt;

                    return pointers[index] = cell_offset;
                }
                else
                    return std::nullopt;
            }
            else
            {
                len_t len = kvcell.value_len() - value.length();
                kvcell.write_value(value);
                if (len > 0)
                    insert_available_entry(AvailableEntry(pointers[index] + kvcell.size(), len));
                return pointers[index];
            }
        }
        // return value: the offset of the new cell
        // return 0 when there is no enough free space
        std::optional<offset_t> insert_value(const std::string &key, const std::string &value)
        {
            offset_t cell_offset = insert_kvcell(key, value);
            if (cell_offset == 0)
                return std::nullopt;

            if (header->cell_end > cell_offset)
                header->cell_end = cell_offset;

            size_t index = find_value_index(key);
            pointers[header->data_num] = cell_offset;
            header->data_num++;

            KeyValueCell kvcell(page + cell_offset);

            for (auto i : views::iota(index + 1, header->data_num) | views::reverse)
                std::swap(pointers[i - 1], pointers[i]);

            return cell_offset;
        }

    private:
        struct AvailableEntry
        {
            offset_t offset;
            len_t len;
            AvailableEntry(offset_t offset, len_t len) : offset(offset), len(len) {}
        };

        // initialize
        bool init_check()
        {
            checksum_t old_checksum = header->checksum;
            if (old_checksum != cal_checksum())
            {
                valid = false;
                return false;
            }

            return true;
        }
        void init_available_list()
        {
            std::vector<offset_t> tmp_pointers(pointers, pointers + header->data_num);
            ranges::sort(tmp_pointers, ranges::greater());

            offset_t boundary = PAGE_SIZE;
            for (auto i : views::iota(0u, header->data_num))
            {
                offset_t l = tmp_pointers[i],
                         r = tmp_pointers[i];
                if (header->type == CellType::KeyCell)
                    r += KeyCell(page + tmp_pointers[i]).size();
                else
                    r += KeyValueCell(page + tmp_pointers[i]).size();

                if (boundary > r)
                    available_list.push_back(AvailableEntry(r, boundary - r));
                boundary = l;
            }
        }

        // available list
        void insert_available_entry(const AvailableEntry &entry)
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
                else
                    break;
            }
        }

        // cell methods
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
        // no side effects remove
        // pointers will not be modified
        void remove_cell(uint32_t index)
        {
            insert_available_entry(AvailableEntry(pointers[index], cell_size(index)));

            while (!available_list.empty() && available_list.back().offset == header->cell_end)
            {
                header->cell_end += available_list.back().len;
                available_list.pop_back();
            }
        }

        // KeyCell methods
        // you should grantee there is enough free space
        offset_t insert_kcell(const std::string &key, const page_id_t &child)
        {
            size_t kcell_size = KEY_CELL_HEADER_SIZE + key.length();
            auto it = std::find_if(available_list.begin(), available_list.end(), [&kcell_size](const AvailableEntry &entry) {
                return entry.len >= kcell_size;
            });

            offset_t cell_offset;
            if (it != available_list.end() && free_space() >= sizeof(offset_t))
            {
                cell_offset = it->offset;
                if (it->len > kcell_size)
                    it->offset += kcell_size;
                else
                    available_list.erase(it);
            }
            else if (free_space() >= kcell_size + sizeof(offset_t))
            {
                cell_offset = header->cell_end - kcell_size;
            }
            else
            {
                return 0;
            }

            KeyCell kcell(page + cell_offset);
            kcell.write_key(key);
            kcell.write_child(child);

            return cell_offset;
        }

        // KeyValueCell methods
        // no side effects insert
        // header and pointers will not be modified
        // you should grantee there is enough free space
        offset_t insert_kvcell(const std::string &key, const std::string &value)
        {
            size_t kvcell_size = KEY_VALUE_CELL_HEADER_SIZE + key.length() + value.length();
            auto it = std::find_if(available_list.begin(), available_list.end(), [&kvcell_size](const AvailableEntry &entry) {
                return entry.len >= kvcell_size;
            });

            offset_t cell_offset;
            if (it != available_list.end() && free_space() >= sizeof(offset_t))
            {
                cell_offset = it->offset;
                if (it->len > kvcell_size)
                    it->offset += kvcell_size;
                else
                    available_list.erase(it);
            }
            else if (free_space() >= kvcell_size + sizeof(offset_t))
            {
                cell_offset = header->cell_end - kvcell_size;
            }
            else
            {
                return 0;
            }

            KeyValueCell kvcell(page + cell_offset, key.length());
            kvcell.write_key(key);
            kvcell.write_value(value);

            return cell_offset;
        }

        // data members
        bool valid = true; // true iff the checksum is correct
        char *page;
        PageHeader *header;
        offset_t *pointers; // point to the offset of cells.
        std::list<AvailableEntry> available_list;
    };
} // namespace cyber