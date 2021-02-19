#pragma once

/*
All structs are POD types.
Should always use them by row pointer, except you try to create a new region.
*/

#include <string>
#include <cstring>
#include <ranges>
#include <cmath>

#include "engines/type.h"
#include "engines/write_ahead_log.hpp"

#include "log.hpp"

namespace cyber
{
    namespace ranges = std::ranges;
    namespace views = std::views;
    using views::iota;

    constexpr size_t BLOCK_SIZE = 512;
    constexpr size_t PAGE_SIZE = 16 << 10; // 16KiB

    static_assert(std::numeric_limits<num_t>::max() >= PAGE_SIZE);
    static_assert(std::numeric_limits<offset_t>::max() >= PAGE_SIZE);
    static_assert(sizeof(len_t) >= sizeof(offset_t));

    // utils
    inline uint64_t page_off(const id_t id) { return id * PAGE_SIZE; }

    enum struct CellType : uint8_t
    {
        KeyCell = 1,
        KeyValueCell = 2,
    };

    struct KeyCellHeader
    {
        len_t key_size;
        id_t child_id;

        KeyCellHeader(std::string_view key, const id_t child_id) : key_size(static_cast<len_t>(key.length())),
                                                                    child_id(child_id) {}
    };

    struct KeyValueCellHeader
    {
        len_t key_size;
        len_t value_size;

        KeyValueCellHeader(std::string_view key, std::string_view value) : key_size(static_cast<len_t>(key.length())),
                                                                           value_size(static_cast<len_t>(value.length())) {}
    };

    struct PageHeader
    {
        checksum_t checksum = 0;
        CellType type;
        num_t data_num = 0;
        offset_t cell_end;    // cells grow left, cell_end is the offset of the last cell.
        id_t rightmost_child; // equal to the own id if no rightmost_child

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
    static_assert(PAGE_HEADER_SIZE <= BLOCK_SIZE, "PageHeader too large");
    static_assert(PAGE_HEADER_SIZE % 8 == 0, "PAGE_HEADER_SIZE can't be divided by 8");

    class Cell
    {
    protected:
        char *key = nullptr;

    public:
        virtual ~Cell() {}

        virtual len_t key_len() const = 0;
        virtual size_t size() const = 0;
        virtual std::string_view key_str() = 0;
        virtual void write_key(const char *key, const len_t n) = 0;
        virtual void write_key(std::string_view key) { write_key(key.data(), static_cast<len_t>(key.length())); }
        friend auto operator<=>(const Cell &lhs, std::string_view rhs)
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
        friend auto operator==(const Cell &lhs, std::string_view rhs) { return lhs <=> rhs == 0; }
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
        std::string_view key_str() override { return std::string_view(key, header->key_size); }
        void write_key(const char *key, const len_t n) override { memcpy(this->key, key, header->key_size = n); }
        using Cell::write_key;
        // void write_key(std::string_view key) override { Cell::write_key(key); }

        id_t child() const { return header->child_id; }
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
        std::string_view key_str() override { return std::string_view(key, header->key_size); }
        void write_key(const char *key, const len_t n) override { memcpy(this->key, key, header->key_size = n); }
        // void write_key(std::string_view key) override { Cell::write_key(key); }
        using Cell::write_key;

        inline len_t value_len() const { return header->value_size; }
        std::string_view value_str() const { return std::string_view(value, header->value_size); }
        inline void write_value(const char *value, const len_t n) { memcpy(this->value, value, header->value_size = n); }
        inline void write_value(std::string_view value) { write_value(value.data(), static_cast<len_t>(value.length())); }
    };

    class BTreeNode
    {
    public:
        const id_t page_id;

        // BTreeNode(uint32_t page_id) : page_id(page_id) {}
        BTreeNode(id_t page_id, char *buf, WriteAheadLog *wal) : page_id(page_id), page(buf), wal(wal)
        {
            header = (PageHeader *)buf;
            pointers = (offset_t *)(buf + PAGE_HEADER_SIZE);

            init_check();
            init_available_list();
        }
        ~BTreeNode() { operator delete(page, (std::align_val_t)BLOCK_SIZE); }

        inline const char *raw_page() { return this->page; }
        inline offset_t wal_end_off() { return this->max_wal_end_off; }

        // header methods
        inline CellType type() const { return header->type; }
        inline num_t data_num() const { return header->data_num; }
        inline id_t &rightmost_child() { return header->rightmost_child; }
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
        inline len_t free_space() const { return header->cell_end - static_cast<len_t>(PAGE_HEADER_SIZE) - header->data_num * static_cast<len_t>(sizeof(offset_t)); }

        // cell methods
        inline KeyCell key_cell(num_t i) { return KeyCell(raw_cell(i)); }
        inline KeyCell key_cell_at(offset_t off) { return KeyCell(page + off); }
        inline KeyValueCell key_value_cell(num_t i) { return KeyValueCell(raw_cell(i)); }
        inline KeyValueCell key_value_cell_at(offset_t off) { return KeyValueCell(page + off); }
        void remove(num_t index)
        {
            Record *rec = LogicalRecord::new_record(wal->gen_id(), page_id,
                                                    RecordType::Remove, sizeof(index), 0,
                                                    (char *)&index, nullptr);
            max_wal_end_off = wal->log(*rec);
            delete[]((char *)rec);

            remove_cell(index);
            std::memmove(pointers + index, pointers + index + 1, (header->data_num - index - 1) * sizeof(uint32_t));
            header->data_num--;
        }

        // KeyCell methods
        num_t find_child_index(std::string_view key)
        {
            return static_cast<num_t>(std::upper_bound(pointers, pointers + header->data_num, key, [&](std::string_view key, const offset_t offset) {
                                          return KeyCell(page + offset) > key;
                                      }) -
                                      pointers);
        }
        id_t find_child(std::string_view key)
        {
            num_t index = find_child_index(key);
            if (index < header->data_num)
                return key_cell(index).child();
            return header->rightmost_child;
        }
        bool can_hold_kcell(std::string_view key)
        {
            size_t kcell_size = KEY_CELL_HEADER_SIZE + key.length();

            if (free_space() >= kcell_size + sizeof(offset_t))
                return true;

            auto it = ranges::find_if(available_list, [kcell_size](const AvailableEntry &entry) {
                return entry.len >= kcell_size;
            });

            if (it != available_list.end() && free_space() >= sizeof(offset_t))
                return true;

            return false;
        }
        std::optional<offset_t> try_update_child(num_t index, const id_t child)
        {
            Record *rec = LogicalRecord::new_record(wal->gen_id(), page_id,
                                                    RecordType::Update, sizeof(index), sizeof(child),
                                                    (char *)&index, (char *)&child);
            max_wal_end_off = wal->log(*rec);
            delete[]((char *)rec);

            if (index >= header->data_num)
            {
                header->rightmost_child = child;
                return 1;
            }

            key_cell(index).write_child(child);
            return pointers[index];
        }
        std::optional<offset_t> try_insert_child(std::string_view key, const id_t child)
        {
            Record *rec = LogicalRecord::new_record(wal->gen_id(), page_id,
                                                    RecordType::Insert, key.length(), sizeof(child),
                                                    key.data(), (char *)&child);
            max_wal_end_off = wal->log(*rec);
            delete[]((char *)rec);

            num_t index = find_child_index(key);
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
        num_t find_value_index(std::string_view key)
        {
            return std::lower_bound(pointers, pointers + header->data_num, key, [&](const offset_t offset, std::string_view key) {
                       return KeyValueCell(page + offset) < key;
                   }) -
                   pointers;
        }
        bool can_hold_kvcell(std::string_view key, std::string_view value)
        {
            size_t kvcell_size = KEY_VALUE_CELL_HEADER_SIZE + key.length() + value.length();

            if (free_space() >= kvcell_size + sizeof(offset_t))
                return true;

            auto it = ranges::find_if(available_list, [kvcell_size](const AvailableEntry &entry) {
                return entry.len >= kvcell_size;
            });

            if (it != available_list.end() && free_space() >= sizeof(offset_t))
                return true;

            return false;
        }
        std::optional<offset_t> try_update_value(num_t index, std::string_view value)
        {
            Record *rec = LogicalRecord::new_record(wal->gen_id(), page_id,
                                                    RecordType::Update, sizeof(index), value.length(),
                                                    (char *)&index, value.data());
            max_wal_end_off = wal->log(*rec);
            delete[]((char *)rec);

            KeyValueCell kvcell(key_value_cell(index));

            // append the new value, and mark the old cell as removed
            if (value.length() > kvcell.value_len())
            {
                if (free_space() >= kvcell.size() - kvcell.value_len() + value.length())
                {
                    remove_cell(index);
                    offset_t cell_offset = insert_kvcell(kvcell.key_str(), value);
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
        std::optional<offset_t> try_insert_value(std::string_view key, std::string_view value)
        {
            Record *rec = LogicalRecord::new_record(wal->gen_id(), page_id,
                                                    RecordType::Insert, key.length(), value.length(),
                                                    key.data(), value.data());
            max_wal_end_off = wal->log(*rec);
            delete[]((char *)rec);

            offset_t cell_offset = insert_kvcell(key, value);
            if (cell_offset == 0)
                return std::nullopt;

            if (header->cell_end > cell_offset)
                header->cell_end = cell_offset;

            num_t index = find_value_index(key);
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
                         r = tmp_pointers[i] + cell_size_at(tmp_pointers[i]);

                if (boundary > r)
                {
                    available_list.push_back(AvailableEntry(r, boundary - r));
                    total_available_space += boundary - r;
                }
                boundary = l;
            }
        }

        // available list
        void insert_available_entry(const AvailableEntry &entry)
        {
            auto it = ranges::find_if(available_list, [&entry](const AvailableEntry &in_list_entry) {
                return entry.offset > in_list_entry.len;
            });
            it = available_list.insert(it, entry);
            total_available_space += entry.len;

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
        inline std::string_view cell_key(uint32_t i)
        {
            if (header->type == CellType::KeyCell)
            {
                return key_cell(i).key_str();
            }
            else
            {
                return key_value_cell(i).key_str();
            }
        }
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
        inline size_t cell_size_at(offset_t off)
        {
            if (header->type == CellType::KeyCell)
            {
                return key_cell_at(off).size();
            }
            else
            {
                return key_value_cell_at(off).size();
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
                total_available_space -= available_list.back().len;
                available_list.pop_back();
            }
        }

        // KeyCell methods
        // you should grantee there is enough free space
        offset_t insert_kcell(std::string_view key, const id_t child)
        {
            size_t kcell_size = KEY_CELL_HEADER_SIZE + key.length();
            auto it = ranges::find_if(available_list, [kcell_size](const AvailableEntry &entry) {
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

                total_available_space -= kcell_size;
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
        offset_t insert_kvcell(std::string_view key, std::string_view value)
        {
            size_t kvcell_size = KEY_VALUE_CELL_HEADER_SIZE + key.length() + value.length();
            auto it = ranges::find_if(available_list, [kvcell_size](const AvailableEntry &entry) {
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

                total_available_space -= kvcell_size;
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

        len_t defragment()
        {
            offset_t total_off = 0;
            for (auto it = available_list.begin(), next = it; it != available_list.end(); it = next)
            {
                total_off += it->len;
                next = std::next(it);
                offset_t cell_offset;
                if (next != available_list.end())
                    cell_offset = next->offset + next->len;
                else
                    cell_offset = header->cell_end;
                std::memmove(page + cell_offset + total_off,
                             page + cell_offset,
                             cell_size_at(cell_offset));
            }

            available_list.clear();
            total_available_space = 0;

            header->cell_end += total_off;

            return total_off;
        }

        // data members
        bool valid = true; // true iff the checksum is correct
        char *page;
        PageHeader *header;
        offset_t *pointers;                       // point to the offset of cells.
        std::list<AvailableEntry> available_list; // descending by offset
        len_t total_available_space = 0;
        offset_t max_wal_end_off = 0;
        WriteAheadLog *wal;
    };
} // namespace cyber