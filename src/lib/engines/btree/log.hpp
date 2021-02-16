#include <string>

#include "engines/type.h"
#include "engines/write_ahead_log.hpp"

namespace cyber
{
    enum struct RecordType : uint8_t
    {
        Insert = 1,
        Update = 2,
        Remove = 3,
    };

    struct LogicalRecord
    {
        RecordType type;
        len_t key_len;

        char record[1];

        std::string key_string() { return std::string(record, key_len); }
        std::string value_string(len_t len) { return std::string(record + key_len, len); }

        static Record *new_record(id_t seq_num, id_t page_id,
                                  RecordType type, len_t key_len, len_t value_len,
                                  const char *raw_key, const char *raw_value)
        {
            len_t total_size = RECORD_HEADER_SIZE + offsetof(LogicalRecord, record[0]) + key_len + value_len;
            char *raw_log = new char[total_size];
            Record *rec = (Record *)raw_log;
            LogicalRecord *lr = (LogicalRecord *)(raw_log + RECORD_HEADER_SIZE);
            lr->type = type;
            lr->key_len = key_len;
            std::memcpy(lr->record, raw_key, key_len);
            std::memcpy(lr->record + key_len, raw_value, value_len);

            rec->seq_num = seq_num;
            rec->page_id = page_id;
            rec->redo_len = offsetof(LogicalRecord, record[0]) + key_len + value_len;

            return rec;
        }
    };

    constexpr size_t LOGICAL_RECORD_HEADER_SIZE = offsetof(LogicalRecord, record[0]);
} // namespace cyber