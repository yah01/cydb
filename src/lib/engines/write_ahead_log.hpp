#pragma once

#include <iostream>
#include <fstream>
#include <cstring>
#include <functional>
#include <filesystem>

#define _GNU_SOURCE
#include "fcntl.h"
#include "unistd.h"

#include "engines/type.h"

#define ROUND_DOWN(v, r) ((v) / (r) * (r))

namespace cyber
{
    namespace fs = std::filesystem;
    struct Record
    {
        id_t seq_num;
        id_t page_id;
        len_t redo_len;

        char redo[1];
    };

    constexpr size_t RECORD_HEADER_SIZE = offsetof(Record, redo[0]);

    class WriteAheadLog
    {
        int log_file;
        fs::path log_file_path;
        id_t cur_seq_num = 0;
        offset_t trim_off = 0;

    public:
        ~WriteAheadLog()
        {
            fs::remove(log_file_path);   
            close(log_file);
        }

        void open(const char *dir_path)
        {
            if (!fs::exists(dir_path))
                fs::create_directory(dir_path);

            log_file_path = fs::path(dir_path) / "cydb.log";
            log_file = open64(log_file_path.c_str(), O_CREAT | O_WRONLY | O_APPEND | O_SYNC, S_IRUSR | S_IWUSR);
        }

        offset_t log(const Record &record)
        {
            write(log_file, &record, RECORD_HEADER_SIZE + record.redo_len);
            fsync(log_file);

            return lseek64(log_file, 0, SEEK_CUR);
        }

        void for_each_record(std::function<void(const Record &)> const &handler)
        {
            static char raw_record_header[RECORD_HEADER_SIZE];

            int reader = open64(log_file_path.c_str(), O_CREAT | O_RDONLY, S_IRUSR | S_IWUSR);
            char *raw_data = nullptr;
            len_t max_len = 0;
            while (true)
            {
                if (read(reader, raw_record_header, RECORD_HEADER_SIZE) != RECORD_HEADER_SIZE)
                    break;
                Record *record = (Record *)raw_record_header;
                if (RECORD_HEADER_SIZE + record->redo_len > max_len)
                {
                    max_len = RECORD_HEADER_SIZE + record->redo_len;
                    delete[] raw_data;
                    raw_data = new char[max_len];
                    std::memcpy(raw_data, raw_record_header, RECORD_HEADER_SIZE);
                }

                read(reader, raw_data + RECORD_HEADER_SIZE, record->redo_len);
                record = (Record *)raw_data;
                handler(*record);
            }

            close(reader);
        }

        id_t gen_id() { return cur_seq_num++; }

        void set_trim_off(offset_t off) { trim_off = off; }
    };
} // namespace cyber