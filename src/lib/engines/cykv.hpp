#pragma once

#include <map>
#include <iostream>
#include <fstream>
#include <unordered_map>

#include "kv_engine.hpp"

namespace cyber
{
    struct LogIndex
    {
        uint32_t id;
        uint64_t offset;
        uint64_t len;
    };

    enum class CommandType : uint8_t
    {
        Set,
        Remove,
    };

    struct Command
    {
        CommandType opt_type;
        std::string &key;
        std::string &value;
    };

    class CyKV : public KvEngine
    {
    public:
        virtual OpStatus open(const char *path)
        {
            
        };

        virtual OpStatus get(std::string_view key){

        };

        virtual OpStatus set(std::string_view key, std::string value){

        };

        virtual OpStatus remove(std::string_view key){

        };

        virtual OpStatus scan(std::string_view start_key, std::string_view end_key){

        };

    private:
        class Reader
        {
        private:
            std::ifstream reader;
        };

        class Writer
        {
        public:
        private:
            CyKV *cykv;
            uint64_t uncompacted;
            std::ofstream writer;
        };

        std::string dir;
        std::map<std::string, LogIndex> keydir;
        std::unordered_map<uint32_t, Reader> readers;
        Writer writer;
        uint32_t log_id;
    };
} // namespace cyber