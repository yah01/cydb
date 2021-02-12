#pragma once

#include <string>

namespace cyber
{
    enum class OpError : uint8_t
    {
        Ok,
        DbNotInit,
        KeyNotFound,
        Io,
        Internal,
    };

    struct OpStatus
    {
        OpError err;
        std::string value;

        OpStatus(const OpError &err) : err(err) {}
        OpStatus(const OpError &err, std::string &value) : err(err), value(value) {}
        OpStatus(const OpError &err, std::string &&value) : err(err), value(std::move(value)) {}
    };

    class KvEngine
    {
    public:
        virtual OpStatus open(const char *path) = 0;
        virtual OpStatus get(const std::string &key) = 0;
        virtual OpStatus set(const std::string &key, std::string value) = 0;
        virtual OpStatus remove(const std::string &key) = 0;
        virtual OpStatus scan(const std::string &start_key, const std::string &end_key) = 0;
        virtual ~KvEngine(){}
    };
}; // namespace cyber
