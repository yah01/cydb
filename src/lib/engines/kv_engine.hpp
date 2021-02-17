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

        OpStatus() = default;
        OpStatus(const OpError &err) : err(err) {}
        OpStatus(const OpError &err, std::string_view value) : err(err), value(value) {}
        OpStatus(const OpError &err, std::string &&value) : err(err), value(std::move(value)) {}
    };

    class KvEngine
    {
    public:
        virtual OpStatus open(const char *path) = 0;
        virtual OpStatus get(std::string_view key) = 0;
        virtual OpStatus set(std::string_view key, std::string_view value) = 0;
        virtual OpStatus remove(std::string_view key) = 0;
        virtual OpStatus scan(std::string_view start_key, std::string_view end_key) = 0;
        virtual ~KvEngine() {}
    };
} // namespace cyber
