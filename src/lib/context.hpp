#pragma once

#include <chrono>
#include <unordered_map>
#include <string>
#include <optional>
#include <any>

namespace cyber
{
    namespace chrono = std::chrono;

    // class Context
    // {
    //     uint64_t id;
    //     chrono::time_point<chrono::steady_clock> timestamp;

    //     std::unordered_map<std::string, std::any> data;

    // public:
    //     Context(const uint64_t &id) : id(id) { timestamp = chrono::steady_clock::now(); }

    //     std::optional<std::any&> get(std::string_view key)
    //     {
    //         auto it = data.find(key);
    //         if (it == data.end())
    //             return std::nullopt;
    //         return it->second;
    //     }

    //     void set(std::string_view key, const std::any &value)
    //     {
    //         data.insert_or_assign(key, value);
    //     }

    //     void set(std::string_view key, std::any &&value)
    //     {
    //         data.insert_or_assign(key, std::move(value));
    //     }
    // };
} // namespace cyber