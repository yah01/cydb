#pragma once

#include <memory>

#include "kv_engine.hpp"

#include "rocksdb/db.h"

namespace cyber
{
    class RocksDB : public KvEngine
    {
    public:
        virtual OpStatus open(const char *path)
        {
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::Status status = rocksdb::DB::Open(options, path, &inner);
            if (status.ok())
                return OpStatus(OpError::Ok);
            else
                return OpStatus(OpError::Internal);
        }

        virtual OpStatus get(std::string_view key)
        {
            if (inner == nullptr)
                return OpStatus(OpError::DbNotInit);

            std::string value;
            auto status = inner->Get(rocksdb::ReadOptions(), key, &value);

            if (status.ok())
                return OpStatus(OpError::Ok, std::move(value));
            else if (status.IsNotFound())
                return OpStatus(OpError::KeyNotFound);
            else
                return OpStatus(OpError::Internal);
        }

        virtual OpStatus set(std::string_view key, std::string_view value)
        {
            if (inner == nullptr)
                return OpStatus(OpError::DbNotInit);

            auto status = inner->Put(rocksdb::WriteOptions(), key, value);

            if (status.ok())
                return OpStatus(OpError::Ok);
            else
                return OpStatus(OpError::Internal);
        }

        virtual OpStatus remove(std::string_view key)
        {
            if (inner == nullptr)
                return OpStatus(OpError::DbNotInit);

            auto status = inner->Delete(rocksdb::WriteOptions(), key);

            if (status.ok())
                return OpStatus(OpError::Ok);
            else if (status.IsNotFound())
                return OpStatus(OpError::KeyNotFound);
            else
                return OpStatus(OpError::Internal);
        }

        virtual OpStatus scan(std::string_view start_key, std::string_view end_key)
        {
            if (inner == nullptr)
                return OpStatus(OpError::DbNotInit);
        }

        ~RocksDB()
        {
            delete inner;
            inner = nullptr;
        }

    private:
        rocksdb::DB *inner = nullptr;
    };
} // namespace cyber
