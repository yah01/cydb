#include <iostream>

#include "rocksdb/db.h"

void init_db(rocksdb::DB *&db, const char *db_name)
{
    rocksdb::Options options;

    options.create_if_missing = true;

    rocksdb::Status status = rocksdb::DB::Open(options, db_name, &db);
    if (!status.ok())
    {
        std::cerr << "can't open db: " << status.getState();
        exit(-1);
    }

    // db->Put(rocksdb::WriteOptions(), "hello", "world");

    // std::string value;
    // db->Get(rocksdb::ReadOptions(), "hello", &value);
}