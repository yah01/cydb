#include <filesystem>

#include "engines/rocksdb.hpp"
#include "gtest/gtest.h"

namespace
{
    using namespace cyber;

    class RocksdbTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            engine = new RocksDB();
            auto s = engine->open("test_db");
            ASSERT_EQ(s.err, OpError::Ok) << "can't open file";
        }
        void TearDown() override
        {
            delete engine;
        }

        static void SetUpTestSuite() { std::filesystem::remove_all("test_db"); }

        RocksDB *engine;
    };

    TEST_F(RocksdbTest, bench)
    {
        for (int i = 0; i < 1000; i++)
        {
            auto s = engine->set(std::to_string(i), std::to_string(i));
            ASSERT_EQ(s.err, OpError::Ok);

            s = engine->get(std::to_string(i));
            ASSERT_EQ(s.err, OpError::Ok) << "failed at " << i;
            ASSERT_EQ(s.value, std::to_string(i)) << "failed at " << i;

            s = engine->remove(std::to_string(i));
            ASSERT_EQ(s.err, OpError::Ok) << "failed at " << i;

            s = engine->get(std::to_string(i));
            ASSERT_EQ(s.err, OpError::KeyNotFound) << "failed at " << i;
        }
    }
}