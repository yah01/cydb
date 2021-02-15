#include <filesystem>

#include "engines/btree/btree.hpp"
#include "gtest/gtest.h"

namespace
{
    using namespace cyber;
    using namespace testing::internal;

    class BTreeTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            engine = new BTree();
            auto s = engine->open("test_db");
            ASSERT_EQ(s.err, OpError::Ok) << "can't open file";
        }
        void TearDown() override
        {
            delete engine;
        }

        static void SetUpTestSuite() { std::filesystem::remove_all("test_db"); }
        static void TearDownTestSuite() { std::filesystem::remove_all("test_db"); }

        BTree *engine;
    };

    TEST_F(BTreeTest, split)
    {
        for (int i = 0; true; i++)
        {
            auto s = engine->set(std::to_string(i), std::to_string(i));
            EXPECT_EQ(s.err, OpError::Ok);

            // splitted
            if (engine->metadata().node_num > 1)
            {
                ASSERT_EQ(engine->metadata().data_num, i + 1) << "metadata.data_num = " << engine->metadata().data_num;
                ASSERT_EQ(engine->metadata().node_num, 3) << "metadata.data_num = " << engine->metadata().node_num;
                ASSERT_EQ(engine->metadata().root_id, 2) << "metadata.data_num = " << engine->metadata().root_id;
                // read
                for (int j = 0; j <= i; j++)
                {
                    s = engine->get(std::to_string(j));
                    ASSERT_EQ(s.err, OpError::Ok) << "failed at " << j;
                    ASSERT_EQ(s.value, std::to_string(j)) << "failed at " << j;
                }

                // remove
                for (int j = 0; j <= i; j++)
                {
                    s = engine->remove(std::to_string(j));
                    ASSERT_EQ(s.err, OpError::Ok) << "failed at " << j;
                }
                ASSERT_EQ(engine->metadata().data_num, 0) << "metadata.data_num = " << engine->metadata().data_num;

                // read after remove all data
                for (int j = 0; j <= i; j++)
                {
                    s = engine->get(std::to_string(j));
                    ASSERT_EQ(s.err, OpError::KeyNotFound) << "failed at " << j;
                }

                break;
            }
        }
    }

    TEST_F(BTreeTest, get_set)
    {
        ASSERT_EQ(engine->metadata().node_num, 3) << "metadata.data_num = " << engine->metadata().node_num;
        ASSERT_EQ(engine->metadata().root_id, 2) << "metadata.data_num = " << engine->metadata().root_id;

        auto s = engine->get("hello");
        EXPECT_EQ(s.err, OpError::KeyNotFound);

        engine->set("hello", "world");
        s = engine->get("hello");
        EXPECT_EQ(s.err, OpError::Ok);
        EXPECT_STREQ(s.value.c_str(), "world");

        s = engine->set("cyber", "yah2er0ne");
        EXPECT_EQ(s.err, OpError::Ok);
    }

    TEST_F(BTreeTest, reopen)
    {
        auto s = engine->get("hello");
        EXPECT_EQ(s.err, OpError::Ok);
        EXPECT_STREQ(s.value.c_str(), "world");

        s = engine->get("cyber");
        EXPECT_EQ(s.err, OpError::Ok);
        EXPECT_STREQ(s.value.c_str(), "yah2er0ne");
    }

    TEST_F(BTreeTest, remove)
    {
        auto s = engine->remove("hello");
        EXPECT_EQ(s.err, OpError::Ok);

        s = engine->get("hello");
        EXPECT_EQ(s.err, OpError::KeyNotFound);

        s = engine->get("cyber");
        EXPECT_EQ(s.err, OpError::Ok) << "get(cyber) wrong";
        EXPECT_STREQ(s.value.c_str(), "yah2er0ne");
    }
} // namespace