#include <filesystem>

#include "engines/btree/btree.hpp"
#include "gtest/gtest.h"

namespace
{
    using namespace cyber;

    class BTreeTest : public ::testing::Test
    {
    protected:
        void SetUp() override
        {
            engine = new BTree();
            auto s = engine->open("testdb");
            ASSERT_EQ(s.err, OpError::Ok) << "can't open file";
        }
        void TearDown() override
        {
            delete engine;
        }

        static void SetUpTestSuite() { std::filesystem::remove_all("testdb"); }
        static void TearDownTestSuite() { std::filesystem::remove_all("testdb"); }

        BTree *engine;
    };

    TEST_F(BTreeTest, get_set)
    {
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

    TEST_F(BTreeTest, split)
    {
        for (int i = 0; true; i++)
        {
            auto s = engine->set(std::to_string(i), std::to_string(i));
            EXPECT_EQ(s.err, OpError::Ok);

            // splitted
            if (engine->metadata().node_num > 1)
            {
                // read
                for (int j = 0; j <= i; j++)
                {
                    s = engine->get(std::to_string(j));
                    ASSERT_EQ(s.err, OpError::Ok) << "j=" << j;
                    ASSERT_EQ(s.value, std::to_string(j)) << "j=" << j;
                }

                // remove
                for (int j = 0; j <= i; j++)
                {
                    s = engine->remove(std::to_string(j));
                    ASSERT_EQ(s.err, OpError::Ok);
                }

                // read after remove all data
                for (int j = 0; j <= i; j++)
                {
                    s = engine->get(std::to_string(j));
                    ASSERT_EQ(s.err, OpError::KeyNotFound);
                }

                break;
            }
        }
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