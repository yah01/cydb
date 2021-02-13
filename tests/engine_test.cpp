#include <filesystem>

#include "btree/btree.hpp"
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

        static void TearDownTestSuite()
        {
            std::filesystem::remove_all("testdb");
        }

        KvEngine *engine;
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

    TEST_F(BTreeTest, remove)
    {
        auto s = engine->remove("hello");
        EXPECT_EQ(s.err, OpError::Ok) << "can't remove hello";

        s = engine->get("hello");
        EXPECT_EQ(s.err, OpError::KeyNotFound) << "get(hello) wrong";

        s = engine->get("cyber");
        EXPECT_EQ(s.err, OpError::Ok) << "get(cyber) wrong";
        EXPECT_STREQ(s.value.c_str(), "yah2er0ne") << "get(cyber) = " << s.value;
    }

} // namespace