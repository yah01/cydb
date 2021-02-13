#include <filesystem>

#include "btree/btree.hpp"
#include "gtest/gtest.h"

namespace
{
    using namespace cyber;

    class BTreeTest : public ::testing::Test
    {
    protected:
        virtual ~BTreeTest()
        {
            std::filesystem::remove_all("testdb");
        }

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
    }

} // namespace