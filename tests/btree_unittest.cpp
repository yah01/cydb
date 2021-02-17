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
            auto s = engine->open("test_db");
            ASSERT_EQ(s.err, OpError::Ok) << "can't open file";
        }
        void TearDown() override
        {
            delete engine;
        }

        static void SetUpTestSuite() { std::filesystem::remove_all("test_db"); }

        BTree *engine;
    };

    TEST_F(BTreeTest, split)
    {
        cyber::OpStatus s;
        try
        {
            for (int i = 0; true; i++)
            {
                s = engine->set(std::to_string(i), std::to_string(i));
                ASSERT_EQ(s.err, OpError::Ok);

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
        catch (const std::exception &e)
        {
            ASSERT_TRUE(false) << e.what();
        }
    }

    TEST_F(BTreeTest, bench)
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

    TEST_F(BTreeTest, get_set)
    {
        ASSERT_EQ(engine->metadata().node_num, 3) << "metadata.data_num = " << engine->metadata().node_num;
        ASSERT_EQ(engine->metadata().root_id, 2) << "metadata.data_num = " << engine->metadata().root_id;

        auto s = engine->get("hello");
        ASSERT_EQ(s.err, OpError::KeyNotFound);

        s = engine->set("hello", "world");
        ASSERT_EQ(s.err, OpError::Ok);
        s = engine->get("hello");
        ASSERT_EQ(s.err, OpError::Ok);
        ASSERT_STREQ(s.value.data(), "world") << "s.value = " << s.value;

        s = engine->set("cyber", "yah2er0ne");
        ASSERT_EQ(s.err, OpError::Ok);
    }

    TEST_F(BTreeTest, remove)
    {
        auto s = engine->remove("hello");
        ASSERT_EQ(s.err, OpError::Ok);

        s = engine->get("hello");
        ASSERT_EQ(s.err, OpError::KeyNotFound);

        s = engine->get("cyber");
        ASSERT_EQ(s.err, OpError::Ok);
        ASSERT_STREQ(s.value.data(), "yah2er0ne") << "s.value = " << s.value;
    }

    TEST_F(BTreeTest, reopen)
    {
        auto s = engine->get("hello");
        ASSERT_EQ(s.err, OpError::KeyNotFound);

        s = engine->get("cyber");
        ASSERT_EQ(s.err, OpError::Ok);
        ASSERT_STREQ(s.value.data(), "yah2er0ne") << "s.value = " << s.value;
    }
} // namespace