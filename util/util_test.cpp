#include <iostream>
#include <gtest/gtest.h>
#include "util.h"

TEST(sprintf, all) 
{
    EXPECT_EQ("", fmt::sprintf(""));
    EXPECT_EQ("format only", fmt::sprintf("format only"));

    EXPECT_EQ("28", fmt::sprintf("%v", 28));
    EXPECT_EQ("multi28", fmt::sprintf("%v%v", "multi", 28));

    EXPECT_EQ("value, text", fmt::sprintf("%v, text", "value"));
    EXPECT_EQ("text, value", fmt::sprintf("text, %v", "value"));
    EXPECT_EQ("text, value, text", fmt::sprintf("text, %v, text", "value"));
    
    EXPECT_THROW(fmt::sprintf("miss: %v"), std::invalid_argument);
    EXPECT_THROW(fmt::sprintf("extra: %v", 1, 2), std::invalid_argument);
}


TEST(toUpper, all)
{
    EXPECT_EQ("123ABC.Z", toUpper("123abc.z"));
}


TEST(trims, all)
{
    EXPECT_EQ(trimRight("hello );", " );"), "hello");
    EXPECT_EQ(trimLeft(" ;) hello","; )"), "hello");
    EXPECT_EQ(trim("\n\nhello \r\t"), "hello");

    EXPECT_EQ(trimRight("hello", " );"), "hello");
    EXPECT_EQ(trimLeft("hello","; )"), "hello");
    EXPECT_EQ(trim("hello"), "hello");
}

int main(int argc, char *argv[])
{
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}