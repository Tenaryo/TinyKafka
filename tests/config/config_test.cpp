#include <fstream>

#include <gtest/gtest.h>

#include "config/properties.hpp"

namespace {

void write_temp_file(const std::string& path, const std::string& content) {
    std::ofstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.write(content.data(), static_cast<std::streamsize>(content.size()));
}

} // namespace

TEST(PropertiesTest, BasicKeyValueParsing) {
    const std::string path = "/tmp/test_basic.properties";
    write_temp_file(path, "host=localhost\nport=9092\n");

    auto result = config::load_properties(path);
    ASSERT_TRUE(result.has_value());
    const auto& props = *result;
    EXPECT_EQ(props.at("host"), "localhost");
    EXPECT_EQ(props.at("port"), "9092");
}

TEST(PropertiesTest, CommentsAndEmptyLines) {
    const std::string path = "/tmp/test_comments.properties";
    write_temp_file(path,
                    R"(# this is a comment
key1=value1

key2=value2
# another comment
key3=value3
)");

    auto result = config::load_properties(path);
    ASSERT_TRUE(result.has_value());
    const auto& props = *result;
    EXPECT_EQ(props.size(), 3U);
    EXPECT_EQ(props.at("key1"), "value1");
    EXPECT_EQ(props.at("key2"), "value2");
    EXPECT_EQ(props.at("key3"), "value3");
}

TEST(PropertiesTest, FileNotFound) {
    auto result = config::load_properties("/nonexistent/path/config.properties");
    EXPECT_FALSE(result.has_value());
}

TEST(PropertiesTest, WhitespaceTrimmingAndInternalPreservation) {
    const std::string path = "/tmp/test_whitespace.properties";
    write_temp_file(path,
                    R"(  host  =  localhost  
message = hello world
)");

    auto result = config::load_properties(path);
    ASSERT_TRUE(result.has_value());
    const auto& props = *result;
    EXPECT_EQ(props.at("host"), "localhost");
    EXPECT_EQ(props.at("message"), "hello world");
}

TEST(PropertiesTest, EdgeCasesNoEqualsDuplicateKeyCrLf) {
    const std::string path = "/tmp/test_edge.properties";
    write_temp_file(path,
                    "a=1\r\n"
                    "orphan_line\r\n"
                    "a=2\r\n");

    auto result = config::load_properties(path);
    ASSERT_TRUE(result.has_value());
    const auto& props = *result;
    EXPECT_EQ(props.size(), 1U);
    EXPECT_EQ(props.at("a"), "2");
}
