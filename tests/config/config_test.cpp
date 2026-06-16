#include <cstring>
#include <fstream>

#include <gtest/gtest.h>

#include "config/config.hpp"

namespace {

void write_temp_file(const std::string& path, const std::string& content) {
    std::ofstream file(path, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.write(content.data(), static_cast<std::streamsize>(content.size()));
}

struct CliArgs {
    std::vector<char*> ptrs;
    std::vector<std::vector<char>> storage;

    explicit CliArgs(std::initializer_list<std::string> args) {
        storage.reserve(args.size());
        for (const auto& arg : args) {
            auto& buf = storage.emplace_back(arg.size() + 1, '\0');
            std::memcpy(buf.data(), arg.data(), arg.size());
            ptrs.push_back(buf.data());
        }
    }

    auto argc() -> int { return static_cast<int>(ptrs.size()); }
    auto argv() -> char** { return ptrs.data(); }
};

} // namespace

TEST(ConfigTest, AllDefaults) {
    CliArgs args({});
    auto config = config::Config::load(args.argc(), args.argv(), "/nonexistent/path");
    EXPECT_EQ(config.port, 9092);
    EXPECT_EQ(config.log_root, "/tmp/kraft-combined-logs");
    EXPECT_EQ(config.max_message_bytes, 1'048'576U);
}

TEST(ConfigTest, LoadFromFile) {
    const std::string path = "/tmp/test_config_file.properties";
    write_temp_file(path, "port=1234\nlog.dirs=/data/logs\nmax.message.bytes=2097152\n");

    CliArgs args({});
    auto config = config::Config::load(args.argc(), args.argv(), path);
    EXPECT_EQ(config.port, 1234);
    EXPECT_EQ(config.log_root, "/data/logs");
    EXPECT_EQ(config.max_message_bytes, 2097152U);
}

TEST(ConfigTest, CliOverridesDefaults) {
    CliArgs args({"./kafka", "--port=9093", "--log.dirs=/cli/path", "--max.message.bytes=65536"});
    auto config = config::Config::load(args.argc(), args.argv(), "/nonexistent/path");
    EXPECT_EQ(config.port, 9093);
    EXPECT_EQ(config.log_root, "/cli/path");
    EXPECT_EQ(config.max_message_bytes, 65536U);
}

TEST(ConfigTest, FileThenCliOverride) {
    const std::string path = "/tmp/test_config_mix.properties";
    write_temp_file(path, "port=1111\nlog.dirs=/file/path\n");

    CliArgs args({"./kafka", "--port=2222"});
    auto config = config::Config::load(args.argc(), args.argv(), path);
    EXPECT_EQ(config.port, 2222);
    EXPECT_EQ(config.log_root, "/file/path");
    EXPECT_EQ(config.max_message_bytes, 1'048'576U);
}

TEST(ConfigTest, UnknownKeyAndInvalidPort) {
    const std::string path = "/tmp/test_config_edge.properties";
    write_temp_file(path, "unknown.key=bar\nport=abc\n");

    CliArgs args({});
    auto config = config::Config::load(args.argc(), args.argv(), path);
    EXPECT_EQ(config.port, 9092);
    EXPECT_EQ(config.log_root, "/tmp/kraft-combined-logs");
    EXPECT_EQ(config.max_message_bytes, 1'048'576U);
}
