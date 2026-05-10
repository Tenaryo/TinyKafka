#include <array>
#include <cstdint>
#include <gtest/gtest.h>

#include "protocol/parser.hpp"

TEST(ParserTest, ParsesValidApiVersionsRequest) {
    std::array<std::uint8_t, 8> buf{};
    buf[0] = 0x00;
    buf[1] = 0x12;
    buf[2] = 0x00;
    buf[3] = 0x04;
    buf[4] = 0x6f;
    buf[5] = 0x7f;
    buf[6] = 0xc6;
    buf[7] = 0x61;

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto& req = std::get<ApiVersionsRequest>(*result);
    EXPECT_EQ(req.header.api_key, 18);
    EXPECT_EQ(req.header.api_version, 4);
    EXPECT_EQ(req.header.correlation_id, 1870644833);
}

TEST(ParserTest, RejectsShortBuffer) {
    std::array<std::uint8_t, 4> buf{};
    auto result = parse_request(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ParserTest, RejectsUnknownApiKey) {
    std::array<std::uint8_t, 8> buf{};
    buf[0] = 0x00;
    buf[1] = 0x01;

    auto result = parse_request(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ParserTest, ParsesDescribeTopicPartitionsRequest) {
    std::vector<std::uint8_t> buf;
    buf.push_back(0x00);
    buf.push_back(0x4B);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x07);
    buf.push_back(0xFF);
    buf.push_back(0xFF);
    buf.push_back(0x00);
    buf.push_back(0x02);
    buf.push_back(0x04);
    buf.push_back('f');
    buf.push_back('o');
    buf.push_back('o');
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x64);
    buf.push_back(0xFF);
    buf.push_back(0x00);

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<DescribeTopicPartitionsRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 75);
    EXPECT_EQ(req->header.api_version, 0);
    EXPECT_EQ(req->header.correlation_id, 7);
    EXPECT_EQ(req->topic_names.size(), 1u);
    EXPECT_EQ(req->topic_names[0], "foo");
}
