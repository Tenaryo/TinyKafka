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

TEST(ParserTest, ParsesMultipleTopicsWithTailFields) {
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
    buf.push_back(0x03);
    buf.push_back(0x06);
    buf.push_back('z');
    buf.push_back('e');
    buf.push_back('b');
    buf.push_back('r');
    buf.push_back('a');
    buf.push_back(0x00);
    buf.push_back(0x06);
    buf.push_back('a');
    buf.push_back('p');
    buf.push_back('p');
    buf.push_back('l');
    buf.push_back('e');
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
    ASSERT_EQ(req->topic_names.size(), 2u);
    EXPECT_EQ(req->topic_names[0], "zebra");
    EXPECT_EQ(req->topic_names[1], "apple");
}

TEST(ParserTest, ParsesFetchV16Request) {
    std::vector<std::uint8_t> buf;
    buf.push_back(0x00);
    buf.push_back(0x01);
    buf.push_back(0x00);
    buf.push_back(0x10);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x2A);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x01);
    buf.push_back(0xF4);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x01);
    buf.push_back(0x00);
    buf.push_back(0x10);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x01);
    buf.push_back(0x01);
    buf.push_back(0x01);
    buf.push_back(0x00);
    buf.push_back(0x00);

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<FetchRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 1);
    EXPECT_EQ(req->header.api_version, 16);
    EXPECT_EQ(req->header.correlation_id, 42);
}

TEST(ParserTest, ParsesFetchV16RequestWithTopicId) {
    constexpr std::array<uint8_t, 16> expected_uuid = {
        0xa1,
        0xb2,
        0xc3,
        0xd4,
        0xe5,
        0xf6,
        0xa7,
        0xb8,
        0xc9,
        0xd0,
        0xe1,
        0xf2,
        0xa3,
        0xb4,
        0xc5,
        0xd6,
    };
    std::vector<std::uint8_t> buf;
    auto push_be16 = [&](int16_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto push_be32 = [&](int32_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto push_be64 = [&](int64_t v) {
        for (int i = 7; i >= 0; --i)
            buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    };

    push_be16(1);
    push_be16(16);
    push_be32(42);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    push_be32(500);
    push_be32(1);
    push_be32(0x00100000);
    buf.push_back(0x00);
    push_be32(0);
    push_be32(0);
    buf.push_back(0x02);
    buf.insert(buf.end(), expected_uuid.begin(), expected_uuid.end());
    buf.push_back(0x02);
    push_be32(0);
    push_be32(-1);
    push_be64(0);
    push_be32(-1);
    push_be64(-1);
    push_be32(0x00100000);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x01);
    buf.push_back(0x01);
    buf.push_back(0x00);

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<FetchRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 1);
    EXPECT_EQ(req->header.api_version, 16);
    EXPECT_EQ(req->header.correlation_id, 42);
    ASSERT_EQ(req->topic_ids.size(), 1u);
    EXPECT_EQ(req->topic_ids[0], expected_uuid);
}
