#include <array>
#include <cstdint>
#include <gtest/gtest.h>

#include "protocol/parser.hpp"

TEST(ParserTest, ParsesValidApiVersionsRequest) {
    std::array<std::uint8_t, 12> buf{};
    buf[4] = 0x00;
    buf[5] = 0x12;
    buf[6] = 0x00;
    buf[7] = 0x04;
    buf[8] = 0x6f;
    buf[9] = 0x7f;
    buf[10] = 0xc6;
    buf[11] = 0x61;

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto& req = std::get<ApiVersionsRequest>(*result);
    EXPECT_EQ(req.header.api_key, 18);
    EXPECT_EQ(req.header.api_version, 4);
    EXPECT_EQ(req.header.correlation_id, 1870644833);
}

TEST(ParserTest, RejectsShortBuffer) {
    std::array<std::uint8_t, 8> buf{};
    auto result = parse_request(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ParserTest, RejectsUnknownApiKey) {
    std::array<std::uint8_t, 12> buf{};
    buf[4] = 0x00;
    buf[5] = 0x01;

    auto result = parse_request(buf);
    EXPECT_FALSE(result.has_value());
}
