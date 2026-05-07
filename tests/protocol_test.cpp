#include <array>
#include <cstdint>
#include <gtest/gtest.h>

#include "protocol.hpp"

TEST(ProtocolTest, EncodeInt32BeZero) {
    auto bytes = encode_int32_be(0);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x00);
}

TEST(ProtocolTest, EncodeInt32BeSeven) {
    auto bytes = encode_int32_be(7);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x07);
}

TEST(ProtocolTest, EncodeInt32BeLargeValue) {
    auto bytes = encode_int32_be(0x01020304);
    EXPECT_EQ(bytes[0], 0x01);
    EXPECT_EQ(bytes[1], 0x02);
    EXPECT_EQ(bytes[2], 0x03);
    EXPECT_EQ(bytes[3], 0x04);
}

TEST(ProtocolTest, BuildResponseV0HasCorrectSize) {
    auto response = build_response_v0();
    EXPECT_EQ(response.size(), 8);
}

TEST(ProtocolTest, BuildResponseV0MessageSizeIsZero) {
    auto response = build_response_v0();
    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x00);
}

TEST(ProtocolTest, BuildResponseV0CorrelationIdIsSeven) {
    auto response = build_response_v0();
    EXPECT_EQ(response[4], 0x00);
    EXPECT_EQ(response[5], 0x00);
    EXPECT_EQ(response[6], 0x00);
    EXPECT_EQ(response[7], 0x07);
}
