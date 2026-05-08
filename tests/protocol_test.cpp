#include <array>
#include <cstdint>
#include <span>
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

TEST(ProtocolTest, DecodeInt32BeZero) {
    const std::array<uint8_t, 4> buf{0x00, 0x00, 0x00, 0x00};
    EXPECT_EQ(decode_int32_be(buf), 0);
}

TEST(ProtocolTest, DecodeInt32BeSeven) {
    const std::array<uint8_t, 4> buf{0x00, 0x00, 0x00, 0x07};
    EXPECT_EQ(decode_int32_be(buf), 7);
}

TEST(ProtocolTest, DecodeInt32BeLargeValue) {
    const std::array<uint8_t, 4> buf{0x01, 0x02, 0x03, 0x04};
    EXPECT_EQ(decode_int32_be(buf), 0x01020304);
}

TEST(ProtocolTest, DecodeInt32BeRoundTrip) {
    constexpr int32_t value = 1870644833;
    auto encoded = encode_int32_be(value);
    EXPECT_EQ(decode_int32_be(encoded), value);
}

TEST(ProtocolTest, DecodeInt32BeWithOffset) {
    const std::array<uint8_t, 12> buf{
        0xAA, 0xBB, 0xCC, 0xDD, 0xCC, 0xDD, 0xEE, 0xFF,
        0x6f, 0x7f, 0xc6, 0x61
    };
    EXPECT_EQ(decode_int32_be(std::span{buf}.subspan<8, 4>()), 1870644833);
}

TEST(ProtocolTest, BuildResponseCorrectSize) {
    auto response = build_response(0);
    EXPECT_EQ(response.size(), 8);
}

TEST(ProtocolTest, BuildResponseMessageSizeIsZero) {
    auto response = build_response(42);
    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x00);
}

TEST(ProtocolTest, BuildResponseCorrelationIdZero) {
    auto response = build_response(0);
    EXPECT_EQ(response[4], 0x00);
    EXPECT_EQ(response[5], 0x00);
    EXPECT_EQ(response[6], 0x00);
    EXPECT_EQ(response[7], 0x00);
}

TEST(ProtocolTest, BuildResponseCorrelationIdSeven) {
    auto response = build_response(7);
    EXPECT_EQ(response[4], 0x00);
    EXPECT_EQ(response[5], 0x00);
    EXPECT_EQ(response[6], 0x00);
    EXPECT_EQ(response[7], 0x07);
}

TEST(ProtocolTest, BuildResponseCorrelationIdLarge) {
    auto response = build_response(1870644833);
    EXPECT_EQ(response[4], 0x6f);
    EXPECT_EQ(response[5], 0x7f);
    EXPECT_EQ(response[6], 0xc6);
    EXPECT_EQ(response[7], 0x61);
}
