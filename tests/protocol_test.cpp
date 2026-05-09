#include <array>
#include <cstdint>
#include <gtest/gtest.h>
#include <span>

#include "protocol.hpp"

TEST(ProtocolTest, WriteInt32BeZero) {
    std::array<uint8_t, 4> buf{};
    write_int32_be(0, buf);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_EQ(buf[1], 0x00);
    EXPECT_EQ(buf[2], 0x00);
    EXPECT_EQ(buf[3], 0x00);
}

TEST(ProtocolTest, WriteInt32BeSeven) {
    std::array<uint8_t, 4> buf{};
    write_int32_be(7, buf);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_EQ(buf[1], 0x00);
    EXPECT_EQ(buf[2], 0x00);
    EXPECT_EQ(buf[3], 0x07);
}

TEST(ProtocolTest, WriteInt32BeLargeValue) {
    std::array<uint8_t, 4> buf{};
    write_int32_be(0x01020304, buf);
    EXPECT_EQ(buf[0], 0x01);
    EXPECT_EQ(buf[1], 0x02);
    EXPECT_EQ(buf[2], 0x03);
    EXPECT_EQ(buf[3], 0x04);
}

TEST(ProtocolTest, DecodeInt16BeZero) {
    const std::array<uint8_t, 2> buf{0x00, 0x00};
    EXPECT_EQ(decode_int16_be(buf), 0);
}

TEST(ProtocolTest, DecodeInt16BeValue) {
    const std::array<uint8_t, 2> buf{0x00, 0x23};
    EXPECT_EQ(decode_int16_be(buf), 35);
}

TEST(ProtocolTest, DecodeInt16BeRoundTrip) {
    constexpr int16_t value = 26442;
    std::array<uint8_t, 2> buf{};
    write_int16_be(value, buf);
    EXPECT_EQ(decode_int16_be(buf), value);
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
    std::array<uint8_t, 4> buf{};
    write_int32_be(value, buf);
    EXPECT_EQ(decode_int32_be(buf), value);
}

TEST(ProtocolTest, DecodeInt32BeWithOffset) {
    const std::array<uint8_t, 12> buf{
        0xAA, 0xBB, 0xCC, 0xDD, 0xCC, 0xDD, 0xEE, 0xFF, 0x6f, 0x7f, 0xc6, 0x61};
    EXPECT_EQ(decode_int32_be(std::span{buf}.subspan<8, 4>()), 1870644833);
}

TEST(ProtocolTest, WriteInt16BeZero) {
    std::array<uint8_t, 2> buf{};
    write_int16_be(0, buf);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_EQ(buf[1], 0x00);
}

TEST(ProtocolTest, WriteInt16BeValue) {
    std::array<uint8_t, 2> buf{};
    write_int16_be(35, buf);
    EXPECT_EQ(buf[0], 0x00);
    EXPECT_EQ(buf[1], 0x23);
}

TEST(ProtocolTest, BuildResponseErrorCodeZero) {
    auto response = build_response(0, 0);
    EXPECT_EQ(response[8], 0x00);
    EXPECT_EQ(response[9], 0x00);
}

TEST(ProtocolTest, BuildResponseErrorCodeUnsupported) {
    auto response = build_response(0, 35);
    EXPECT_EQ(response[8], 0x00);
    EXPECT_EQ(response[9], 0x23);
}

TEST(ProtocolTest, BuildResponseCorrectSize) {
    auto response = build_response(0, 0);
    EXPECT_EQ(response.size(), 10);
}

TEST(ProtocolTest, BuildResponseMessageSizeIsZero) {
    auto response = build_response(42, 0);
    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x00);
}

TEST(ProtocolTest, BuildResponseCorrelationIdZero) {
    auto response = build_response(0, 0);
    EXPECT_EQ(response[4], 0x00);
    EXPECT_EQ(response[5], 0x00);
    EXPECT_EQ(response[6], 0x00);
    EXPECT_EQ(response[7], 0x00);
}

TEST(ProtocolTest, BuildResponseCorrelationIdSeven) {
    auto response = build_response(7, 0);
    EXPECT_EQ(response[4], 0x00);
    EXPECT_EQ(response[5], 0x00);
    EXPECT_EQ(response[6], 0x00);
    EXPECT_EQ(response[7], 0x07);
}

TEST(ProtocolTest, BuildResponseCorrelationIdLarge) {
    auto response = build_response(1870644833, 0);
    EXPECT_EQ(response[4], 0x6f);
    EXPECT_EQ(response[5], 0x7f);
    EXPECT_EQ(response[6], 0xc6);
    EXPECT_EQ(response[7], 0x61);
}
