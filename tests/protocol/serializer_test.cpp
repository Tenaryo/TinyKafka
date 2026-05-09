#include <cstdint>
#include <gtest/gtest.h>

#include "protocol/serializer.hpp"

TEST(SerializerTest, SerializesApiVersionsResponseValid) {
    ApiVersionsResponse resp{1870644833, 0};
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 10);
    EXPECT_EQ(bytes[8], 0x00);
    EXPECT_EQ(bytes[9], 0x00);
}

TEST(SerializerTest, SerializesApiVersionsResponseUnsupported) {
    ApiVersionsResponse resp{1870644833, 35};
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 10);
    EXPECT_EQ(bytes[8], 0x00);
    EXPECT_EQ(bytes[9], 0x23);
}
