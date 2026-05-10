#include <cstdint>
#include <gtest/gtest.h>

#include "protocol/serializer.hpp"

TEST(SerializerTest, SerializesApiVersionsResponseValid) {
    ApiVersionsResponse resp{
        .correlation_id = 1870644833,
        .error_code = 0,
        .api_keys = {},
        .throttle_time_ms = 0,
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 16);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x0C);  // message_size = 12
    EXPECT_EQ(bytes[10], 0x01); // compact array length = 0 (encoded as 1)
    EXPECT_EQ(bytes[15], 0x00); // TAG_BUFFER
}

TEST(SerializerTest, SerializesApiVersionsResponseUnsupported) {
    ApiVersionsResponse resp{
        .correlation_id = 1870644833,
        .error_code = 35,
        .api_keys = {},
        .throttle_time_ms = 0,
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 16);
    EXPECT_EQ(bytes[9], 0x23); // error_code = 35
}

TEST(SerializerTest, SerializesDescribeTopicPartitionsUnknownTopic) {
    DescribeTopicPartitionsResponse resp{
        .correlation_id = 0x12345678,
        .throttle_time_ms = 0,
        .topics =
            {
                TopicMetadata{
                    .error_code = 3,
                    .topic_name = "foo",
                    .topic_id = {},
                    .is_internal = false,
                    .authorized_operations = 0,
                },
            },
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 45);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x29); // message_size = 41
    EXPECT_EQ(bytes[4], 0x12);
    EXPECT_EQ(bytes[5], 0x34);
    EXPECT_EQ(bytes[6], 0x56);
    EXPECT_EQ(bytes[7], 0x78); // correlation_id
    EXPECT_EQ(bytes[8], 0x00); // TAG_BUFFER (response header v1)
    EXPECT_EQ(bytes[9], 0x00);
    EXPECT_EQ(bytes[10], 0x00);
    EXPECT_EQ(bytes[11], 0x00);
    EXPECT_EQ(bytes[12], 0x00); // throttle_time_ms = 0
    EXPECT_EQ(bytes[13], 0x02); // topics array length = 1
    EXPECT_EQ(bytes[14], 0x00);
    EXPECT_EQ(bytes[15], 0x03); // error_code = 3
    EXPECT_EQ(bytes[16], 0x04); // topic_name length = 3
    EXPECT_EQ(bytes[17], 'f');
    EXPECT_EQ(bytes[18], 'o');
    EXPECT_EQ(bytes[19], 'o');
    EXPECT_EQ(bytes[36], 0x00); // is_internal = false
    EXPECT_EQ(bytes[37], 0x01); // partitions array length = 0
    EXPECT_EQ(bytes[38], 0x00);
    EXPECT_EQ(bytes[39], 0x00);
    EXPECT_EQ(bytes[40], 0x00);
    EXPECT_EQ(bytes[41], 0x00); // authorized_operations = 0
    EXPECT_EQ(bytes[42], 0x00); // topic TAG_BUFFER
    EXPECT_EQ(bytes[43], 0xFF); // next_cursor = -1 (null)
    EXPECT_EQ(bytes[44], 0x00); // body TAG_BUFFER
}

TEST(SerializerTest, SerializesFullApiVersionsResponse) {
    ApiVersionsResponse resp{
        .correlation_id = 0x0a0b0c0d,
        .error_code = 0,
        .api_keys = {{.api_key = 18, .min_version = 0, .max_version = 4}},
        .throttle_time_ms = 1234,
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 23);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x13); // message_size = 19
    EXPECT_EQ(bytes[4], 0x0a);
    EXPECT_EQ(bytes[5], 0x0b);
    EXPECT_EQ(bytes[6], 0x0c);
    EXPECT_EQ(bytes[7], 0x0d); // correlation_id
    EXPECT_EQ(bytes[8], 0x00);
    EXPECT_EQ(bytes[9], 0x00);  // error_code = 0
    EXPECT_EQ(bytes[10], 0x02); // compact array length = 1
    EXPECT_EQ(bytes[11], 0x00);
    EXPECT_EQ(bytes[12], 0x12); // api_key = 18
    EXPECT_EQ(bytes[13], 0x00);
    EXPECT_EQ(bytes[14], 0x00); // min_version = 0
    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x04); // max_version = 4
    EXPECT_EQ(bytes[17], 0x00); // TAG_BUFFER (entry)
    EXPECT_EQ(bytes[18], 0x00);
    EXPECT_EQ(bytes[19], 0x00);
    EXPECT_EQ(bytes[20], 0x04);
    EXPECT_EQ(bytes[21], 0xd2); // throttle_time_ms = 1234
    EXPECT_EQ(bytes[22], 0x00); // TAG_BUFFER
}
