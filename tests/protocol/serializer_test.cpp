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
                    .partitions = {},
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

TEST(SerializerTest, SerializesDescribeTopicPartitionsWithPartition) {
    DescribeTopicPartitionsResponse resp{
        .correlation_id = 0x12345678,
        .throttle_time_ms = 0,
        .topics =
            {
                TopicMetadata{
                    .error_code = 0,
                    .topic_name = "foo",
                    .topic_id = {0xa1,
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
                                 0xd6},
                    .is_internal = false,
                    .authorized_operations = 0,
                    .partitions =
                        {
                            PartitionMetadata{
                                .error_code = 0,
                                .partition_index = 0,
                                .leader_id = 1,
                                .leader_epoch = 0,
                                .replica_nodes = {1},
                                .isr_nodes = {1},
                                .eligible_leader_replicas = {},
                                .last_known_elr = {},
                                .offline_replicas = {},
                            },
                        },
                },
            },
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 73);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x45);  // message_size = 69
    EXPECT_EQ(bytes[8], 0x00);  // TAG_BUFFER (response header v1)
    EXPECT_EQ(bytes[13], 0x02); // topics array length = 1
    EXPECT_EQ(bytes[14], 0x00);
    EXPECT_EQ(bytes[15], 0x00); // error_code = 0
    EXPECT_EQ(bytes[16], 0x04); // topic_name varint = 3+1
    EXPECT_EQ(bytes[17], 'f');
    EXPECT_EQ(bytes[18], 'o');
    EXPECT_EQ(bytes[19], 'o');
    EXPECT_EQ(bytes[20], 0xa1); // topic_id
    EXPECT_EQ(bytes[35], 0xd6);
    EXPECT_EQ(bytes[36], 0x00); // is_internal = false
    EXPECT_EQ(bytes[37], 0x02); // partitions array length = 1
    EXPECT_EQ(bytes[38], 0x00);
    EXPECT_EQ(bytes[39], 0x00); // partition error_code = 0
    EXPECT_EQ(bytes[40], 0x00);
    EXPECT_EQ(bytes[41], 0x00);
    EXPECT_EQ(bytes[42], 0x00);
    EXPECT_EQ(bytes[43], 0x00); // partition_index = 0
    EXPECT_EQ(bytes[44], 0x00);
    EXPECT_EQ(bytes[45], 0x00);
    EXPECT_EQ(bytes[46], 0x00);
    EXPECT_EQ(bytes[47], 0x01); // leader_id = 1
    EXPECT_EQ(bytes[52], 0x02); // replica_nodes varint = 1+1
    EXPECT_EQ(bytes[53], 0x00);
    EXPECT_EQ(bytes[54], 0x00);
    EXPECT_EQ(bytes[55], 0x00);
    EXPECT_EQ(bytes[56], 0x01); // broker 1
    EXPECT_EQ(bytes[57], 0x02); // isr_nodes varint = 1+1
    EXPECT_EQ(bytes[62], 0x01); // eligible_leader_replicas varint = 0+1
    EXPECT_EQ(bytes[63], 0x01); // last_known_elr varint = 0+1
    EXPECT_EQ(bytes[64], 0x01); // offline_replicas varint = 0+1
    EXPECT_EQ(bytes[65], 0x00); // partition TAG_BUFFER
    EXPECT_EQ(bytes[70], 0x00); // topic TAG_BUFFER
    EXPECT_EQ(bytes[71], 0xFF); // next_cursor = -1
    EXPECT_EQ(bytes[72], 0x00); // body TAG_BUFFER
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

TEST(SerializerTest, SerializesDescribeTopicPartitionsMultiTopic) {
    DescribeTopicPartitionsResponse resp{
        .correlation_id = 0x12345678,
        .throttle_time_ms = 0,
        .topics =
            {
                TopicMetadata{
                    .error_code = 0,
                    .topic_name = "apple",
                    .topic_id = {0xa1,
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
                                 0xd6},
                    .is_internal = false,
                    .authorized_operations = 0,
                    .partitions =
                        {
                            PartitionMetadata{
                                .error_code = 0,
                                .partition_index = 0,
                                .leader_id = 1,
                                .leader_epoch = 0,
                                .replica_nodes = {1},
                                .isr_nodes = {1},
                                .eligible_leader_replicas = {},
                                .last_known_elr = {},
                                .offline_replicas = {},
                            },
                        },
                },
                TopicMetadata{
                    .error_code = 0,
                    .topic_name = "zebra",
                    .topic_id = {0xa1,
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
                                 0xd6},
                    .is_internal = false,
                    .authorized_operations = 0,
                    .partitions =
                        {
                            PartitionMetadata{
                                .error_code = 0,
                                .partition_index = 0,
                                .leader_id = 1,
                                .leader_epoch = 0,
                                .replica_nodes = {1},
                                .isr_nodes = {1},
                                .eligible_leader_replicas = {},
                                .last_known_elr = {},
                                .offline_replicas = {},
                            },
                            PartitionMetadata{
                                .error_code = 0,
                                .partition_index = 1,
                                .leader_id = 1,
                                .leader_epoch = 0,
                                .replica_nodes = {1},
                                .isr_nodes = {1},
                                .eligible_leader_replicas = {},
                                .last_known_elr = {},
                                .offline_replicas = {},
                            },
                        },
                },
            },
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 162);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x9E);  // message_size = 158
    EXPECT_EQ(bytes[8], 0x00);  // TAG_BUFFER (response header v1)
    EXPECT_EQ(bytes[13], 0x03); // topics array length = 2

    EXPECT_EQ(bytes[14], 0x00);
    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x06);
    EXPECT_EQ(bytes[17], 'a');
    EXPECT_EQ(bytes[18], 'p');
    EXPECT_EQ(bytes[19], 'p');
    EXPECT_EQ(bytes[20], 'l');
    EXPECT_EQ(bytes[21], 'e');

    EXPECT_EQ(bytes[73], 0x00);
    EXPECT_EQ(bytes[74], 0x00);
    EXPECT_EQ(bytes[75], 0x06);
    EXPECT_EQ(bytes[76], 'z');
    EXPECT_EQ(bytes[77], 'e');
    EXPECT_EQ(bytes[78], 'b');
    EXPECT_EQ(bytes[79], 'r');
    EXPECT_EQ(bytes[80], 'a');

    EXPECT_EQ(bytes[160], 0xFF); // next_cursor = -1 (null)
    EXPECT_EQ(bytes[161], 0x00); // body TAG_BUFFER
}

TEST(SerializerTest, SerializesFetchResponseEmptyTopics) {
    FetchResponse resp{
        .correlation_id = 0x12345678,
        .throttle_time_ms = 0,
        .error_code = 0,
        .session_id = 0,
        .responses = {},
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 21);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x11); // message_size = 17
    EXPECT_EQ(bytes[4], 0x12);
    EXPECT_EQ(bytes[5], 0x34);
    EXPECT_EQ(bytes[6], 0x56);
    EXPECT_EQ(bytes[7], 0x78); // correlation_id
    EXPECT_EQ(bytes[8], 0x00); // header TAG_BUFFER
    EXPECT_EQ(bytes[9], 0x00);
    EXPECT_EQ(bytes[10], 0x00);
    EXPECT_EQ(bytes[11], 0x00);
    EXPECT_EQ(bytes[12], 0x00); // throttle_time_ms = 0
    EXPECT_EQ(bytes[13], 0x00);
    EXPECT_EQ(bytes[14], 0x00); // error_code = 0
    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x00);
    EXPECT_EQ(bytes[17], 0x00);
    EXPECT_EQ(bytes[18], 0x00); // session_id = 0
    EXPECT_EQ(bytes[19], 0x01); // responses varint = 1 (0 entries)
    EXPECT_EQ(bytes[20], 0x00); // body TAG_BUFFER
}

TEST(SerializerTest, SerializesFetchApiEntry) {
    ApiVersionsResponse resp{
        .correlation_id = 1,
        .error_code = 0,
        .api_keys = {{.api_key = 1, .min_version = 0, .max_version = 16}},
        .throttle_time_ms = 0,
    };
    auto bytes = serialize(resp);

    EXPECT_EQ(bytes[10], 0x02); // compact array length = 1 (entry count + 1)
    EXPECT_EQ(bytes[11], 0x00);
    EXPECT_EQ(bytes[12], 0x01); // api_key = 1
    EXPECT_EQ(bytes[13], 0x00);
    EXPECT_EQ(bytes[14], 0x00); // min_version = 0
    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x10); // max_version = 16
    EXPECT_EQ(bytes[17], 0x00); // TAG_BUFFER (entry)
}

TEST(SerializerTest, SerializesFetchResponseUnknownTopic) {
    constexpr std::array<uint8_t, 16> topic_uuid = {
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
    FetchResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .error_code = 0,
        .session_id = 0,
        .responses =
            {
                FetchTopicResponse{
                    .topic_id = topic_uuid,
                    .partitions =
                        {
                            FetchPartitionResponse{
                                .partition_index = 0,
                                .error_code = 100,
                                .records = {},
                            },
                        },
                },
            },
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 76);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x48); // message_size = 72
    EXPECT_EQ(bytes[4], 0x00);
    EXPECT_EQ(bytes[5], 0x00);
    EXPECT_EQ(bytes[6], 0x00);
    EXPECT_EQ(bytes[7], 0x2A); // correlation_id = 42
    EXPECT_EQ(bytes[8], 0x00); // header TAG_BUFFER
    EXPECT_EQ(bytes[9], 0x00);
    EXPECT_EQ(bytes[10], 0x00);
    EXPECT_EQ(bytes[11], 0x00);
    EXPECT_EQ(bytes[12], 0x00); // throttle_time_ms = 0
    EXPECT_EQ(bytes[13], 0x00);
    EXPECT_EQ(bytes[14], 0x00); // error_code = 0
    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x00);
    EXPECT_EQ(bytes[17], 0x00);
    EXPECT_EQ(bytes[18], 0x00); // session_id = 0
    EXPECT_EQ(bytes[19], 0x02); // responses varint = 2 (1 element)
    for (size_t i = 0; i < 16; ++i) {
        EXPECT_EQ(bytes[20 + i], topic_uuid[i]);
    }
    EXPECT_EQ(bytes[36], 0x02); // partitions varint = 2 (1 element)
    EXPECT_EQ(bytes[37], 0x00);
    EXPECT_EQ(bytes[38], 0x00);
    EXPECT_EQ(bytes[39], 0x00);
    EXPECT_EQ(bytes[40], 0x00); // partition_index = 0
    EXPECT_EQ(bytes[41], 0x00);
    EXPECT_EQ(bytes[42], 0x64); // error_code = 100
    EXPECT_EQ(bytes[67], 0x01); // aborted_transactions varint (empty)
    EXPECT_EQ(bytes[72], 0x01); // records varint (empty)
    EXPECT_EQ(bytes[73], 0x00); // partition TAG_BUFFER
    EXPECT_EQ(bytes[74], 0x00); // topic TAG_BUFFER
    EXPECT_EQ(bytes[75], 0x00); // body TAG_BUFFER
}

TEST(SerializerTest, SerializesFetchResponseWithRecords) {
    constexpr std::array<uint8_t, 16> topic_uuid = {
        0x01,
        0x02,
        0x03,
        0x04,
        0x05,
        0x06,
        0x07,
        0x08,
        0x09,
        0x0a,
        0x0b,
        0x0c,
        0x0d,
        0x0e,
        0x0f,
        0x10,
    };
    std::vector<uint8_t> records_data = {0xAB, 0xCD};
    FetchResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .error_code = 0,
        .session_id = 0,
        .responses =
            {
                FetchTopicResponse{
                    .topic_id = topic_uuid,
                    .partitions =
                        {
                            FetchPartitionResponse{
                                .partition_index = 0,
                                .error_code = 0,
                                .records = records_data,
                            },
                        },
                },
            },
    };
    auto bytes = serialize(resp);

    // body_size = 4+1+4+2+4 + 1+16+1 + 35+varint(3)+2+1 + 1+1 = 74
    // total = 4+74 = 78
    ASSERT_EQ(bytes.size(), 78);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x4A); // message_size = 74

    EXPECT_EQ(bytes[4], 0x00);
    EXPECT_EQ(bytes[5], 0x00);
    EXPECT_EQ(bytes[6], 0x00);
    EXPECT_EQ(bytes[7], 0x2A); // correlation_id = 42
    EXPECT_EQ(bytes[8], 0x00); // header TAG_BUFFER

    EXPECT_EQ(bytes[9], 0x00);
    EXPECT_EQ(bytes[10], 0x00);
    EXPECT_EQ(bytes[11], 0x00);
    EXPECT_EQ(bytes[12], 0x00); // throttle_time_ms = 0

    EXPECT_EQ(bytes[13], 0x00);
    EXPECT_EQ(bytes[14], 0x00); // error_code = 0

    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x00);
    EXPECT_EQ(bytes[17], 0x00);
    EXPECT_EQ(bytes[18], 0x00); // session_id = 0

    EXPECT_EQ(bytes[19], 0x02); // responses varint = 2 (1 element)

    for (size_t i = 0; i < 16; ++i) {
        EXPECT_EQ(bytes[20 + i], topic_uuid[i]);
    }

    EXPECT_EQ(bytes[36], 0x02); // partitions varint = 2 (1 element)

    EXPECT_EQ(bytes[37], 0x00);
    EXPECT_EQ(bytes[38], 0x00);
    EXPECT_EQ(bytes[39], 0x00);
    EXPECT_EQ(bytes[40], 0x00); // partition_index = 0

    EXPECT_EQ(bytes[41], 0x00);
    EXPECT_EQ(bytes[42], 0x00); // error_code = 0

    EXPECT_EQ(bytes[67], 0x01); // aborted_transactions varint (empty)

    EXPECT_EQ(bytes[72], 0x03); // records varint = 3 (2 bytes)
    EXPECT_EQ(bytes[73], 0xAB); // records data[0]
    EXPECT_EQ(bytes[74], 0xCD); // records data[1]

    EXPECT_EQ(bytes[75], 0x00); // partition TAG_BUFFER
    EXPECT_EQ(bytes[76], 0x00); // topic TAG_BUFFER
    EXPECT_EQ(bytes[77], 0x00); // body TAG_BUFFER
}

TEST(SerializerTest, SerializesProduceResponseUnknownTopic) {
    ProduceResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .responses =
            {
                ProduceTopicResponse{
                    .topic_name = "foo",
                    .partitions =
                        {
                            ProducePartitionResponse{
                                .partition_index = 0,
                                .error_code = 3,
                                .base_offset = -1,
                                .log_append_time_ms = -1,
                                .log_start_offset = -1,
                            },
                        },
                },
            },
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 54);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);
    EXPECT_EQ(bytes[2], 0x00);
    EXPECT_EQ(bytes[3], 0x32); // message_size = 50

    EXPECT_EQ(bytes[4], 0x00);
    EXPECT_EQ(bytes[5], 0x00);
    EXPECT_EQ(bytes[6], 0x00);
    EXPECT_EQ(bytes[7], 0x2A); // correlation_id = 42

    EXPECT_EQ(bytes[8], 0x00); // header TAG_BUFFER

    EXPECT_EQ(bytes[9], 0x02); // responses varint = 2 (1 element)

    EXPECT_EQ(bytes[10], 0x04); // name varint = 4 (len=3)
    EXPECT_EQ(bytes[11], 'f');
    EXPECT_EQ(bytes[12], 'o');
    EXPECT_EQ(bytes[13], 'o');

    EXPECT_EQ(bytes[14], 0x02); // partitions varint = 2 (1 element)

    EXPECT_EQ(bytes[15], 0x00);
    EXPECT_EQ(bytes[16], 0x00);
    EXPECT_EQ(bytes[17], 0x00);
    EXPECT_EQ(bytes[18], 0x00); // partition_index = 0

    EXPECT_EQ(bytes[19], 0x00);
    EXPECT_EQ(bytes[20], 0x03); // error_code = 3

    // base_offset = -1 (8 bytes of 0xFF)
    for (size_t i = 21; i < 29; ++i)
        EXPECT_EQ(bytes[i], 0xFF);

    // log_append_time_ms = -1 (8 bytes of 0xFF)
    for (size_t i = 29; i < 37; ++i)
        EXPECT_EQ(bytes[i], 0xFF);

    // log_start_offset = -1 (8 bytes of 0xFF)
    for (size_t i = 37; i < 45; ++i)
        EXPECT_EQ(bytes[i], 0xFF);

    EXPECT_EQ(bytes[45], 0x01); // record_errors = empty (varint 1)
    EXPECT_EQ(bytes[46], 0x00); // error_message = null (varint 0)
    EXPECT_EQ(bytes[47], 0x00); // partition tagged_fields

    EXPECT_EQ(bytes[48], 0x00); // topic tagged_fields

    EXPECT_EQ(bytes[49], 0x00);
    EXPECT_EQ(bytes[50], 0x00);
    EXPECT_EQ(bytes[51], 0x00);
    EXPECT_EQ(bytes[52], 0x00); // throttle_time_ms = 0

    EXPECT_EQ(bytes[53], 0x00); // body tagged_fields
}

TEST(SerializerTest, SerializesProduceResponseValid) {
    ProduceResponse resp{
        .correlation_id = 999,
        .throttle_time_ms = 0,
        .responses =
            {
                ProduceTopicResponse{
                    .topic_name = "orders",
                    .partitions =
                        {
                            ProducePartitionResponse{
                                .partition_index = 0,
                                .error_code = 0,
                                .base_offset = 0,
                                .log_append_time_ms = -1,
                                .log_start_offset = 0,
                            },
                        },
                },
            },
    };
    auto bytes = serialize(resp);

    ASSERT_EQ(bytes.size(), 57);
    EXPECT_EQ(bytes[3], 0x35); // message_size = 53

    EXPECT_EQ(bytes[4], 0x00);
    EXPECT_EQ(bytes[5], 0x00);
    EXPECT_EQ(bytes[6], 0x03);
    EXPECT_EQ(bytes[7], 0xE7); // correlation_id = 999

    EXPECT_EQ(bytes[8], 0x00); // header TAG_BUFFER
    EXPECT_EQ(bytes[9], 0x02); // responses varint = 2

    EXPECT_EQ(bytes[10], 0x07); // name varint = 7
    EXPECT_EQ(bytes[11], 'o');
    EXPECT_EQ(bytes[12], 'r');
    EXPECT_EQ(bytes[13], 'd');
    EXPECT_EQ(bytes[14], 'e');
    EXPECT_EQ(bytes[15], 'r');
    EXPECT_EQ(bytes[16], 's');

    EXPECT_EQ(bytes[17], 0x02); // partitions varint = 2

    EXPECT_EQ(bytes[18], 0x00);
    EXPECT_EQ(bytes[19], 0x00);
    EXPECT_EQ(bytes[20], 0x00);
    EXPECT_EQ(bytes[21], 0x00); // partition_index = 0

    EXPECT_EQ(bytes[22], 0x00);
    EXPECT_EQ(bytes[23], 0x00); // error_code = 0

    for (size_t i = 24; i < 32; ++i)
        EXPECT_EQ(bytes[i], 0x00); // base_offset = 0

    for (size_t i = 32; i < 40; ++i)
        EXPECT_EQ(bytes[i], 0xFF); // log_append_time_ms = -1

    for (size_t i = 40; i < 48; ++i)
        EXPECT_EQ(bytes[i], 0x00); // log_start_offset = 0

    EXPECT_EQ(bytes[48], 0x01); // record_errors empty
    EXPECT_EQ(bytes[49], 0x00); // error_message null
    EXPECT_EQ(bytes[50], 0x00); // partition tagged_fields
    EXPECT_EQ(bytes[51], 0x00); // topic tagged_fields
    EXPECT_EQ(bytes[56], 0x00); // body tagged_fields
}

TEST(SerializerTest, SerializesMetadataResponse) {
    MetadataResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .brokers = {{.node_id = 1, .host = "localhost", .port = 9092, .rack = {}}},
        .cluster_id = "test-cluster",
        .controller_id = 1,
        .topics = {},
        .cluster_authorized_operations = 0,
    };

    auto bytes = serialize(resp);
    EXPECT_GT(bytes.size(), 20u) << "Response should have a reasonable size";
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);

    int32_t message_size = (static_cast<int32_t>(bytes[0]) << 24) |
                           (static_cast<int32_t>(bytes[1]) << 16) |
                           (static_cast<int32_t>(bytes[2]) << 8) | static_cast<int32_t>(bytes[3]);
    int32_t correlation_id = (static_cast<int32_t>(bytes[4]) << 24) |
                             (static_cast<int32_t>(bytes[5]) << 16) |
                             (static_cast<int32_t>(bytes[6]) << 8) | static_cast<int32_t>(bytes[7]);
    EXPECT_EQ(static_cast<size_t>(message_size) + 4, bytes.size());
    EXPECT_EQ(correlation_id, 42);
}

TEST(SerializerTest, SerializesListOffsetsResponse) {
    ListOffsetsResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .topics = {{.topic_name = "test",
                    .partitions =
                        {{.partition_index = 0, .error_code = 0, .offset = 5, .timestamp = -1},
                         {.partition_index = 1, .error_code = 0, .offset = 0, .timestamp = -2}}}},
    };

    auto bytes = serialize(resp);
    EXPECT_GT(bytes.size(), 20u);
    EXPECT_EQ(bytes[0], 0x00);
    EXPECT_EQ(bytes[1], 0x00);

    int32_t message_size = (static_cast<int32_t>(bytes[0]) << 24) |
                           (static_cast<int32_t>(bytes[1]) << 16) |
                           (static_cast<int32_t>(bytes[2]) << 8) | static_cast<int32_t>(bytes[3]);
    int32_t correlation_id = (static_cast<int32_t>(bytes[4]) << 24) |
                             (static_cast<int32_t>(bytes[5]) << 16) |
                             (static_cast<int32_t>(bytes[6]) << 8) | static_cast<int32_t>(bytes[7]);
    EXPECT_EQ(static_cast<size_t>(message_size) + 4, bytes.size());
    EXPECT_EQ(correlation_id, 42);
}

TEST(SerializerTest, SerializesFindCoordinatorResponse) {
    FindCoordinatorResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .error_code = 0,
        .error_message = {},
        .node_id = 1,
        .host = "localhost",
        .port = 9092,
    };

    auto bytes = serialize(resp);
    EXPECT_GT(bytes.size(), 20u);
    int32_t corr_id = (static_cast<int32_t>(bytes[4]) << 24) |
                      (static_cast<int32_t>(bytes[5]) << 16) |
                      (static_cast<int32_t>(bytes[6]) << 8) | static_cast<int32_t>(bytes[7]);
    EXPECT_EQ(corr_id, 42);
}

TEST(SerializerTest, SerializesOffsetCommitResponse) {
    OffsetCommitResponse resp{
        .correlation_id = 42,
        .throttle_time_ms = 0,
        .topics = {
            {.topic_name = "test", .partitions = {{.partition_index = 0, .error_code = 0}}}}};

    auto bytes = serialize(resp);
    EXPECT_GT(bytes.size(), 20u);
    int32_t corr_id = (static_cast<int32_t>(bytes[4]) << 24) |
                      (static_cast<int32_t>(bytes[5]) << 16) |
                      (static_cast<int32_t>(bytes[6]) << 8) | static_cast<int32_t>(bytes[7]);
    EXPECT_EQ(corr_id, 42);
}
