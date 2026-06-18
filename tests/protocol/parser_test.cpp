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
    buf.push_back(0x01);
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
    push_be32(0);          // partition_index
    push_be32(-1);         // current_leader_epoch
    push_be64(0);          // fetch_offset
    push_be32(-1);         // last_fetched_epoch
    push_be64(-1);         // log_start_offset
    push_be32(0x00100000); // max_bytes
    buf.push_back(0x00);   // part tag
    buf.push_back(0x00);   // topic tag
    buf.push_back(0x01);   // forgotten_topics
    buf.push_back(0x01);   // rack_id
    buf.push_back(0x00);   // body tag

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<FetchRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 1);
    EXPECT_EQ(req->header.api_version, 16);
    EXPECT_EQ(req->header.correlation_id, 42);
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_id, expected_uuid);
    ASSERT_EQ(req->topics[0].partitions.size(), 1u);
    EXPECT_EQ(req->topics[0].partitions[0].partition_index, 0);
}

TEST(ParserTest, ParsesFetchV16WithPartitionIndex) {
    constexpr std::array<uint8_t, 16> test_uuid = {
        0x00,
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

    push_be16(1);   // api_key = Fetch
    push_be16(16);  // api_version = 16
    push_be32(123); // correlation_id

    buf.push_back(0x00);
    buf.push_back(0x00);   // client_id = null
    buf.push_back(0x00);   // header TAG_BUFFER
    push_be32(100);        // max_wait_ms
    push_be32(1);          // min_bytes
    push_be32(0x00100000); // max_bytes
    buf.push_back(0x00);   // isolation_level
    push_be32(0);          // session_id
    push_be32(0);          // session_epoch

    buf.push_back(0x02);                                       // topics array: 1 element
    buf.insert(buf.end(), test_uuid.begin(), test_uuid.end()); // topic_id
    buf.push_back(0x02);                                       // partitions array: 1 element
    push_be32(3);                                              // partition_index = 3
    push_be32(-1);                                             // current_leader_epoch
    push_be64(10);                                             // fetch_offset
    push_be32(-1);                                             // last_fetched_epoch
    push_be64(-1);                                             // log_start_offset
    push_be32(0x00100000);                                     // max_bytes
    buf.push_back(0x00);                                       // partition TAG_BUFFER
    buf.push_back(0x00);                                       // topic TAG_BUFFER

    buf.push_back(0x01); // forgotten_topics = empty
    buf.push_back(0x01); // rack_id = empty
    buf.push_back(0x00); // body TAG_BUFFER

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<FetchRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.correlation_id, 123);
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_id, test_uuid);
    ASSERT_EQ(req->topics[0].partitions.size(), 1u);
    EXPECT_EQ(req->topics[0].partitions[0].partition_index, 3);
}

TEST(ParserTest, ParsesProduceV11Request) {
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

    push_be16(0);  // api_key = Produce
    push_be16(11); // api_version = 11
    push_be32(42); // correlation_id

    push_be16(0);        // client_id = null (INT16)
    buf.push_back(0x00); // header TAG_BUFFER

    buf.push_back(0x00); // transactional_id = null

    push_be16(1);    // acks = 1
    push_be32(5000); // timeout_ms = 5000

    buf.push_back(0x02); // topics array: 1 element

    buf.push_back(0x04); // topic name length = 3
    buf.push_back('f');
    buf.push_back('o');
    buf.push_back('o');

    buf.push_back(0x02); // partitions array: 1 element
    push_be32(0);        // partition_index = 0
    buf.push_back(0x01); // records: empty compact bytes
    buf.push_back(0x00); // partition _tagged_fields

    buf.push_back(0x00); // topic TAG_BUFFER

    buf.push_back(0x00); // body TAG_BUFFER

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<ProduceRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 0);
    EXPECT_EQ(req->header.api_version, 11);
    EXPECT_EQ(req->header.correlation_id, 42);
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_name, "foo");
    ASSERT_EQ(req->topics[0].partitions.size(), 1u);
    EXPECT_EQ(req->topics[0].partitions[0].partition_index, 0);
    EXPECT_TRUE(req->topics[0].partitions[0].records.empty());
}

TEST(ParserTest, ParsesProduceV11RequestWithRecords) {
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

    push_be16(0);
    push_be16(11);
    push_be32(42);

    push_be16(0);
    buf.push_back(0x00);

    buf.push_back(0x00);

    push_be16(1);
    push_be32(5000);

    buf.push_back(0x02);

    buf.push_back(0x04);
    buf.push_back('f');
    buf.push_back('o');
    buf.push_back('o');

    buf.push_back(0x02);
    push_be32(0);
    buf.push_back(0x04); // records varint = 4 → 3 bytes
    buf.push_back(0xAB);
    buf.push_back(0xCD);
    buf.push_back(0xEF);
    buf.push_back(0x00);

    buf.push_back(0x00);
    buf.push_back(0x00);

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());

    auto req = std::get_if<ProduceRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 0);
    EXPECT_EQ(req->header.api_version, 11);
    EXPECT_EQ(req->header.correlation_id, 42);
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_name, "foo");
    ASSERT_EQ(req->topics[0].partitions.size(), 1u);
    EXPECT_EQ(req->topics[0].partitions[0].partition_index, 0);
    ASSERT_EQ(req->topics[0].partitions[0].records.size(), 3u);
    EXPECT_EQ(req->topics[0].partitions[0].records[0], 0xAB);
    EXPECT_EQ(req->topics[0].partitions[0].records[1], 0xCD);
    EXPECT_EQ(req->topics[0].partitions[0].records[2], 0xEF);
}

TEST(ParserTest, RejectsUnknownApiKeyDefaultBranch) {
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
    push_be16(99);
    push_be16(0);
    push_be32(0);
    auto result = parse_request(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ParserTest, ParsesDescribeTopicPartitionsWithCursor) {
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
    push_be16(75);
    push_be16(0);
    push_be32(7);
    buf.push_back(0xFF);
    buf.push_back(0xFF);
    buf.push_back(0x00);
    buf.push_back(0x02);
    buf.push_back(0x04);
    buf.push_back('f');
    buf.push_back('o');
    buf.push_back('o');
    buf.push_back(0x00);
    push_be32(0x64);
    buf.push_back(0x00);
    buf.push_back(0x06);
    buf.push_back('c');
    buf.push_back('u');
    buf.push_back('r');
    buf.push_back('s');
    buf.push_back('o');
    buf.push_back('r');
    push_be32(5);
    buf.push_back(0x00);
    buf.push_back(0x00);
    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<DescribeTopicPartitionsRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->topic_names.size(), 1u);
    EXPECT_EQ(req->topic_names[0], "foo");
}

TEST(ParserTest, ParsesDescribeTopicPartitionsWithClientId) {
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
    push_be16(75);
    push_be16(0);
    push_be32(7);
    push_be16(5);
    buf.push_back('h');
    buf.push_back('e');
    buf.push_back('l');
    buf.push_back('l');
    buf.push_back('o');
    buf.push_back(0x00);
    buf.push_back(0x01);
    push_be32(0x64);
    buf.push_back(0xFF);
    buf.push_back(0x00);
    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<DescribeTopicPartitionsRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_TRUE(req->topic_names.empty());
}

TEST(ParserTest, ParsesFetchV16WithForgottenTopicsAndRack) {
    constexpr std::array<uint8_t, 16> test_uuid = {
        0x00,
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
    buf.insert(buf.end(), test_uuid.begin(), test_uuid.end());
    buf.push_back(0x02);
    push_be32(0);
    push_be32(-1);
    push_be64(0);
    push_be32(-1);
    push_be64(-1);
    push_be32(0x00100000);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x02);
    buf.insert(buf.end(), test_uuid.begin(), test_uuid.end());
    buf.push_back(0x02);
    push_be32(0);
    push_be32(0);
    push_be32(0);
    push_be32(0);
    buf.push_back(0x00);
    buf.push_back(0x04);
    buf.push_back('r');
    buf.push_back('a');
    buf.push_back('c');
    buf.push_back('k');
    buf.push_back(0x00);
    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<FetchRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.correlation_id, 42);
}

TEST(ParserTest, ParsesProduceV11WithClientIdAndTxId) {
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
    push_be16(0);
    push_be16(11);
    push_be32(42);
    push_be16(4);
    buf.push_back('t');
    buf.push_back('e');
    buf.push_back('s');
    buf.push_back('t');
    buf.push_back(0x00);
    buf.push_back(0x06);
    buf.push_back('t');
    buf.push_back('x');
    buf.push_back('n');
    buf.push_back('-');
    buf.push_back('1');
    push_be16(1);
    push_be32(5000);
    buf.push_back(0x02);
    buf.push_back(0x04);
    buf.push_back('f');
    buf.push_back('o');
    buf.push_back('o');
    buf.push_back(0x02);
    push_be32(0);
    buf.push_back(0x01);
    buf.push_back(0x00);
    buf.push_back(0x00);
    buf.push_back(0x00);
    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<ProduceRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.correlation_id, 42);
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_name, "foo");
}

TEST(ParserTest, ParsesMetadataRequestWithTopics) {
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

    push_be16(3);        // api_key = 3
    push_be16(0);        // api_version = 0
    push_be32(42);       // correlation_id = 42
    push_be16(-1);       // client_id = null
    buf.push_back(0x00); // header TAG_BUFFER
    push_be32(2);        // topics array length = 2
    push_be16(3);        // topic name length = 3
    buf.push_back('f');
    buf.push_back('o');
    buf.push_back('o');
    push_be16(3); // topic name length = 3
    buf.push_back('b');
    buf.push_back('a');
    buf.push_back('r');
    buf.push_back(0x01); // allow_auto_topic_creation = true

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<MetadataRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 3);
    EXPECT_EQ(req->header.api_version, 0);
    EXPECT_EQ(req->header.correlation_id, 42);
    ASSERT_EQ(req->topics.size(), 2u);
    EXPECT_EQ(req->topics[0], "foo");
    EXPECT_EQ(req->topics[1], "bar");
    EXPECT_TRUE(req->allow_auto_topic_creation);
}

TEST(ParserTest, ParsesMetadataRequestEmptyTopics) {
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

    push_be16(3);        // api_key = 3
    push_be16(0);        // api_version = 0
    push_be32(42);       // correlation_id = 42
    push_be16(-1);       // client_id = null
    buf.push_back(0x00); // header TAG_BUFFER
    push_be32(0);        // topics array empty
    buf.push_back(0x00); // allow_auto_topic_creation = false

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<MetadataRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 3);
    EXPECT_EQ(req->header.correlation_id, 42);
    EXPECT_TRUE(req->topics.empty());
    EXPECT_FALSE(req->allow_auto_topic_creation);
}

TEST(ParserTest, ParsesListOffsetsRequest) {
    std::vector<std::uint8_t> buf;
    auto pb16 = [&](int16_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb64 = [&](int64_t v) {
        for (int i = 7; i >= 0; --i)
            buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    };

    pb16(2);             // api_key = 2
    pb16(0);             // api_version = 0
    pb32(42);            // correlation_id
    pb16(-1);            // client_id = null
    buf.push_back(0x00); // header TAG_BUFFER
    pb32(-1);            // replica_id
    buf.push_back(0x00); // isolation_level
    buf.push_back(0x02); // topics array: 1 element (varint 2 = 1+1)
    buf.push_back(0x05); // topic name varint = 5 (4+1)
    buf.push_back('t');
    buf.push_back('e');
    buf.push_back('s');
    buf.push_back('t');
    buf.push_back(0x02); // partitions array: 1 element
    pb32(0);             // partition_index = 0
    pb64(-1);            // timestamp = -1 (latest)
    buf.push_back(0x00); // partition TAG_BUFFER
    buf.push_back(0x00); // topic TAG_BUFFER
    buf.push_back(0x00); // body TAG_BUFFER

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<ListOffsetsRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 2);
    EXPECT_EQ(req->header.correlation_id, 42);
    EXPECT_EQ(req->replica_id, -1);
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_name, "test");
    ASSERT_EQ(req->topics[0].partitions.size(), 1u);
    EXPECT_EQ(req->topics[0].partitions[0].partition_index, 0);
    EXPECT_EQ(req->topics[0].partitions[0].timestamp, -1);
}

TEST(ParserTest, ParsesFindCoordinatorRequest) {
    std::vector<std::uint8_t> buf;
    auto pb16 = [&](int16_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    pb16(10);            // api_key = 10
    pb16(0);             // api_version = 0
    pb32(42);            // correlation_id
    pb16(-1);            // client_id = null
    buf.push_back(0x00); // header TAG_BUFFER
    buf.push_back(0x07); // key varint = 7 (6+1)
    buf.push_back('m');
    buf.push_back('y');
    buf.push_back('-');
    buf.push_back('g');
    buf.push_back('r');
    buf.push_back('p');
    buf.push_back(0x00); // key_type = 0 (consumer)
    buf.push_back(0x00); // body TAG_BUFFER

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<FindCoordinatorRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 10);
    EXPECT_EQ(req->header.correlation_id, 42);
    EXPECT_EQ(req->coordinator_key, "my-grp");
    EXPECT_EQ(req->key_type, 0);
}

TEST(ParserTest, ParsesOffsetCommitRequest) {
    std::vector<std::uint8_t> buf;
    auto pb16 = [&](int16_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb64 = [&](int64_t v) {
        for (int i = 7; i >= 0; --i)
            buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    };

    pb16(8);
    pb16(0);
    pb32(42);
    pb16(-1);
    buf.push_back(0x00);
    buf.push_back(0x0B); // group_id varint = 11 (10+1)
    buf.push_back('m');
    buf.push_back('y');
    buf.push_back('-');
    buf.push_back('g');
    buf.push_back('r');
    buf.push_back('o');
    buf.push_back('u');
    buf.push_back('p');
    buf.push_back('1');
    buf.push_back('0');
    buf.push_back(0x01); // member_id = empty (varint 1)
    pb32(1);             // generation_id
    buf.push_back(0x02); // topics array: 1 element
    buf.push_back(0x05); // topic name varint
    buf.push_back('t');
    buf.push_back('e');
    buf.push_back('s');
    buf.push_back('t');
    buf.push_back(0x02); // partitions: 1 element
    pb32(0);             // partition_index
    pb64(100);           // committed_offset
    buf.push_back(0x00); // partition TAG_BUFFER
    buf.push_back(0x00); // topic TAG_BUFFER
    buf.push_back(0x00); // body TAG_BUFFER

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<OffsetCommitRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 8);
    EXPECT_EQ(req->group_id, "my-group10");
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_name, "test");
    ASSERT_EQ(req->topics[0].partitions.size(), 1u);
    EXPECT_EQ(req->topics[0].partitions[0].partition_index, 0);
    EXPECT_EQ(req->topics[0].partitions[0].committed_offset, 100);
}

TEST(ParserTest, ParsesOffsetFetchRequest) {
    std::vector<uint8_t> buf;
    auto pb16 = [&](int16_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    pb16(9);
    pb16(0);
    pb32(42);
    pb16(-1);
    buf.push_back(0x00);
    buf.push_back(0x03);
    buf.push_back('g');
    buf.push_back('1');
    buf.push_back(0x02);
    buf.push_back(0x02);
    buf.push_back('t');
    buf.push_back(0x03);
    pb32(0);
    pb32(1);
    buf.push_back(0x00);
    buf.push_back(0x00);

    auto result = parse_request(buf);
    ASSERT_TRUE(result.has_value());
    auto req = std::get_if<OffsetFetchRequest>(&*result);
    ASSERT_NE(req, nullptr);
    EXPECT_EQ(req->header.api_key, 9);
    EXPECT_EQ(req->group_id, "g1");
    ASSERT_EQ(req->topics.size(), 1u);
    EXPECT_EQ(req->topics[0].topic_name, "t");
    ASSERT_EQ(req->topics[0].partition_indexes.size(), 2u);
    EXPECT_EQ(req->topics[0].partition_indexes[0], 0);
    EXPECT_EQ(req->topics[0].partition_indexes[1], 1);
}
