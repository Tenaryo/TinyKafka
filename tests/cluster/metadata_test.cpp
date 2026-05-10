#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "cluster/metadata.hpp"
#include "util/varint.hpp"

namespace {

auto make_topic_record_value(std::string_view name,
                             const std::array<uint8_t, 16>& uuid) -> std::vector<uint8_t> {
    std::vector<uint8_t> v;
    v.push_back(0x00);
    v.push_back(0x02);
    v.push_back(0x00);
    v.push_back(0x00);
    v.push_back(static_cast<uint8_t>(name.size() + 1));
    for (char c : name) {
        v.push_back(static_cast<uint8_t>(c));
    }
    for (auto b : uuid) {
        v.push_back(b);
    }
    v.push_back(0x00);
    return v;
}

auto make_partition_record_value(int32_t partition_id,
                                 const std::array<uint8_t, 16>& uuid) -> std::vector<uint8_t> {
    std::vector<uint8_t> v;
    auto push_be32 = [&](int32_t val) {
        v.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
        v.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
        v.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
        v.push_back(static_cast<uint8_t>(val & 0xFF));
    };
    v.push_back(0x00);
    v.push_back(0x03);
    v.push_back(0x00);
    v.push_back(0x00);
    push_be32(partition_id);
    for (auto b : uuid) {
        v.push_back(b);
    }
    v.push_back(0x01);
    v.push_back(0x01);
    v.push_back(0x01);
    v.push_back(0x01);
    push_be32(0);
    push_be32(0);
    push_be32(0);
    v.push_back(0x01);
    v.push_back(0x00);
    return v;
}

auto make_record(const std::vector<uint8_t>& value) -> std::vector<uint8_t> {
    std::vector<uint8_t> record;
    auto push_signed_varint = [&](int32_t val) {
        uint32_t encoded = zigzag_encode(val);
        while (encoded > 0x7F) {
            record.push_back(static_cast<uint8_t>((encoded & 0x7F) | 0x80));
            encoded >>= 7;
        }
        record.push_back(static_cast<uint8_t>(encoded & 0x7F));
    };
    uint32_t body_size = 1 + 1 + 1 + 1 + 0 + 1 + static_cast<uint32_t>(value.size()) + 1;
    push_signed_varint(static_cast<int32_t>(body_size));
    record.push_back(0x00);
    push_signed_varint(0);
    push_signed_varint(0);
    push_signed_varint(-1);
    push_signed_varint(static_cast<int32_t>(value.size()));
    record.insert(record.end(), value.begin(), value.end());
    push_signed_varint(0);
    return record;
}

auto build_record_batch(const std::vector<std::vector<uint8_t>>& record_values)
    -> std::vector<uint8_t> {
    std::vector<uint8_t> buf;
    auto push_be32 = [&](int32_t val) {
        buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(val & 0xFF));
    };
    auto push_be64 = [&](int64_t val) {
        for (int i = 7; i >= 0; --i) {
            buf.push_back(static_cast<uint8_t>((val >> (i * 8)) & 0xFF));
        }
    };
    auto push_be16 = [&](int16_t val) {
        buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(val & 0xFF));
    };
    push_be64(0);
    size_t batch_len_pos = buf.size();
    push_be32(0);
    push_be32(0);
    buf.push_back(0x02);
    push_be32(0);
    push_be16(0);
    push_be32(static_cast<int32_t>(record_values.size()) - 1);
    push_be64(0);
    push_be64(0);
    push_be64(0);
    push_be16(0);
    push_be32(0);
    push_be32(static_cast<int32_t>(record_values.size()));

    std::vector<uint8_t> records;
    for (const auto& value : record_values) {
        auto rec = make_record(value);
        records.insert(records.end(), rec.begin(), rec.end());
    }
    buf.insert(buf.end(), records.begin(), records.end());

    int32_t batch_len = static_cast<int32_t>(buf.size() - batch_len_pos - 4);
    buf[batch_len_pos] = static_cast<uint8_t>((batch_len >> 24) & 0xFF);
    buf[batch_len_pos + 1] = static_cast<uint8_t>((batch_len >> 16) & 0xFF);
    buf[batch_len_pos + 2] = static_cast<uint8_t>((batch_len >> 8) & 0xFF);
    buf[batch_len_pos + 3] = static_cast<uint8_t>(batch_len & 0xFF);

    return buf;
}

} // namespace

TEST(MetadataTest, ParseTopicAndPartitionRecords) {
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

    auto topic_val = make_topic_record_value("foo", expected_uuid);
    auto partition_val = make_partition_record_value(0, expected_uuid);

    auto batch = build_record_batch({topic_val, partition_val});

    auto result = parse_cluster_metadata(batch);
    ASSERT_TRUE(result.has_value()) << "parse_cluster_metadata failed";

    const auto& meta = *result;
    ASSERT_EQ(meta.topics.size(), 1u);
    auto it = meta.topics.find("foo");
    ASSERT_NE(it, meta.topics.end());
    EXPECT_EQ(it->second.uuid, expected_uuid);
    ASSERT_EQ(it->second.partitions.size(), 1u);
    EXPECT_EQ(it->second.partitions[0], 0);
}

TEST(MetadataTest, UnknownRecordTypeSkipped) {
    auto unknown_val = std::vector<uint8_t>{
        0x00,
        0x63,
        0x00,
        0x00, // api_key = 99 (unknown), version = 0
        0x01,
        0x00 // dummy data + tagged
    };
    constexpr std::array<uint8_t, 16> uuid = {
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
    auto topic_val = make_topic_record_value("foo", uuid);

    auto batch = build_record_batch({unknown_val, topic_val});

    auto result = parse_cluster_metadata(batch);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->topics.size(), 1u);
    EXPECT_NE(result->topics.find("foo"), result->topics.end());
}

TEST(MetadataTest, EmptyInputReturnsEmptyMetadata) {
    std::vector<uint8_t> empty;
    auto result = parse_cluster_metadata(empty);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->topics.empty());
}
