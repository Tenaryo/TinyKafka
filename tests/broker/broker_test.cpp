#include <cstdint>
#include <gtest/gtest.h>

#include "broker/broker.hpp"
#include "cluster/metadata.hpp"

TEST(BrokerTest, HandlesValidVersion) {
    RequestHeader header{18, 0, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}).handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.correlation_id, 42);
    EXPECT_EQ(r.error_code, 0);
}

TEST(BrokerTest, HandlesUnsupportedVersion) {
    RequestHeader header{18, 26442, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}).handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.correlation_id, 42);
    EXPECT_EQ(r.error_code, 35);
}

TEST(BrokerTest, HandlesDescribeTopicPartitionsUnknownTopic) {
    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"foo"}};

    auto resp = Broker(ClusterMetadata{}).handle(req);
    auto r = std::get_if<DescribeTopicPartitionsResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    ASSERT_EQ(r->topics.size(), 1u);
    EXPECT_EQ(r->topics[0].error_code, 3);
    EXPECT_EQ(r->topics[0].topic_name, "foo");
    for (const auto& b : r->topics[0].topic_id) {
        EXPECT_EQ(b, 0x00);
    }
    EXPECT_EQ(r->topics[0].is_internal, false);
    EXPECT_EQ(r->topics[0].authorized_operations, 0);
    EXPECT_TRUE(r->topics[0].partitions.empty());
}

TEST(BrokerTest, ReturnsApiKeysForValidVersion) {
    RequestHeader header{18, 4, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}).handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.error_code, 0);
    ASSERT_FALSE(r.api_keys.empty());
    auto it18 = std::ranges::find_if(r.api_keys, [](const auto& e) {
        return e.api_key == 18 && e.min_version == 0 && e.max_version == 4;
    });
    EXPECT_NE(it18, r.api_keys.end());
    auto it75 = std::ranges::find_if(r.api_keys, [](const auto& e) {
        return e.api_key == 75 && e.min_version == 0 && e.max_version == 0;
    });
    EXPECT_NE(it75, r.api_keys.end());
    EXPECT_EQ(r.throttle_time_ms, 0);
}

TEST(BrokerTest, HandlesDescribeTopicPartitionsKnownTopic) {
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
    ClusterMetadata meta;
    meta.topics["foo"] = {.uuid = topic_uuid, .partitions = {0}};

    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"foo"}};

    auto resp = Broker(std::move(meta)).handle(req);
    auto r = std::get_if<DescribeTopicPartitionsResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    ASSERT_EQ(r->topics.size(), 1u);
    EXPECT_EQ(r->topics[0].error_code, 0);
    EXPECT_EQ(r->topics[0].topic_name, "foo");
    EXPECT_EQ(r->topics[0].topic_id, topic_uuid);
    EXPECT_EQ(r->topics[0].is_internal, false);
    ASSERT_EQ(r->topics[0].partitions.size(), 1u);
    EXPECT_EQ(r->topics[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->topics[0].partitions[0].error_code, 0);
}

TEST(BrokerTest, HandlesDescribeTopicPartitionsTopicNotFoundInMetadata) {
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
    ClusterMetadata meta;
    meta.topics["foo"] = {.uuid = topic_uuid, .partitions = {0}};

    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"nonexistent"}};

    auto resp = Broker(std::move(meta)).handle(req);
    auto r = std::get_if<DescribeTopicPartitionsResponse>(&resp);
    ASSERT_NE(r, nullptr);
    ASSERT_EQ(r->topics.size(), 1u);
    EXPECT_EQ(r->topics[0].error_code, 3);
    EXPECT_EQ(r->topics[0].topic_name, "nonexistent");
    for (const auto& b : r->topics[0].topic_id) {
        EXPECT_EQ(b, 0x00);
    }
    EXPECT_TRUE(r->topics[0].partitions.empty());
}

TEST(BrokerTest, SortsDescribeTopicPartitionsMultiTopicAlphabetically) {
    ClusterMetadata meta;
    meta.topics["apple"] = {.uuid = {}, .partitions = {0}};
    meta.topics["zebra"] = {.uuid = {}, .partitions = {1}};

    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"zebra", "apple"}};

    auto resp = Broker(std::move(meta)).handle(req);
    auto r = std::get_if<DescribeTopicPartitionsResponse>(&resp);
    ASSERT_NE(r, nullptr);
    ASSERT_EQ(r->topics.size(), 2u);
    EXPECT_EQ(r->topics[0].topic_name, "apple");
    EXPECT_EQ(r->topics[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->topics[1].topic_name, "zebra");
    EXPECT_EQ(r->topics[1].partitions[0].partition_index, 1);
}
