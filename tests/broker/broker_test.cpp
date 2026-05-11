#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include "broker/broker.hpp"
#include "cluster/metadata.hpp"

using TopicId = std::array<uint8_t, 16>;

namespace {

auto make_meta_with_topic(std::string name,
                          TopicId uuid,
                          std::vector<int32_t> partitions) -> ClusterMetadata {
    ClusterMetadata meta;
    meta.topics.push_back(
        {.name = std::move(name), .uuid = uuid, .partitions = std::move(partitions)});
    size_t idx = meta.topics.size() - 1;
    meta.name_to_topic[meta.topics[idx].name] = idx;
    meta.uuid_to_topic[uuid] = idx;
    return meta;
}

auto make_tmp_log_dir() -> std::string {
    auto path =
        std::filesystem::temp_directory_path() / ("tinytk_test_" + std::to_string(std::rand()));
    std::filesystem::create_directories(path);
    return path.string();
}

} // namespace

TEST(BrokerTest, HandlesValidVersion) {
    RequestHeader header{18, 0, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.correlation_id, 42);
    EXPECT_EQ(r.error_code, 0);
}

TEST(BrokerTest, HandlesUnsupportedVersion) {
    RequestHeader header{18, 26442, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.correlation_id, 42);
    EXPECT_EQ(r.error_code, 35);
}

TEST(BrokerTest, HandlesDescribeTopicPartitionsUnknownTopic) {
    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"foo"}};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
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

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
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
    constexpr TopicId topic_uuid = {
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
    auto meta = make_meta_with_topic("foo", topic_uuid, {0});

    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"foo"}};

    auto resp = Broker(std::move(meta), "").handle(req);
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
    constexpr TopicId topic_uuid = {
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
    auto meta = make_meta_with_topic("foo", topic_uuid, {0});

    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"nonexistent"}};

    auto resp = Broker(std::move(meta), "").handle(req);
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
    constexpr TopicId uuid_a = {
        0x01,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
    };
    constexpr TopicId uuid_z = {
        0x02,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
    };
    ClusterMetadata meta;
    meta.topics.push_back({.name = "apple", .uuid = uuid_a, .partitions = {0}});
    meta.name_to_topic["apple"] = 0;
    meta.uuid_to_topic[uuid_a] = 0;
    meta.topics.push_back({.name = "zebra", .uuid = uuid_z, .partitions = {1}});
    meta.name_to_topic["zebra"] = 1;
    meta.uuid_to_topic[uuid_z] = 1;

    RequestHeader header{75, 0, 42};
    DescribeTopicPartitionsRequest req{header, {"zebra", "apple"}};

    auto resp = Broker(std::move(meta), "").handle(req);
    auto r = std::get_if<DescribeTopicPartitionsResponse>(&resp);
    ASSERT_NE(r, nullptr);
    ASSERT_EQ(r->topics.size(), 2u);
    EXPECT_EQ(r->topics[0].topic_name, "apple");
    EXPECT_EQ(r->topics[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->topics[1].topic_name, "zebra");
    EXPECT_EQ(r->topics[1].partitions[0].partition_index, 1);
}

TEST(BrokerTest, ReturnsFetchApiEntryWithMaxVersion16) {
    RequestHeader header{18, 4, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.error_code, 0);
    ASSERT_FALSE(r.api_keys.empty());
    auto it = std::ranges::find_if(r.api_keys, [](const auto& e) {
        return e.api_key == 1 && e.min_version == 0 && e.max_version == 16;
    });
    EXPECT_NE(it, r.api_keys.end()) << "Fetch API (key=1) entry not found";
}

TEST(BrokerTest, ReturnsProduceApiEntryWithMaxVersion11) {
    RequestHeader header{18, 4, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.error_code, 0);
    ASSERT_FALSE(r.api_keys.empty());
    auto it = std::ranges::find_if(r.api_keys, [](const auto& e) {
        return e.api_key == 0 && e.min_version == 0 && e.max_version >= 11;
    });
    EXPECT_NE(it, r.api_keys.end()) << "Produce API (key=0) entry not found";
}

TEST(BrokerTest, HandlesFetchRequestEmptyTopics) {
    RequestHeader header{1, 16, 42};
    FetchRequest req{header, {}, 0};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto r = std::get_if<FetchResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    EXPECT_EQ(r->error_code, 0);
    EXPECT_EQ(r->session_id, 0);
    EXPECT_TRUE(r->responses.empty());
}

TEST(BrokerTest, HandlesFetchRequestUnknownTopic) {
    constexpr TopicId topic_uuid = {
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
    RequestHeader header{1, 16, 42};
    FetchRequest req{header, {{.topic_id = topic_uuid, .partitions = {{.partition_index = 0}}}}, 0};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto r = std::get_if<FetchResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    EXPECT_EQ(r->error_code, 0);
    EXPECT_EQ(r->session_id, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_id, topic_uuid);
    ASSERT_EQ(r->responses[0].partitions.size(), 1u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 100);
}

TEST(BrokerTest, HandlesFetchRequestKnownTopicNoMessages) {
    constexpr TopicId topic_uuid = {
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
    auto meta = make_meta_with_topic("foo", topic_uuid, {0, 1});

    RequestHeader header{1, 16, 42};
    FetchRequest req{
        header,
        {{.topic_id = topic_uuid, .partitions = {{.partition_index = 0}, {.partition_index = 1}}}},
        0};

    auto resp = Broker(std::move(meta), "").handle(req);
    auto r = std::get_if<FetchResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    EXPECT_EQ(r->error_code, 0);
    EXPECT_EQ(r->session_id, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_id, topic_uuid);
    ASSERT_EQ(r->responses[0].partitions.size(), 2u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
    EXPECT_TRUE(r->responses[0].partitions[0].records.empty());
    EXPECT_EQ(r->responses[0].partitions[1].partition_index, 1);
    EXPECT_EQ(r->responses[0].partitions[1].error_code, 0);
}

TEST(BrokerTest, HandlesFetchRequestKnownTopicNoPartitions) {
    constexpr TopicId topic_uuid = {
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
    auto meta = make_meta_with_topic("foo", topic_uuid, {});

    RequestHeader header{1, 16, 42};
    FetchRequest req{header, {{.topic_id = topic_uuid, .partitions = {}}}, 0};

    auto resp = Broker(std::move(meta), "").handle(req);
    auto r = std::get_if<FetchResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    EXPECT_EQ(r->error_code, 0);
    EXPECT_EQ(r->session_id, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_id, topic_uuid);
    EXPECT_TRUE(r->responses[0].partitions.empty());
}

TEST(BrokerTest, HandlesFetchRequestReadsRecordBatchFromDisk) {
    constexpr TopicId topic_uuid = {
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
    std::vector<uint8_t> record_batch = {0x00, 0x01, 0x02, 0x03, 0x04};

    auto tmp_dir = make_tmp_log_dir();
    auto topic_dir = tmp_dir + "/bar-0";
    std::filesystem::create_directories(topic_dir);

    {
        std::ofstream file(topic_dir + "/00000000000000000000.log", std::ios::binary);
        file.write(reinterpret_cast<const char*>(record_batch.data()),
                   static_cast<std::streamsize>(record_batch.size()));
    }

    auto meta = make_meta_with_topic("bar", topic_uuid, {0});

    RequestHeader header{1, 16, 42};
    FetchRequest req{header, {{.topic_id = topic_uuid, .partitions = {{.partition_index = 0}}}}, 0};

    auto resp = Broker(std::move(meta), tmp_dir).handle(req);
    auto r = std::get_if<FetchResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->error_code, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_id, topic_uuid);
    ASSERT_EQ(r->responses[0].partitions.size(), 1u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
    EXPECT_EQ(r->responses[0].partitions[0].records, record_batch);

    std::filesystem::remove_all(tmp_dir);
}

TEST(BrokerTest, HandlesProduceRequestUnknownTopicOrPartition) {
    RequestHeader header{0, 11, 42};
    ProduceRequest req{header, {{.topic_name = "foo", .partitions = {{.partition_index = 0}}}}};

    auto resp = Broker(ClusterMetadata{}, "").handle(req);
    auto r = std::get_if<ProduceResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_name, "foo");
    ASSERT_EQ(r->responses[0].partitions.size(), 1u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 3);
    EXPECT_EQ(r->responses[0].partitions[0].base_offset, -1);
    EXPECT_EQ(r->responses[0].partitions[0].log_append_time_ms, -1);
    EXPECT_EQ(r->responses[0].partitions[0].log_start_offset, -1);
}
