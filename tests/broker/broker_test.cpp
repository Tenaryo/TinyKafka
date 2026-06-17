#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <latch>
#include <thread>
#include <unistd.h>

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
    static std::atomic<int> counter{0};
    auto path = std::filesystem::temp_directory_path() /
                ("tinytk_test_" + std::to_string(getpid()) + "_" + std::to_string(++counter));
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
    ProduceRequest req{
        header, {{.topic_name = "foo", .partitions = {{.partition_index = 0, .records = {}}}}}};

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

TEST(BrokerTest, HandlesProduceRequestValidTopicAndPartition) {
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
    auto meta = make_meta_with_topic("orders", topic_uuid, {0, 1});

    RequestHeader header{0, 11, 999};
    ProduceRequest req{
        header,
        {{.topic_name = "orders",
          .partitions = {{.partition_index = 0, .records = {}},
                         {.partition_index = 1, .records = {}}}}},
    };

    auto resp = Broker(std::move(meta), "").handle(req);
    auto r = std::get_if<ProduceResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 999);
    EXPECT_EQ(r->throttle_time_ms, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_name, "orders");
    ASSERT_EQ(r->responses[0].partitions.size(), 2u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
    EXPECT_EQ(r->responses[0].partitions[0].base_offset, 0);
    EXPECT_EQ(r->responses[0].partitions[0].log_append_time_ms, -1);
    EXPECT_EQ(r->responses[0].partitions[0].log_start_offset, 0);
    EXPECT_EQ(r->responses[0].partitions[1].partition_index, 1);
    EXPECT_EQ(r->responses[0].partitions[1].error_code, 0);
    EXPECT_EQ(r->responses[0].partitions[1].base_offset, 0);
    EXPECT_EQ(r->responses[0].partitions[1].log_append_time_ms, -1);
    EXPECT_EQ(r->responses[0].partitions[1].log_start_offset, 0);
}

TEST(BrokerTest, HandlesProduceRequestKnownTopicUnknownPartition) {
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
    auto meta = make_meta_with_topic("orders", topic_uuid, {0});

    RequestHeader header{0, 11, 42};
    ProduceRequest req{header,
                       {{.topic_name = "orders",
                         .partitions = {{.partition_index = 0, .records = {}},
                                        {.partition_index = 99, .records = {}}}}}};

    auto resp = Broker(std::move(meta), "").handle(req);
    auto r = std::get_if<ProduceResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 42);
    EXPECT_EQ(r->throttle_time_ms, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_name, "orders");
    ASSERT_EQ(r->responses[0].partitions.size(), 2u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
    EXPECT_EQ(r->responses[0].partitions[0].base_offset, 0);
    EXPECT_EQ(r->responses[0].partitions[1].partition_index, 99);
    EXPECT_EQ(r->responses[0].partitions[1].error_code, 3);
    EXPECT_EQ(r->responses[0].partitions[1].base_offset, -1);
}

TEST(BrokerTest, HandlesProduceRequestWritesToDisk) {
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
    std::vector<uint8_t> record_batch = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05};

    auto tmp_dir = make_tmp_log_dir();
    auto meta = make_meta_with_topic("orders", topic_uuid, {0});

    RequestHeader header{0, 11, 999};
    ProduceRequest req{header,
                       {{.topic_name = "orders",
                         .partitions = {{.partition_index = 0, .records = record_batch}}}}};

    auto resp = Broker(std::move(meta), tmp_dir).handle(req);
    auto r = std::get_if<ProduceResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 999);
    EXPECT_EQ(r->throttle_time_ms, 0);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_name, "orders");
    ASSERT_EQ(r->responses[0].partitions.size(), 1u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
    EXPECT_EQ(r->responses[0].partitions[0].base_offset, 0);
    EXPECT_EQ(r->responses[0].partitions[0].log_append_time_ms, -1);
    EXPECT_EQ(r->responses[0].partitions[0].log_start_offset, 0);

    auto log_path = tmp_dir + "/orders-0/00000000000000000000.log";
    EXPECT_TRUE(std::filesystem::exists(log_path));
    std::ifstream f(log_path, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(f.is_open());
    auto sz = f.tellg();
    ASSERT_EQ(static_cast<size_t>(sz), record_batch.size());
    f.seekg(0);
    std::vector<uint8_t> readback(static_cast<size_t>(sz));
    f.read(reinterpret_cast<char*>(readback.data()), sz);
    EXPECT_EQ(readback, record_batch);

    std::filesystem::remove_all(tmp_dir);
}

TEST(BrokerTest, HandlesProduceRequestWriteFailure) {
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
    std::vector<uint8_t> record_batch = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05};

    auto tmp_dir = make_tmp_log_dir();
    std::filesystem::permissions(
        tmp_dir, std::filesystem::perms::owner_exec, std::filesystem::perm_options::replace);

    auto meta = make_meta_with_topic("orders", topic_uuid, {0});

    RequestHeader header{0, 11, 999};
    ProduceRequest req{header,
                       {{.topic_name = "orders",
                         .partitions = {{.partition_index = 0, .records = record_batch}}}}};

    auto resp = Broker(std::move(meta), tmp_dir).handle(req);
    auto r = std::get_if<ProduceResponse>(&resp);
    ASSERT_NE(r, nullptr);
    EXPECT_EQ(r->correlation_id, 999);
    ASSERT_EQ(r->responses.size(), 1u);
    EXPECT_EQ(r->responses[0].topic_name, "orders");
    ASSERT_EQ(r->responses[0].partitions.size(), 1u);
    EXPECT_EQ(r->responses[0].partitions[0].partition_index, 0);
    EXPECT_EQ(r->responses[0].partitions[0].error_code, 56);
    EXPECT_EQ(r->responses[0].partitions[0].base_offset, -1);
    EXPECT_EQ(r->responses[0].partitions[0].log_append_time_ms, -1);
    EXPECT_EQ(r->responses[0].partitions[0].log_start_offset, -1);

    std::filesystem::permissions(
        tmp_dir, std::filesystem::perms::owner_all, std::filesystem::perm_options::replace);
    std::filesystem::remove_all(tmp_dir);
}

TEST(BrokerTest, ProducesSequentialOffsets) {
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

    auto tmp_dir = make_tmp_log_dir();
    auto meta = make_meta_with_topic("orders", topic_uuid, {0});
    Broker broker(std::move(meta), tmp_dir);

    for (int batch = 0; batch < 3; ++batch) {
        std::vector<uint8_t> records = {static_cast<uint8_t>(batch), 0x01, 0x02};
        RequestHeader header{0, 11, 100 + batch};
        ProduceRequest req{
            header,
            {{.topic_name = "orders", .partitions = {{.partition_index = 0, .records = records}}}}};

        auto resp = broker.handle(req);
        auto r = std::get_if<ProduceResponse>(&resp);
        ASSERT_NE(r, nullptr);
        ASSERT_EQ(r->responses.size(), 1u);
        ASSERT_EQ(r->responses[0].partitions.size(), 1u);
        EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
        EXPECT_EQ(r->responses[0].partitions[0].base_offset, batch);
        EXPECT_EQ(r->responses[0].partitions[0].log_start_offset, 0);
    }

    auto log_path = tmp_dir + "/orders-0/00000000000000000000.log";
    EXPECT_TRUE(std::filesystem::exists(log_path));
    std::ifstream f(log_path, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(f.is_open());
    auto sz = f.tellg();
    ASSERT_EQ(static_cast<size_t>(sz), 9u);

    std::filesystem::remove_all(tmp_dir);
}

TEST(BrokerTest, ProducesConcurrentSamePartition) {
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

    auto tmp_dir = make_tmp_log_dir();
    auto meta = make_meta_with_topic("orders", topic_uuid, {0});
    Broker broker(std::move(meta), tmp_dir);

    constexpr int kThreads = 8;
    std::vector<int64_t> offsets(kThreads, -1);
    std::latch start(1);

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(kThreads));
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            start.wait();
            std::vector<uint8_t> records = {static_cast<uint8_t>(t), 0x01, 0x02, 0x03};
            RequestHeader header{0, 11, 1000 + t};
            ProduceRequest req{header,
                               {{.topic_name = "orders",
                                 .partitions = {{.partition_index = 0, .records = records}}}}};
            auto resp = broker.handle(req);
            auto r = std::get_if<ProduceResponse>(&resp);
            ASSERT_NE(r, nullptr);
            ASSERT_EQ(r->responses.size(), 1u);
            ASSERT_EQ(r->responses[0].partitions.size(), 1u);
            EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
            offsets[static_cast<size_t>(t)] = r->responses[0].partitions[0].base_offset;
        });
    }

    start.count_down();

    for (auto& t : threads) {
        t.join();
    }

    std::ranges::sort(offsets);
    for (int i = 0; i < kThreads; ++i) {
        EXPECT_EQ(offsets[static_cast<size_t>(i)], i);
    }

    auto log_path = tmp_dir + "/orders-0/00000000000000000000.log";
    EXPECT_TRUE(std::filesystem::exists(log_path));
    std::ifstream f(log_path, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(f.is_open());
    auto sz = f.tellg();
    ASSERT_EQ(static_cast<size_t>(sz), static_cast<size_t>(kThreads) * 4u);

    std::filesystem::remove_all(tmp_dir);
}

TEST(BrokerTest, ProducesConcurrentDifferentPartitions) {
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

    auto tmp_dir = make_tmp_log_dir();
    auto meta = make_meta_with_topic("orders", topic_uuid, {0, 1, 2});
    Broker broker(std::move(meta), tmp_dir);

    constexpr int kPartitions = 3;
    std::latch start(1);

    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(kPartitions));
    for (int p = 0; p < kPartitions; ++p) {
        threads.emplace_back([&, p] {
            start.wait();
            for (int batch = 0; batch < 5; ++batch) {
                std::vector<uint8_t> records = {static_cast<uint8_t>(batch), 0xFF};
                RequestHeader header{0, 11, 2000 + p * 100 + batch};
                ProduceRequest req{header,
                                   {{.topic_name = "orders",
                                     .partitions = {{.partition_index = p, .records = records}}}}};
                auto resp = broker.handle(req);
                auto r = std::get_if<ProduceResponse>(&resp);
                ASSERT_NE(r, nullptr);
                ASSERT_EQ(r->responses.size(), 1u);
                ASSERT_EQ(r->responses[0].partitions.size(), 1u);
                EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
                EXPECT_EQ(r->responses[0].partitions[0].base_offset, batch);
            }
        });
    }

    start.count_down();

    for (auto& t : threads) {
        t.join();
    }

    for (int p = 0; p < kPartitions; ++p) {
        auto log_path = tmp_dir + "/orders-" + std::to_string(p) + "/00000000000000000000.log";
        EXPECT_TRUE(std::filesystem::exists(log_path)) << "partition " << p;
        std::ifstream f(log_path, std::ios::binary | std::ios::ate);
        ASSERT_TRUE(f.is_open());
        auto sz = f.tellg();
        ASSERT_EQ(static_cast<size_t>(sz), 10u);
    }

    std::filesystem::remove_all(tmp_dir);
}

TEST(BrokerTest, FetchReturnsProducedRecords) {
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

    auto tmp_dir = make_tmp_log_dir();
    auto meta = make_meta_with_topic("orders", topic_uuid, {0, 1});
    Broker broker(std::move(meta), tmp_dir);

    std::vector<uint8_t> batch0 = {0x00, 0x01, 0x02};
    std::vector<uint8_t> batch1 = {0x03, 0x04, 0x05, 0x06};

    {
        RequestHeader header{0, 11, 100};
        ProduceRequest req{header,
                           {{.topic_name = "orders",
                             .partitions = {{.partition_index = 0, .records = batch0},
                                            {.partition_index = 1, .records = batch1}}}}};
        auto resp = broker.handle(req);
        auto r = std::get_if<ProduceResponse>(&resp);
        ASSERT_NE(r, nullptr);
        ASSERT_EQ(r->responses[0].partitions.size(), 2u);
        EXPECT_EQ(r->responses[0].partitions[0].base_offset, 0);
        EXPECT_EQ(r->responses[0].partitions[1].base_offset, 0);
    }

    {
        RequestHeader header{1, 16, 200};
        FetchRequest req{header,
                         {{.topic_id = topic_uuid,
                           .partitions = {{.partition_index = 0}, {.partition_index = 1}}}},
                         0};
        auto resp = broker.handle(req);
        auto r = std::get_if<FetchResponse>(&resp);
        ASSERT_NE(r, nullptr);
        ASSERT_EQ(r->responses.size(), 1u);
        ASSERT_EQ(r->responses[0].partitions.size(), 2u);
        EXPECT_EQ(r->responses[0].partitions[0].error_code, 0);
        EXPECT_EQ(r->responses[0].partitions[0].records, batch0);
        EXPECT_EQ(r->responses[0].partitions[1].error_code, 0);
        EXPECT_EQ(r->responses[0].partitions[1].records, batch1);
    }

    std::filesystem::remove_all(tmp_dir);
}
