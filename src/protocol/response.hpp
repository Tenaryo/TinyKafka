#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

struct ApiVersionEntry {
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
};

struct ApiVersionsResponse {
    int32_t correlation_id;
    int16_t error_code;
    std::vector<ApiVersionEntry> api_keys;
    int32_t throttle_time_ms;
};

struct PartitionMetadata {
    int16_t error_code = 0;
    int32_t partition_index = 0;
    int32_t leader_id = 0;
    int32_t leader_epoch = 0;
    std::vector<int32_t> replica_nodes;
    std::vector<int32_t> isr_nodes;
    std::vector<int32_t> eligible_leader_replicas;
    std::vector<int32_t> last_known_elr;
    std::vector<int32_t> offline_replicas;
};

struct TopicMetadata {
    int16_t error_code;
    std::string topic_name;
    std::array<uint8_t, 16> topic_id{};
    bool is_internal;
    int32_t authorized_operations;
    std::vector<PartitionMetadata> partitions;
};

struct DescribeTopicPartitionsResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    std::vector<TopicMetadata> topics;
};

struct FetchPartitionResponse {
    int32_t partition_index = 0;
    int16_t error_code = 0;
    std::vector<uint8_t> records;
};

struct FetchTopicResponse {
    std::array<uint8_t, 16> topic_id{};
    std::vector<FetchPartitionResponse> partitions;
};

struct FetchResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    int16_t error_code;
    int32_t session_id;
    std::vector<FetchTopicResponse> responses;
};

struct ProducePartitionResponse {
    int32_t partition_index = 0;
    int16_t error_code = 0;
    int64_t base_offset = -1;
    int64_t log_append_time_ms = -1;
    int64_t log_start_offset = -1;
};

struct ProduceTopicResponse {
    std::string topic_name;
    std::vector<ProducePartitionResponse> partitions;
};

struct ProduceResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    std::vector<ProduceTopicResponse> responses;
};

using Response = std::
    variant<ApiVersionsResponse, DescribeTopicPartitionsResponse, FetchResponse, ProduceResponse>;
