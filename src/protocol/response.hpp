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

struct MetadataBroker {
    int32_t node_id = 0;
    std::string host;
    int32_t port = 0;
    std::string rack;
};

struct MetadataTopicResponse {
    int16_t error_code = 0;
    std::string topic_name;
    std::array<uint8_t, 16> topic_id{};
    bool is_internal = false;
    std::vector<PartitionMetadata> partitions;
    int32_t topic_authorized_operations = 0;
};

struct MetadataResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    std::vector<MetadataBroker> brokers;
    std::string cluster_id;
    int32_t controller_id;
    std::vector<MetadataTopicResponse> topics;
    int32_t cluster_authorized_operations;
};

struct ListOffsetsPartitionResponse {
    int32_t partition_index = 0;
    int16_t error_code = 0;
    int64_t offset = -1;
    int64_t timestamp = -1;
};

struct ListOffsetsTopicResponse {
    std::string topic_name;
    std::vector<ListOffsetsPartitionResponse> partitions;
};

struct ListOffsetsResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    std::vector<ListOffsetsTopicResponse> topics;
};

struct FindCoordinatorResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    int16_t error_code;
    std::string error_message;
    int32_t node_id;
    std::string host;
    int32_t port;
};

struct OffsetCommitPartitionResponse {
    int32_t partition_index = 0;
    int16_t error_code = 0;
};

struct OffsetCommitTopicResponse {
    std::string topic_name;
    std::vector<OffsetCommitPartitionResponse> partitions;
};

struct OffsetCommitResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    std::vector<OffsetCommitTopicResponse> topics;
};

using Response = std::variant<ApiVersionsResponse,
                              DescribeTopicPartitionsResponse,
                              FetchResponse,
                              FindCoordinatorResponse,
                              ListOffsetsResponse,
                              MetadataResponse,
                              OffsetCommitResponse,
                              ProduceResponse>;
