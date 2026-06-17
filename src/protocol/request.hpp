#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

struct RequestHeader {
    int16_t api_key;
    int16_t api_version;
    int32_t correlation_id;
};

struct ApiVersionsRequest {
    RequestHeader header;
};

struct DescribeTopicPartitionsRequest {
    RequestHeader header;
    std::vector<std::string> topic_names;
};

struct FetchPartitionRequest {
    int32_t partition_index = 0;
};

struct FetchTopicRequest {
    std::array<uint8_t, 16> topic_id{};
    std::vector<FetchPartitionRequest> partitions;
};

struct FetchRequest {
    RequestHeader header;
    std::vector<FetchTopicRequest> topics;
    int32_t max_bytes = 0;
};

struct ProducePartitionRequest {
    int32_t partition_index = 0;
    std::vector<uint8_t> records;
};

struct ProduceTopicRequest {
    std::string topic_name;
    std::vector<ProducePartitionRequest> partitions;
};

struct ProduceRequest {
    RequestHeader header;
    std::vector<ProduceTopicRequest> topics;
};

struct MetadataRequest {
    RequestHeader header;
    std::vector<std::string> topics;
    bool allow_auto_topic_creation = false;
};

struct ListOffsetsPartitionRequest {
    int32_t partition_index = 0;
    int64_t timestamp = 0;
};

struct ListOffsetsTopicRequest {
    std::string topic_name;
    std::vector<ListOffsetsPartitionRequest> partitions;
};

struct ListOffsetsRequest {
    RequestHeader header;
    int32_t replica_id = -1;
    int8_t isolation_level = 0;
    std::vector<ListOffsetsTopicRequest> topics;
};

using Request = std::variant<ApiVersionsRequest,
                             DescribeTopicPartitionsRequest,
                             FetchRequest,
                             ListOffsetsRequest,
                             MetadataRequest,
                             ProduceRequest>;
