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

struct TopicMetadata {
    int16_t error_code;
    std::string topic_name;
    std::array<uint8_t, 16> topic_id{};
    bool is_internal;
    int32_t authorized_operations;
};

struct DescribeTopicPartitionsResponse {
    int32_t correlation_id;
    int32_t throttle_time_ms;
    std::vector<TopicMetadata> topics;
};

using Response = std::variant<ApiVersionsResponse, DescribeTopicPartitionsResponse>;
