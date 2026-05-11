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

struct FetchRequest {
    RequestHeader header;
    std::vector<std::array<uint8_t, 16>> topic_ids;
};

using Request = std::variant<ApiVersionsRequest, DescribeTopicPartitionsRequest, FetchRequest>;
