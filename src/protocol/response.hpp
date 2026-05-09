#pragma once

#include <cstdint>
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

using Response = std::variant<ApiVersionsResponse>;
