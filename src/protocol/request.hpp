#pragma once

#include <cstdint>
#include <variant>

struct RequestHeader {
    int16_t api_key;
    int16_t api_version;
    int32_t correlation_id;
};

struct ApiVersionsRequest {
    RequestHeader header;
};

using Request = std::variant<ApiVersionsRequest>;
