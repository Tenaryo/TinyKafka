#pragma once

#include <cstdint>
#include <variant>

struct ApiVersionsResponse {
    int32_t correlation_id;
    int16_t error_code;
};

using Response = std::variant<ApiVersionsResponse>;
