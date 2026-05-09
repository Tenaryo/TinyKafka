#pragma once

#include <array>

#include "protocol/response.hpp"

inline constexpr std::array<ApiVersionEntry, 1> kSupportedApis{{
    {.api_key = 18, .min_version = 0, .max_version = 4},
}};
