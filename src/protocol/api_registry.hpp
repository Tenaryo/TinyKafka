#pragma once

#include <array>

#include "protocol/response.hpp"

inline constexpr std::array<ApiVersionEntry, 3> kSupportedApis{{
    {.api_key = 1, .min_version = 0, .max_version = 16},
    {.api_key = 18, .min_version = 0, .max_version = 4},
    {.api_key = 75, .min_version = 0, .max_version = 0},
}};
