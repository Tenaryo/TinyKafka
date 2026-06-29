#pragma once

#include <array>

#include "protocol/response.hpp"

inline constexpr std::array<ApiVersionEntry, 13> kSupportedApis{{
    {.api_key = 0, .min_version = 0, .max_version = 11},
    {.api_key = 1, .min_version = 0, .max_version = 16},
    {.api_key = 2, .min_version = 0, .max_version = 8},
    {.api_key = 3, .min_version = 0, .max_version = 12},
    {.api_key = 8, .min_version = 0, .max_version = 8},
    {.api_key = 9, .min_version = 0, .max_version = 8},
    {.api_key = 10, .min_version = 0, .max_version = 4},
    {.api_key = 11, .min_version = 0, .max_version = 9},
    {.api_key = 12, .min_version = 0, .max_version = 4},
    {.api_key = 13, .min_version = 0, .max_version = 5},
    {.api_key = 14, .min_version = 0, .max_version = 5},
    {.api_key = 18, .min_version = 0, .max_version = 4},
    {.api_key = 75, .min_version = 0, .max_version = 0},
}};
