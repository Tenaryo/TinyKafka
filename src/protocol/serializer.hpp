#pragma once

#include <cstdint>
#include <vector>

#include "protocol/response.hpp"

auto serialize(const Response& resp) -> std::vector<std::uint8_t>;
