#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <system_error>

#include "protocol/request.hpp"

auto parse_request(std::span<const std::uint8_t> buf) -> std::expected<Request, std::error_code>;
