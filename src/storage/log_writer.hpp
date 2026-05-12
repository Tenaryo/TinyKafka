#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <system_error>

namespace storage {

auto write_topic_log(std::string_view root_path,
                     std::string_view topic_name,
                     int32_t partition,
                     std::span<const uint8_t> records) -> std::error_code;

} // namespace storage
