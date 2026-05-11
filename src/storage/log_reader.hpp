#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

namespace storage {

auto read_topic_log(std::string_view root_path,
                    std::string_view topic_name,
                    int32_t partition) -> std::vector<uint8_t>;

} // namespace storage
