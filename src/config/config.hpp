#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace config {

struct Config {
    uint16_t port = 9092;
    std::string log_root = "/tmp/kraft-combined-logs";
    uint32_t max_message_bytes = 1'048'576;
    size_t max_write_buffer_bytes = 4'194'304;
    size_t segment_bytes = 1'073'741'824;

    [[nodiscard]] static auto
    load(int argc, char** argv, std::string_view config_path = "config.properties") -> Config;
};

} // namespace config
