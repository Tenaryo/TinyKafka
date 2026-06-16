#pragma once

#include <cstdint>
#include <string_view>

namespace logging {

void debug(std::string_view msg, uint32_t request_id = 0);
void info(std::string_view msg, uint32_t request_id = 0);
void warn(std::string_view msg, uint32_t request_id = 0);
void error(std::string_view msg, uint32_t request_id = 0);

} // namespace logging
