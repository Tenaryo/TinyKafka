#pragma once

#include <expected>
#include <string>
#include <system_error>
#include <unordered_map>

namespace config {

using Properties = std::unordered_map<std::string, std::string>;

auto load_properties(const std::string& path) -> std::expected<Properties, std::error_code>;

} // namespace config
