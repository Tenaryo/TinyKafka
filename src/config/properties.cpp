#include "config/properties.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <string_view>

#include "logging/logger.hpp"

namespace config {
namespace {

auto trim(std::string_view sv) -> std::string_view {
    const auto* start =
        std::ranges::find_if_not(sv, [](unsigned char c) { return std::isspace(c); });
    if (start == sv.end()) {
        return {};
    }
    sv.remove_prefix(static_cast<size_t>(start - sv.begin()));
    while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.back()))) {
        sv.remove_suffix(1);
    }
    return sv;
}

} // namespace

auto load_properties(const std::string& path) -> std::expected<Properties, std::error_code> {
    std::ifstream file(path);
    if (!file.is_open()) [[unlikely]] {
        logging::error("cannot open file: " + path);
        return std::unexpected(std::error_code(errno, std::generic_category()));
    }

    Properties props;
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        auto trimmed = trim(line);
        if (trimmed.empty() || trimmed.front() == '#') {
            continue;
        }
        auto eq_pos = trimmed.find('=');
        if (eq_pos == std::string_view::npos) {
            continue;
        }
        auto key = trim(trimmed.substr(0, eq_pos));
        if (key.empty()) {
            continue;
        }
        auto val = trim(trimmed.substr(eq_pos + 1));
        props.insert_or_assign(std::string{key}, std::string{val});
    }

    return props;
}

} // namespace config
