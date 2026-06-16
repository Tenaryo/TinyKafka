#include "config/config.hpp"

#include "config/properties.hpp"

#include "logging/logger.hpp"
#include <charconv>
#include <string_view>

namespace config {

namespace {

template <typename T> auto parse_int(std::string_view sv) -> T {
    T result = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);
    if (ec == std::errc{} && ptr == sv.data() + sv.size()) {
        return result;
    }
    return 0;
}

auto parse_cli_arg(std::string_view arg) -> std::pair<std::string_view, std::string_view> {
    if (arg.starts_with("--")) {
        arg.remove_prefix(2);
    }
    auto eq_pos = arg.find('=');
    if (eq_pos == std::string_view::npos) {
        return {};
    }
    return {arg.substr(0, eq_pos), arg.substr(eq_pos + 1)};
}

void apply_config_key(Config& config, std::string_view key, std::string_view value) {
    if (key == "port") {
        auto parsed = parse_int<uint16_t>(value);
        if (parsed != 0) {
            config.port = parsed;
        } else {
            logging::warn("invalid port value: " + std::string(value) + ", using default " +
                          std::to_string(config.port));
        }
    } else if (key == "log.dirs") {
        config.log_root = value;
    } else if (key == "max.message.bytes") {
        auto parsed = parse_int<uint32_t>(value);
        if (parsed != 0) {
            config.max_message_bytes = parsed;
        } else {
            logging::warn("invalid max.message.bytes value: " + std::string(value) +
                          ", using default " + std::to_string(config.max_message_bytes));
        }
    }
}

} // namespace

auto Config::load(int argc, char** argv, std::string_view config_path) -> Config {
    Config config;

    if (!config_path.empty()) {
        auto props = load_properties(std::string{config_path});
        if (props) {
            for (const auto& [key, value] : *props) {
                apply_config_key(config, key, value);
            }
        }
    }

    for (int i = 1; i < argc; ++i) {
        auto [key, value] = parse_cli_arg(argv[i]);
        if (!key.empty()) {
            apply_config_key(config, key, value);
        }
    }

    return config;
}

} // namespace config
