#include "config/config.hpp"

#include "config/properties.hpp"

#include <charconv>
#include <string_view>

namespace config {

static auto parse_uint16(std::string_view sv) -> uint16_t {
    uint16_t result = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);
    if (ec == std::errc{} && ptr == sv.data() + sv.size()) {
        return result;
    }
    return 0;
}

static auto parse_uint32(std::string_view sv) -> uint32_t {
    uint32_t result = 0;
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), result);
    if (ec == std::errc{} && ptr == sv.data() + sv.size()) {
        return result;
    }
    return 0;
}

static auto parse_cli_arg(std::string_view arg) -> std::pair<std::string_view, std::string_view> {
    if (arg.starts_with("--")) {
        arg.remove_prefix(2);
    }
    auto eq_pos = arg.find('=');
    if (eq_pos == std::string_view::npos) {
        return {};
    }
    return {arg.substr(0, eq_pos), arg.substr(eq_pos + 1)};
}

static void apply_config_key(Config& config, std::string_view key, std::string_view value) {
    if (key == "port") {
        auto parsed = parse_uint16(value);
        if (parsed != 0) {
            config.port = parsed;
        }
    } else if (key == "log.dirs") {
        config.log_root = value;
    } else if (key == "max.message.bytes") {
        auto parsed = parse_uint32(value);
        if (parsed != 0) {
            config.max_message_bytes = parsed;
        }
    }
}

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

    for (int i = 0; i < argc; ++i) {
        auto [key, value] = parse_cli_arg(argv[i]);
        if (!key.empty()) {
            apply_config_key(config, key, value);
        }
    }

    return config;
}

} // namespace config
