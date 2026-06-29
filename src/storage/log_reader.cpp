#include "storage/log_reader.hpp"

#include <filesystem>
#include <format>
#include <fstream>
#include <iterator>
#include <map>

namespace storage {

auto read_topic_log(std::string_view root_path,
                    std::string_view topic_name,
                    int32_t partition) -> std::vector<uint8_t> {
    auto dir = std::format("{}/{}-{}", root_path, topic_name, partition);
    std::vector<uint8_t> result;

    std::map<int64_t, std::filesystem::path> segments;
    std::error_code ec;
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        auto name = entry.path().filename().string();
        if (name.size() == 24 && name.ends_with(".log")) {
            segments[std::stoll(name.substr(0, 20))] = entry.path();
        }
    }

    for (const auto& [_, segment_path] : segments) {
        std::ifstream file(segment_path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            continue;
        }
        auto sz = file.tellg();
        if (sz <= 0) {
            continue;
        }
        file.seekg(0);
        size_t old_size = result.size();
        result.resize(old_size + static_cast<size_t>(sz));
        file.read(reinterpret_cast<char*>(result.data() + old_size), sz);
    }

    return result;
}

} // namespace storage
