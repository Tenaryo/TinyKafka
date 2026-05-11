#include "storage/log_reader.hpp"

#include <format>
#include <fstream>
#include <iterator>

namespace storage {

// TODO: Reading the entire file into memory is wasteful for large logs.
// Future improvement: read incrementally by offset + max_bytes from the Fetch request.
auto read_topic_log(std::string_view root_path,
                    std::string_view topic_name,
                    int32_t partition) -> std::vector<uint8_t> {
    auto path = std::format("{}/{}-{}/00000000000000000000.log", root_path, topic_name, partition);
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return {};
    }
    auto sz = file.tellg();
    if (sz <= 0) {
        return {};
    }
    file.seekg(0);
    std::vector<uint8_t> data(static_cast<size_t>(sz));
    file.read(reinterpret_cast<char*>(data.data()), sz);
    return data;
}

} // namespace storage
