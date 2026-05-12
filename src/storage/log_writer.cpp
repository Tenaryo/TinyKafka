#include "storage/log_writer.hpp"

#include <cerrno>
#include <filesystem>
#include <format>
#include <fstream>

namespace storage {

auto write_topic_log(std::string_view root_path,
                     std::string_view topic_name,
                     int32_t partition,
                     std::span<const uint8_t> records) -> std::error_code {
    auto dir = std::format("{}/{}-{}", root_path, topic_name, partition);
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec)
        return ec;

    auto path = std::format("{}/00000000000000000000.log", dir);
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file)
        return std::error_code(errno, std::generic_category());

    file.write(reinterpret_cast<const char*>(records.data()),
               static_cast<std::streamsize>(records.size()));
    if (!file)
        return std::error_code(errno, std::generic_category());

    return {};
}

} // namespace storage
