#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

namespace test_helpers {

using TopicId = std::array<uint8_t, 16>;

inline auto make_tmp_log_dir() -> std::string {
    static std::atomic<int> counter{0};
    auto path = std::filesystem::temp_directory_path() /
                ("tinytk_test_" + std::to_string(getpid()) + "_" + std::to_string(++counter));
    std::filesystem::create_directories(path);
    return path.string();
}

inline auto make_unique_temp_dir() -> std::string {
    static std::atomic<int> counter{0};
    auto path = std::filesystem::temp_directory_path() /
                ("tinytk_int_" + std::to_string(getpid()) + "_" + std::to_string(++counter));
    std::filesystem::create_directories(path);
    return path.string();
}

inline void push_be16(std::vector<uint8_t>& buf, int16_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

inline void push_be32(std::vector<uint8_t>& buf, int32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

inline void push_be64(std::vector<uint8_t>& buf, int64_t v) {
    for (int i = 7; i >= 0; --i)
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
}

inline void push_signed_varint(std::vector<uint8_t>& buf, int32_t val) {
    uint32_t encoded =
        static_cast<uint32_t>((static_cast<uint32_t>(val) << 1) ^ static_cast<uint32_t>(val >> 31));
    while (encoded > 0x7F) {
        buf.push_back(static_cast<uint8_t>((encoded & 0x7F) | 0x80));
        encoded >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(encoded & 0x7F));
}

inline void push_unsigned_varint(std::vector<uint8_t>& buf, uint32_t val) {
    while (val > 0x7F) {
        buf.push_back(static_cast<uint8_t>((val & 0x7F) | 0x80));
        val >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(val & 0x7F));
}

inline auto make_record_batch_v2(const std::vector<std::vector<uint8_t>>& record_values)
    -> std::vector<uint8_t> {
    std::vector<uint8_t> buf;
    push_be64(buf, 0);
    size_t batch_len_pos = buf.size();
    push_be32(buf, 0);
    push_be32(buf, 0);
    buf.push_back(0x02);
    push_be32(buf, 0);
    push_be16(buf, 0);
    push_be32(buf, static_cast<int32_t>(record_values.size()) - 1);
    push_be64(buf, 0);
    push_be64(buf, 0);
    push_be64(buf, 0);
    push_be16(buf, 0);
    push_be32(buf, 0);

    for (const auto& value : record_values) {
        uint32_t body_size = 1 + 1 + 1 + 1 + 1 + static_cast<uint32_t>(value.size()) + 1;
        push_signed_varint(buf, static_cast<int32_t>(body_size));
        buf.push_back(0x00);
        push_signed_varint(buf, 0);
        push_signed_varint(buf, 0);
        push_signed_varint(buf, -1);
        push_signed_varint(buf, static_cast<int32_t>(value.size()));
        buf.insert(buf.end(), value.begin(), value.end());
        push_signed_varint(buf, 0);
    }

    int32_t batch_len = static_cast<int32_t>(buf.size() - batch_len_pos - 4);
    buf[batch_len_pos] = static_cast<uint8_t>((batch_len >> 24) & 0xFF);
    buf[batch_len_pos + 1] = static_cast<uint8_t>((batch_len >> 16) & 0xFF);
    buf[batch_len_pos + 2] = static_cast<uint8_t>((batch_len >> 8) & 0xFF);
    buf[batch_len_pos + 3] = static_cast<uint8_t>(batch_len & 0xFF);

    return buf;
}

} // namespace test_helpers
