#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <system_error>

constexpr auto varint_encoded_size(uint32_t value) noexcept -> size_t {
    if (value == 0)
        return 1;
    size_t size = 0;
    while (value > 0) {
        value >>= 7;
        ++size;
    }
    return size;
}

constexpr auto write_unsigned_varint(uint32_t value, std::span<uint8_t> buf) noexcept -> size_t {
    size_t written = 0;
    while (value > 0x7F) {
        buf[written++] = static_cast<uint8_t>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    buf[written++] = static_cast<uint8_t>(value & 0x7F);
    return written;
}

constexpr auto read_unsigned_varint(std::span<const uint8_t> buf) noexcept
    -> std::expected<uint32_t, std::error_code> {
    uint32_t value = 0;
    uint32_t shift = 0;
    size_t i = 0;
    while (i < buf.size()) {
        uint8_t byte = buf[i++];
        value |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            return value;
        }
        shift += 7;
    }
    return std::unexpected(make_error_code(std::errc::message_size));
}

constexpr auto zigzag_encode(int32_t value) noexcept -> uint32_t {
    return (static_cast<uint32_t>(value) << 1) ^ static_cast<uint32_t>(value >> 31);
}

constexpr auto zigzag_decode(uint32_t encoded) noexcept -> int32_t {
    return static_cast<int32_t>(encoded >> 1) ^ -static_cast<int32_t>(encoded & 1);
}

constexpr auto signed_varint_encoded_size(int32_t value) noexcept -> size_t {
    return varint_encoded_size(zigzag_encode(value));
}

constexpr auto write_signed_varint(int32_t value, std::span<uint8_t> buf) noexcept -> size_t {
    return write_unsigned_varint(zigzag_encode(value), buf);
}

constexpr auto read_signed_varint(std::span<const uint8_t> buf) noexcept
    -> std::expected<int32_t, std::error_code> {
    auto result = read_unsigned_varint(buf);
    if (!result) {
        return std::unexpected(result.error());
    }
    return zigzag_decode(*result);
}

constexpr auto read_compact_string(std::span<const uint8_t> buf) noexcept
    -> std::expected<std::string_view, std::error_code> {
    if (buf.empty()) {
        return std::unexpected(make_error_code(std::errc::message_size));
    }
    auto len_result = read_unsigned_varint(buf);
    if (!len_result) {
        return std::unexpected(len_result.error());
    }
    uint32_t len = *len_result;
    if (len < 1) {
        return std::unexpected(make_error_code(std::errc::message_size));
    }
    size_t str_len = len - 1;
    size_t consumed = varint_encoded_size(len);
    if (buf.size() < consumed + str_len) {
        return std::unexpected(make_error_code(std::errc::message_size));
    }
    return std::string_view{reinterpret_cast<const char*>(buf.data() + consumed), str_len};
}
