#pragma once

#include <cstdint>
#include <span>

constexpr void write_int16_be(int16_t value, std::span<std::uint8_t, 2> out) noexcept {
    out[0] = static_cast<std::uint8_t>((value >> 8) & 0xFF);
    out[1] = static_cast<std::uint8_t>(value & 0xFF);
}

constexpr void write_int32_be(int32_t value, std::span<std::uint8_t, 4> out) noexcept {
    out[0] = static_cast<std::uint8_t>((value >> 24) & 0xFF);
    out[1] = static_cast<std::uint8_t>((value >> 16) & 0xFF);
    out[2] = static_cast<std::uint8_t>((value >> 8) & 0xFF);
    out[3] = static_cast<std::uint8_t>(value & 0xFF);
}

constexpr void write_int64_be(int64_t value, std::span<std::uint8_t, 8> out) noexcept {
    out[0] = static_cast<std::uint8_t>((value >> 56) & 0xFF);
    out[1] = static_cast<std::uint8_t>((value >> 48) & 0xFF);
    out[2] = static_cast<std::uint8_t>((value >> 40) & 0xFF);
    out[3] = static_cast<std::uint8_t>((value >> 32) & 0xFF);
    out[4] = static_cast<std::uint8_t>((value >> 24) & 0xFF);
    out[5] = static_cast<std::uint8_t>((value >> 16) & 0xFF);
    out[6] = static_cast<std::uint8_t>((value >> 8) & 0xFF);
    out[7] = static_cast<std::uint8_t>(value & 0xFF);
}

constexpr auto decode_int16_be(std::span<const std::uint8_t, 2> data) noexcept -> int16_t {
    return static_cast<int16_t>((static_cast<int16_t>(data[0]) << 8) |
                                static_cast<int16_t>(data[1]));
}

constexpr auto decode_int32_be(std::span<const std::uint8_t, 4> data) noexcept -> int32_t {
    return (static_cast<int32_t>(data[0]) << 24) | (static_cast<int32_t>(data[1]) << 16) |
           (static_cast<int32_t>(data[2]) << 8) | static_cast<int32_t>(data[3]);
}

constexpr auto decode_int64_be(std::span<const std::uint8_t, 8> data) noexcept -> int64_t {
    return (static_cast<int64_t>(data[0]) << 56) | (static_cast<int64_t>(data[1]) << 48) |
           (static_cast<int64_t>(data[2]) << 40) | (static_cast<int64_t>(data[3]) << 32) |
           (static_cast<int64_t>(data[4]) << 24) | (static_cast<int64_t>(data[5]) << 16) |
           (static_cast<int64_t>(data[6]) << 8) | static_cast<int64_t>(data[7]);
}
