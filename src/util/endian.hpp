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

constexpr auto decode_int16_be(std::span<const std::uint8_t, 2> data) noexcept -> int16_t {
    return (static_cast<int16_t>(data[0]) << 8) | static_cast<int16_t>(data[1]);
}

constexpr auto decode_int32_be(std::span<const std::uint8_t, 4> data) noexcept -> int32_t {
    return (static_cast<int32_t>(data[0]) << 24) | (static_cast<int32_t>(data[1]) << 16) |
           (static_cast<int32_t>(data[2]) << 8) | static_cast<int32_t>(data[3]);
}
