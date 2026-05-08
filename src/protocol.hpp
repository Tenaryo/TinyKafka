#pragma once

#include <array>
#include <cstdint>
#include <span>

constexpr void write_int32_be(int32_t value, std::span<uint8_t, 4> out) noexcept {
    out[0] = static_cast<uint8_t>((value >> 24) & 0xFF);
    out[1] = static_cast<uint8_t>((value >> 16) & 0xFF);
    out[2] = static_cast<uint8_t>((value >> 8) & 0xFF);
    out[3] = static_cast<uint8_t>(value & 0xFF);
}

constexpr auto decode_int32_be(std::span<const uint8_t, 4> data) noexcept -> int32_t {
    return (static_cast<int32_t>(data[0]) << 24) | (static_cast<int32_t>(data[1]) << 16) |
           (static_cast<int32_t>(data[2]) << 8) | static_cast<int32_t>(data[3]);
}

constexpr auto build_response(int32_t correlation_id) noexcept -> std::array<uint8_t, 8> {
    std::array<uint8_t, 8> buf{};
    write_int32_be(0, std::span<uint8_t, 4>{buf.data(), 4});
    write_int32_be(correlation_id, std::span<uint8_t, 4>{buf.data() + 4, 4});
    return buf;
}
