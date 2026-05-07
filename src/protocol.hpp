#pragma once

#include <array>
#include <cstdint>

constexpr auto encode_int32_be(int32_t value) -> std::array<uint8_t, 4> {
    return {
        static_cast<uint8_t>((value >> 24) & 0xFF),
        static_cast<uint8_t>((value >> 16) & 0xFF),
        static_cast<uint8_t>((value >> 8) & 0xFF),
        static_cast<uint8_t>(value & 0xFF),
    };
}

constexpr auto build_response_v0() -> std::array<uint8_t, 8> {
    auto size = encode_int32_be(0);
    auto id = encode_int32_be(7);
    return {size[0], size[1], size[2], size[3], id[0], id[1], id[2], id[3]};
}
