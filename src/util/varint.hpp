#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

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
