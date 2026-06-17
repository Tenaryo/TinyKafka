#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "util/endian.hpp"
#include "util/varint.hpp"

class ByteWriter {
  public:
    constexpr explicit ByteWriter(std::span<uint8_t> buf) noexcept : data_(buf) {}

    [[nodiscard]] constexpr auto offset() const noexcept -> size_t { return offset_; }

    [[nodiscard]] constexpr auto written() const noexcept -> std::span<uint8_t> {
        return data_.first(offset_);
    }

    [[nodiscard]] constexpr auto remaining() const noexcept -> std::span<uint8_t> {
        return data_.subspan(offset_);
    }

    constexpr void write_int8(uint8_t value) noexcept {
        assert(offset_ < data_.size());
        data_[offset_++] = value;
    }

    constexpr void write_int16(int16_t value) noexcept {
        assert(offset_ + 2 <= data_.size());
        write_int16_be(value, std::span<uint8_t, 2>{data_.data() + offset_, 2});
        offset_ += 2;
    }

    constexpr void write_int32(int32_t value) noexcept {
        assert(offset_ + 4 <= data_.size());
        write_int32_be(value, std::span<uint8_t, 4>{data_.data() + offset_, 4});
        offset_ += 4;
    }

    constexpr void write_int64(int64_t value) noexcept {
        assert(offset_ + 8 <= data_.size());
        write_int64_be(value, std::span<uint8_t, 8>{data_.data() + offset_, 8});
        offset_ += 8;
    }

    void write_bytes(std::span<const uint8_t> data) noexcept {
        assert(offset_ + data.size() <= data_.size());
        std::memcpy(data_.data() + offset_, data.data(), data.size());
        offset_ += data.size();
    }

    auto write_varint(uint32_t value) noexcept -> size_t {
        size_t written = write_unsigned_varint(value, data_.subspan(offset_));
        offset_ += written;
        return written;
    }

    auto write_signed_varint(int32_t value) noexcept -> size_t {
        size_t written = ::write_signed_varint(value, data_.subspan(offset_));
        offset_ += written;
        return written;
    }

    void write_compact_string(std::string_view str) noexcept {
        write_varint(static_cast<uint32_t>(str.size()) + 1);
        write_bytes(
            std::span<const uint8_t>{reinterpret_cast<const uint8_t*>(str.data()), str.size()});
    }
  private:
    std::span<uint8_t> data_;
    size_t offset_ = 0;
};
