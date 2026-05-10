#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <string>
#include <system_error>

#include "util/endian.hpp"
#include "util/varint.hpp"

class ByteReader {
  public:
    constexpr explicit ByteReader(std::span<const uint8_t> data) noexcept : data_(data) {}

    [[nodiscard]] constexpr auto remaining() const noexcept -> size_t {
        return data_.size() - offset_;
    }

    [[nodiscard]] constexpr auto offset() const noexcept -> size_t { return offset_; }

    constexpr auto read_int8() noexcept -> std::expected<uint8_t, std::error_code> {
        if (remaining() < 1) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        return data_[offset_++];
    }

    constexpr auto read_int16() noexcept -> std::expected<int16_t, std::error_code> {
        if (remaining() < 2) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        auto val = decode_int16_be(std::span<const uint8_t, 2>{data_.data() + offset_, 2});
        offset_ += 2;
        return val;
    }

    constexpr auto read_int32() noexcept -> std::expected<int32_t, std::error_code> {
        if (remaining() < 4) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        auto val = decode_int32_be(std::span<const uint8_t, 4>{data_.data() + offset_, 4});
        offset_ += 4;
        return val;
    }

    constexpr auto
    read_bytes(size_t n) noexcept -> std::expected<std::span<const uint8_t>, std::error_code> {
        if (remaining() < n) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        auto view = data_.subspan(offset_, n);
        offset_ += n;
        return view;
    }

    auto read_varint() noexcept -> std::expected<uint32_t, std::error_code> {
        auto result = read_unsigned_varint(data_.subspan(offset_));
        if (!result) {
            return std::unexpected(result.error());
        }
        offset_ += varint_encoded_size(*result);
        return result;
    }

    auto read_compact_string() noexcept -> std::expected<std::string, std::error_code> {
        auto result = ::read_compact_string(data_.subspan(offset_));
        if (!result) {
            return std::unexpected(result.error());
        }
        auto sv = *result;
        size_t consumed = varint_encoded_size(static_cast<uint32_t>(sv.size()) + 1) + sv.size();
        offset_ += consumed;
        return std::string{sv};
    }

    constexpr auto skip(size_t n) noexcept -> std::expected<void, std::error_code> {
        if (remaining() < n) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        offset_ += n;
        return {};
    }

    auto read_signed_varint() noexcept -> std::expected<int32_t, std::error_code> {
        auto result = ::read_signed_varint(data_.subspan(offset_));
        if (!result) {
            return std::unexpected(result.error());
        }
        offset_ += signed_varint_encoded_size(*result);
        return result;
    }

    auto skip_varint() noexcept -> std::expected<void, std::error_code> {
        auto result = read_unsigned_varint(data_.subspan(offset_));
        if (!result) {
            return std::unexpected(result.error());
        }
        offset_ += varint_encoded_size(*result);
        return {};
    }
  private:
    std::span<const uint8_t> data_;
    size_t offset_ = 0;
};
