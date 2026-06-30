#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <system_error>
#include <vector>

#include "util/arena.hpp"
#include "util/byte_reader.hpp"

namespace util {

namespace {
inline auto parse_single_record_batch(std::span<const uint8_t> data)
    -> std::expected<std::vector<std::vector<uint8_t>>, std::error_code> {
    ByteReader reader(data);

    auto base_offset = reader.read_int64();
    if (!base_offset) [[unlikely]] {
        return std::unexpected(base_offset.error());
    }

    auto batch_length = reader.read_int32();
    if (!batch_length) [[unlikely]] {
        return std::unexpected(batch_length.error());
    }

    auto partition_leader_epoch = reader.read_int32();
    if (!partition_leader_epoch) [[unlikely]] {
        return std::unexpected(partition_leader_epoch.error());
    }

    auto magic = reader.read_int8();
    if (!magic) [[unlikely]] {
        return std::unexpected(magic.error());
    }
    if (*magic != 2) [[unlikely]] {
        return std::unexpected(make_error_code(std::errc::protocol_not_supported));
    }

    auto crc = reader.read_int32();
    if (!crc) [[unlikely]] {
        return std::unexpected(crc.error());
    }

    auto attributes = reader.read_int16();
    if (!attributes) [[unlikely]] {
        return std::unexpected(attributes.error());
    }

    auto last_offset_delta = reader.read_int32();
    if (!last_offset_delta) [[unlikely]] {
        return std::unexpected(last_offset_delta.error());
    }

    auto base_timestamp = reader.read_int64();
    if (!base_timestamp) [[unlikely]] {
        return std::unexpected(base_timestamp.error());
    }

    auto max_timestamp = reader.read_int64();
    if (!max_timestamp) [[unlikely]] {
        return std::unexpected(max_timestamp.error());
    }

    auto producer_id = reader.read_int64();
    if (!producer_id) [[unlikely]] {
        return std::unexpected(producer_id.error());
    }

    auto producer_epoch = reader.read_int16();
    if (!producer_epoch) [[unlikely]] {
        return std::unexpected(producer_epoch.error());
    }

    auto base_sequence = reader.read_int32();
    if (!base_sequence) [[unlikely]] {
        return std::unexpected(base_sequence.error());
    }

    int32_t record_count = *last_offset_delta + 1;
    std::vector<std::vector<uint8_t>> values;
    values.reserve(static_cast<size_t>(record_count));

    for (int32_t i = 0; i < record_count; ++i) {
        auto body_size = reader.read_signed_varint();
        if (!body_size) [[unlikely]] {
            return std::unexpected(body_size.error());
        }
        if (*body_size <= 0) {
            continue;
        }

        auto attrs = reader.read_int8();
        if (!attrs) [[unlikely]] {
            return std::unexpected(attrs.error());
        }

        auto timestamp_delta = reader.read_signed_varint();
        if (!timestamp_delta) [[unlikely]] {
            return std::unexpected(timestamp_delta.error());
        }

        auto offset_delta = reader.read_signed_varint();
        if (!offset_delta) [[unlikely]] {
            return std::unexpected(offset_delta.error());
        }

        auto key_len = reader.read_signed_varint();
        if (!key_len) [[unlikely]] {
            return std::unexpected(key_len.error());
        }
        if (*key_len > 0) {
            auto key_result = reader.read_bytes(static_cast<size_t>(*key_len));
            if (!key_result) [[unlikely]] {
                return std::unexpected(key_result.error());
            }
        }

        auto value_len = reader.read_signed_varint();
        if (!value_len) [[unlikely]] {
            return std::unexpected(value_len.error());
        }
        std::vector<uint8_t> value;
        if (*value_len > 0) {
            auto val_result = reader.read_bytes(static_cast<size_t>(*value_len));
            if (!val_result) [[unlikely]] {
                return std::unexpected(val_result.error());
            }
            value.assign(val_result->begin(), val_result->end());
        }

        auto headers_count = reader.read_varint();
        if (!headers_count) [[unlikely]] {
            return std::unexpected(headers_count.error());
        }
        if (*headers_count > 0) {
            uint32_t header_count = *headers_count - 1;
            for (uint32_t h = 0; h < header_count; ++h) {
                auto h_key_result = reader.read_signed_varint();
                if (!h_key_result) [[unlikely]] {
                    return std::unexpected(h_key_result.error());
                }
                if (*h_key_result > 0) {
                    auto hk = reader.read_bytes(static_cast<size_t>(*h_key_result));
                    if (!hk) [[unlikely]] {
                        return std::unexpected(hk.error());
                    }
                }
                auto h_val_result = reader.read_signed_varint();
                if (!h_val_result) [[unlikely]] {
                    return std::unexpected(h_val_result.error());
                }
                if (*h_val_result > 0) {
                    auto hv = reader.read_bytes(static_cast<size_t>(*h_val_result));
                    if (!hv) [[unlikely]] {
                        return std::unexpected(hv.error());
                    }
                }
            }
        }

        values.push_back(std::move(value));
    }

    return values;
}
} // namespace

inline auto parse_record_batch(std::span<const uint8_t> data)
    -> std::expected<std::vector<std::vector<uint8_t>>, std::error_code> {
    std::vector<std::vector<uint8_t>> all_values;
    size_t offset = 0;

    while (offset + 12 <= data.size()) {
        auto chunk = data.subspan(offset);
        ByteReader header_reader(chunk);
        auto header_bo = header_reader.read_int64();
        auto header_bl = header_reader.read_int32();
        if (!header_bo || !header_bl || *header_bl < 0) {
            if (all_values.empty()) [[unlikely]] {
                return std::unexpected(make_error_code(std::errc::message_size));
            }
            break;
        }

        size_t batch_size = 12 + static_cast<size_t>(*header_bl);
        if (offset + batch_size > data.size()) {
            if (all_values.empty()) [[unlikely]] {
                return std::unexpected(make_error_code(std::errc::message_size));
            }
            break;
        }

        auto result = parse_single_record_batch(data.subspan(offset, batch_size));
        if (!result) {
            if (all_values.empty()) [[unlikely]] {
                return std::unexpected(result.error());
            }
            break;
        }

        for (auto& v : *result) {
            all_values.push_back(std::move(v));
        }
        offset += batch_size;
    }

    if (all_values.empty()) [[unlikely]] {
        return std::unexpected(make_error_code(std::errc::message_size));
    }
    return all_values;
}

inline auto record_batch_count(std::span<const uint8_t> data)
    -> std::expected<int32_t, std::error_code> {
    if (data.size() < 28) return std::unexpected(make_error_code(std::errc::message_size));
    ByteReader reader(data);
    auto base_offset = reader.read_int64();      (void)base_offset;
    auto batch_len = reader.read_int32();        (void)batch_len;
    auto leader_epoch = reader.read_int32();     (void)leader_epoch;
    auto magic = reader.read_int8();
    if (!magic || *magic != 2) return std::unexpected(make_error_code(std::errc::protocol_not_supported));
    auto crc = reader.read_int32();              (void)crc;
    auto attrs = reader.read_int16();            (void)attrs;
    auto last_offset_delta = reader.read_int32();
    if (!last_offset_delta) return std::unexpected(last_offset_delta.error());
    return *last_offset_delta + 1;
}

inline auto parse_record_batch(std::span<const uint8_t> data, [[maybe_unused]] Arena& arena)
    -> std::expected<std::vector<std::vector<uint8_t>>, std::error_code> {
    auto result = parse_record_batch(data);
    if (!result) return std::unexpected(result.error());
    return std::move(*result);
}

} // namespace util
