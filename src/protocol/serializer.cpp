#include "protocol/serializer.hpp"

#include "util/endian.hpp"
#include "util/overloaded.hpp"
#include "util/varint.hpp"

auto serialize(const Response& resp) -> std::vector<std::uint8_t> {
    return std::visit(
        overloaded{
            [](const ApiVersionsResponse& r) -> std::vector<std::uint8_t> {
                const uint32_t count = static_cast<uint32_t>(r.api_keys.size()) + 1;
                const size_t varint_len = varint_encoded_size(count);
                const size_t body_size = 4 + 2 + varint_len + r.api_keys.size() * 7 + 4 + 1;
                std::vector<uint8_t> buf(4 + body_size);

                write_int32_be(static_cast<int32_t>(body_size),
                               std::span<uint8_t, 4>{buf.data(), 4});
                write_int32_be(r.correlation_id, std::span<uint8_t, 4>{buf.data() + 4, 4});
                write_int16_be(r.error_code, std::span<uint8_t, 2>{buf.data() + 8, 2});

                size_t offset = 10;
                offset += write_unsigned_varint(
                    count, std::span<uint8_t>{buf.data() + offset, buf.size() - offset});

                for (const auto& entry : r.api_keys) {
                    write_int16_be(entry.api_key, std::span<uint8_t, 2>{buf.data() + offset, 2});
                    write_int16_be(entry.min_version,
                                   std::span<uint8_t, 2>{buf.data() + offset + 2, 2});
                    write_int16_be(entry.max_version,
                                   std::span<uint8_t, 2>{buf.data() + offset + 4, 2});
                    buf[offset + 6] = 0x00;
                    offset += 7;
                }

                write_int32_be(r.throttle_time_ms, std::span<uint8_t, 4>{buf.data() + offset, 4});
                buf[offset + 4] = 0x00;

                return buf;
            },
        },
        resp);
}
