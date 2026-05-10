#include "protocol/serializer.hpp"

#include <cstring>

#include "util/endian.hpp"
#include "util/overloaded.hpp"
#include "util/varint.hpp"

namespace {

constexpr size_t kResponseHeaderV1Size = 5;

void write_response_header_v1(int32_t correlation_id,
                              std::span<uint8_t, kResponseHeaderV1Size> out) {
    write_int32_be(correlation_id, std::span<uint8_t, 4>{out.data(), 4});
    out[4] = 0x00;
}

} // namespace

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
            [](const DescribeTopicPartitionsResponse& r) -> std::vector<std::uint8_t> {
                size_t body_size = kResponseHeaderV1Size + 4 + 1;
                for (const auto& t : r.topics) {
                    uint32_t name_varint = static_cast<uint32_t>(t.topic_name.size()) + 1;
                    body_size += 2 + varint_encoded_size(name_varint) + t.topic_name.size() + 16 +
                                 1 + 1 + 4 + 1;
                }
                body_size += 2;

                std::vector<uint8_t> buf(4 + body_size);
                write_int32_be(static_cast<int32_t>(body_size),
                               std::span<uint8_t, 4>{buf.data(), 4});
                write_response_header_v1(r.correlation_id,
                                         std::span<uint8_t, kResponseHeaderV1Size>{
                                             buf.data() + 4, kResponseHeaderV1Size});
                size_t offset = 4 + kResponseHeaderV1Size;

                write_int32_be(r.throttle_time_ms, std::span<uint8_t, 4>{buf.data() + offset, 4});
                offset += 4;

                uint32_t topic_count = static_cast<uint32_t>(r.topics.size()) + 1;
                offset += write_unsigned_varint(
                    topic_count, std::span<uint8_t>{buf.data() + offset, buf.size() - offset});

                for (const auto& t : r.topics) {
                    write_int16_be(t.error_code, std::span<uint8_t, 2>{buf.data() + offset, 2});
                    offset += 2;

                    uint32_t name_varint = static_cast<uint32_t>(t.topic_name.size()) + 1;
                    offset += write_unsigned_varint(
                        name_varint, std::span<uint8_t>{buf.data() + offset, buf.size() - offset});
                    std::memcpy(buf.data() + offset, t.topic_name.data(), t.topic_name.size());
                    offset += t.topic_name.size();

                    std::memcpy(buf.data() + offset, t.topic_id.data(), 16);
                    offset += 16;

                    buf[offset++] = t.is_internal ? 1 : 0;

                    buf[offset++] = 0x01;

                    write_int32_be(t.authorized_operations,
                                   std::span<uint8_t, 4>{buf.data() + offset, 4});
                    offset += 4;

                    buf[offset++] = 0x00;
                }

                buf[offset++] = 0xFF;
                buf[offset++] = 0x00;

                return buf;
            },
        },
        resp);
}
