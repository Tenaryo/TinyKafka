#include "protocol/parser.hpp"

#include "util/endian.hpp"
#include "util/varint.hpp"

auto parse_request(std::span<const std::uint8_t> buf) -> std::expected<Request, std::error_code> {
    if (buf.size() < 8) {
        return std::unexpected(make_error_code(std::errc::message_size));
    }

    auto api_key = decode_int16_be(std::span<const std::uint8_t, 2>{buf.data() + 0, 2});
    auto api_version = decode_int16_be(std::span<const std::uint8_t, 2>{buf.data() + 2, 2});
    auto correlation_id = decode_int32_be(std::span<const std::uint8_t, 4>{buf.data() + 4, 4});

    switch (api_key) {
    case 18:
        return ApiVersionsRequest{RequestHeader{api_key, api_version, correlation_id}};
    case 75: {
        size_t offset = 8;
        if (buf.size() < offset + 2) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        auto client_id_len =
            decode_int16_be(std::span<const std::uint8_t, 2>{buf.data() + offset, 2});
        offset += 2;
        if (client_id_len > 0) {
            if (buf.size() < offset + static_cast<size_t>(client_id_len)) {
                return std::unexpected(make_error_code(std::errc::message_size));
            }
            offset += static_cast<size_t>(client_id_len);
        }
        if (buf.size() < offset + 1) {
            return std::unexpected(make_error_code(std::errc::message_size));
        }
        offset += 1;

        auto array_len_result = read_unsigned_varint(buf.subspan(offset));
        if (!array_len_result) {
            return std::unexpected(array_len_result.error());
        }
        offset += varint_encoded_size(*array_len_result);
        uint32_t topic_count = *array_len_result - 1;

        std::vector<std::string> topic_names;
        topic_names.reserve(topic_count);
        for (uint32_t i = 0; i < topic_count; ++i) {
            auto name_result = read_compact_string(buf.subspan(offset));
            if (!name_result) {
                return std::unexpected(name_result.error());
            }
            uint32_t name_len = static_cast<uint32_t>(name_result->size());
            size_t consumed = varint_encoded_size(name_len + 1) + name_len;
            topic_names.emplace_back(*name_result);
            offset += consumed;
            if (buf.size() < offset + 1) {
                return std::unexpected(make_error_code(std::errc::message_size));
            }
            offset += 1;
        }

        return DescribeTopicPartitionsRequest{RequestHeader{api_key, api_version, correlation_id},
                                              std::move(topic_names)};
    }
    default:
        return std::unexpected(make_error_code(std::errc::function_not_supported));
    }
}
