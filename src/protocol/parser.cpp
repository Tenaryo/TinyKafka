#include "protocol/parser.hpp"

#include "util/endian.hpp"

auto parse_request(std::span<const std::uint8_t> buf) -> std::expected<Request, std::error_code> {
    if (buf.size() < 12) {
        return std::unexpected(make_error_code(std::errc::message_size));
    }

    auto api_key = decode_int16_be(std::span<const std::uint8_t, 2>{buf.data() + 4, 2});
    auto api_version = decode_int16_be(std::span<const std::uint8_t, 2>{buf.data() + 6, 2});
    auto correlation_id = decode_int32_be(std::span<const std::uint8_t, 4>{buf.data() + 8, 4});

    switch (api_key) {
    case 18:
        return ApiVersionsRequest{RequestHeader{api_key, api_version, correlation_id}};
    default:
        return std::unexpected(make_error_code(std::errc::function_not_supported));
    }
}
