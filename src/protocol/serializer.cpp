#include "protocol/serializer.hpp"

#include <array>

#include "util/endian.hpp"
#include "util/overloaded.hpp"

auto serialize(const Response& resp) -> std::vector<std::uint8_t> {
    return std::visit(
        overloaded{
            [](const ApiVersionsResponse& r) -> std::vector<std::uint8_t> {
                std::array<std::uint8_t, 10> buf{};
                write_int32_be(0, std::span<std::uint8_t, 4>{buf.data(), 4});
                write_int32_be(r.correlation_id, std::span<std::uint8_t, 4>{buf.data() + 4, 4});
                write_int16_be(r.error_code, std::span<std::uint8_t, 2>{buf.data() + 8, 2});
                return {buf.begin(), buf.end()};
            },
        },
        resp);
}
