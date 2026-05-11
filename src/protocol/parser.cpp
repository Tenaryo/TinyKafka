#include "protocol/parser.hpp"

#include "util/byte_reader.hpp"
#include "util/endian.hpp"

auto parse_request(std::span<const std::uint8_t> buf) -> std::expected<Request, std::error_code> {
    ByteReader reader(buf);

    auto api_key = reader.read_int16();
    if (!api_key)
        return std::unexpected(api_key.error());
    auto api_version = reader.read_int16();
    if (!api_version)
        return std::unexpected(api_version.error());
    auto correlation_id = reader.read_int32();
    if (!correlation_id)
        return std::unexpected(correlation_id.error());

    switch (*api_key) {
    case 18:
        return ApiVersionsRequest{RequestHeader{*api_key, *api_version, *correlation_id}};
    case 75: {
        auto client_id_len = reader.read_int16();
        if (!client_id_len) {
            return std::unexpected(client_id_len.error());
        }
        if (*client_id_len > 0) {
            auto skip_client = reader.skip(static_cast<size_t>(*client_id_len));
            if (!skip_client)
                return std::unexpected(skip_client.error());
        }
        auto skip_tag = reader.skip(1);
        if (!skip_tag)
            return std::unexpected(skip_tag.error());

        auto array_len = reader.read_varint();
        if (!array_len)
            return std::unexpected(array_len.error());
        uint32_t topic_count = *array_len - 1;

        std::vector<std::string> topic_names;
        topic_names.reserve(topic_count);
        for (uint32_t i = 0; i < topic_count; ++i) {
            auto name = reader.read_compact_string();
            if (!name)
                return std::unexpected(name.error());
            topic_names.push_back(std::move(*name));
            auto skip_topic_tag = reader.skip(1);
            if (!skip_topic_tag)
                return std::unexpected(skip_topic_tag.error());
        }

        auto partition_limit = reader.read_int32();
        if (!partition_limit)
            return std::unexpected(partition_limit.error());

        auto cursor_flag = reader.read_int8();
        if (!cursor_flag)
            return std::unexpected(cursor_flag.error());
        if (*cursor_flag != 0xFF) {
            auto cursor_name = reader.read_compact_string();
            if (!cursor_name)
                return std::unexpected(cursor_name.error());
            auto cursor_partition = reader.read_int32();
            if (!cursor_partition)
                return std::unexpected(cursor_partition.error());
            auto cursor_tag = reader.skip(1);
            if (!cursor_tag)
                return std::unexpected(cursor_tag.error());
        }

        auto final_tag = reader.skip(1);
        if (!final_tag)
            return std::unexpected(final_tag.error());

        return DescribeTopicPartitionsRequest{
            RequestHeader{*api_key, *api_version, *correlation_id}, std::move(topic_names)};
    }
    case 1: {
        auto client_id = reader.read_compact_string();
        if (!client_id)
            return std::unexpected(client_id.error());
        auto header_tag = reader.skip(1);
        if (!header_tag)
            return std::unexpected(header_tag.error());

        return FetchRequest{RequestHeader{*api_key, *api_version, *correlation_id}};
    }
    default:
        return std::unexpected(make_error_code(std::errc::function_not_supported));
    }
}
