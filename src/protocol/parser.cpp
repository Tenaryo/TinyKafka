#include "protocol/parser.hpp"

#include <cstring>

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
            auto skip_topic_tag = reader.skip(1); // TAG_BUFFER
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
            auto cursor_tag = reader.skip(1); // TAG_BUFFER
            if (!cursor_tag)
                return std::unexpected(cursor_tag.error());
        }

        auto final_tag = reader.skip(1); // TAG_BUFFER
        if (!final_tag)
            return std::unexpected(final_tag.error());

        return DescribeTopicPartitionsRequest{
            RequestHeader{*api_key, *api_version, *correlation_id}, std::move(topic_names)};
    }
    case 1: {
        auto client_id_len = reader.read_int16();
        if (!client_id_len)
            return std::unexpected(client_id_len.error());
        if (*client_id_len > 0) {
            auto skip_client = reader.skip(static_cast<size_t>(*client_id_len));
            if (!skip_client)
                return std::unexpected(skip_client.error());
        }
        auto header_tag = reader.skip(1); // TAG_BUFFER
        if (!header_tag)
            return std::unexpected(header_tag.error());

        auto max_wait_ms = reader.read_int32();
        if (!max_wait_ms)
            return std::unexpected(max_wait_ms.error());
        auto min_bytes = reader.read_int32();
        if (!min_bytes)
            return std::unexpected(min_bytes.error());
        auto max_bytes = reader.read_int32();
        if (!max_bytes)
            return std::unexpected(max_bytes.error());
        auto isolation_level = reader.read_int8();
        if (!isolation_level)
            return std::unexpected(isolation_level.error());
        auto session_id = reader.read_int32();
        if (!session_id)
            return std::unexpected(session_id.error());
        auto session_epoch = reader.read_int32();
        if (!session_epoch)
            return std::unexpected(session_epoch.error());

        auto topic_array_len = reader.read_varint();
        if (!topic_array_len)
            return std::unexpected(topic_array_len.error());
        uint32_t topic_count = *topic_array_len > 0 ? *topic_array_len - 1 : 0;

        std::vector<FetchTopicRequest> topics;
        topics.reserve(topic_count);
        for (uint32_t ti = 0; ti < topic_count; ++ti) {
            auto tid_bytes = reader.read_bytes(16);
            if (!tid_bytes)
                return std::unexpected(tid_bytes.error());
            std::array<uint8_t, 16> tid{};
            std::memcpy(tid.data(), tid_bytes->data(), 16);

            auto part_array_len = reader.read_varint();
            if (!part_array_len)
                return std::unexpected(part_array_len.error());
            uint32_t part_count = *part_array_len > 0 ? *part_array_len - 1 : 0;

            std::vector<FetchPartitionRequest> parts;
            parts.reserve(part_count);
            for (uint32_t pi = 0; pi < part_count; ++pi) {
                auto part_idx = reader.read_int32(); // partition_index
                if (!part_idx)
                    return std::unexpected(part_idx.error());
                auto skip_leader_epoch = reader.skip(4); // current_leader_epoch
                if (!skip_leader_epoch)
                    return std::unexpected(skip_leader_epoch.error());
                auto skip_fetch_offset = reader.skip(8); // fetch_offset
                if (!skip_fetch_offset)
                    return std::unexpected(skip_fetch_offset.error());
                auto skip_last_epoch = reader.skip(4); // last_fetched_epoch
                if (!skip_last_epoch)
                    return std::unexpected(skip_last_epoch.error());
                auto skip_log_start = reader.skip(8); // log_start_offset
                if (!skip_log_start)
                    return std::unexpected(skip_log_start.error());
                auto skip_max_bytes = reader.skip(4); // max_bytes
                if (!skip_max_bytes)
                    return std::unexpected(skip_max_bytes.error());
                auto skip_part_tag = reader.skip(1); // TAG_BUFFER
                if (!skip_part_tag)
                    return std::unexpected(skip_part_tag.error());
                parts.push_back({.partition_index = *part_idx});
            }

            auto skip_topic_tag = reader.skip(1); // TAG_BUFFER
            if (!skip_topic_tag)
                return std::unexpected(skip_topic_tag.error());
            topics.push_back({.topic_id = tid, .partitions = std::move(parts)});
        }

        auto forgotten_len = reader.read_varint();
        if (!forgotten_len)
            return std::unexpected(forgotten_len.error());
        uint32_t forgotten_count = *forgotten_len > 0 ? *forgotten_len - 1 : 0;
        for (uint32_t fi = 0; fi < forgotten_count; ++fi) {
            auto skip_ftid = reader.skip(16); // forgotten topic_id
            if (!skip_ftid)
                return std::unexpected(skip_ftid.error());
            auto fpart_len = reader.read_varint();
            if (!fpart_len)
                return std::unexpected(fpart_len.error());
            uint32_t fpart_count = *fpart_len > 0 ? *fpart_len - 1 : 0;
            auto skip_fparts = reader.skip(fpart_count * 4); // partition indices
            if (!skip_fparts)
                return std::unexpected(skip_fparts.error());
            auto skip_ftag = reader.skip(1); // TAG_BUFFER
            if (!skip_ftag)
                return std::unexpected(skip_ftag.error());
        }

        auto rack_id_len = reader.read_varint();
        if (!rack_id_len)
            return std::unexpected(rack_id_len.error());
        if (*rack_id_len > 1) {
            auto skip_rack = reader.skip(static_cast<size_t>(*rack_id_len - 1));
            if (!skip_rack)
                return std::unexpected(skip_rack.error());
        }

        auto body_tag = reader.skip(1); // TAG_BUFFER
        if (!body_tag)
            return std::unexpected(body_tag.error());

        return FetchRequest{
            RequestHeader{*api_key, *api_version, *correlation_id}, std::move(topics), *max_bytes};
    }
    case 0: {
        auto client_id_len = reader.read_int16();
        if (!client_id_len)
            return std::unexpected(client_id_len.error());
        if (*client_id_len > 0) {
            auto skip_client = reader.skip(static_cast<size_t>(*client_id_len));
            if (!skip_client)
                return std::unexpected(skip_client.error());
        }
        auto header_tag = reader.skip(1); // TAG_BUFFER
        if (!header_tag)
            return std::unexpected(header_tag.error());

        auto tx_id_len = reader.read_varint();
        if (!tx_id_len)
            return std::unexpected(tx_id_len.error());
        if (*tx_id_len > 1) {
            auto skip_tx = reader.skip(static_cast<size_t>(*tx_id_len - 1));
            if (!skip_tx)
                return std::unexpected(skip_tx.error());
        }

        auto acks = reader.read_int16();
        if (!acks)
            return std::unexpected(acks.error());

        auto timeout = reader.read_int32();
        if (!timeout)
            return std::unexpected(timeout.error());

        auto topic_array_len = reader.read_varint();
        if (!topic_array_len)
            return std::unexpected(topic_array_len.error());
        uint32_t topic_count = *topic_array_len > 0 ? *topic_array_len - 1 : 0;

        std::vector<ProduceTopicRequest> topics;
        topics.reserve(topic_count);
        for (uint32_t ti = 0; ti < topic_count; ++ti) {
            auto name = reader.read_compact_string();
            if (!name)
                return std::unexpected(name.error());

            auto part_array_len = reader.read_varint();
            if (!part_array_len)
                return std::unexpected(part_array_len.error());
            uint32_t part_count = *part_array_len > 0 ? *part_array_len - 1 : 0;

            std::vector<ProducePartitionRequest> parts;
            parts.reserve(part_count);
            for (uint32_t pi = 0; pi < part_count; ++pi) {
                auto part_idx = reader.read_int32();
                if (!part_idx)
                    return std::unexpected(part_idx.error());

                auto records_len = reader.read_varint();
                if (!records_len)
                    return std::unexpected(records_len.error());
                if (*records_len > 1) {
                    auto skip_records = reader.skip(static_cast<size_t>(*records_len - 1));
                    if (!skip_records)
                        return std::unexpected(skip_records.error());
                }

                auto rec_tag = reader.skip(1); // TAG_BUFFER
                if (!rec_tag)
                    return std::unexpected(rec_tag.error());

                parts.push_back({.partition_index = *part_idx});
            }

            auto topic_end_tag = reader.skip(1); // TAG_BUFFER
            if (!topic_end_tag)
                return std::unexpected(topic_end_tag.error());

            topics.push_back({.topic_name = std::move(*name), .partitions = std::move(parts)});
        }

        auto body_tag = reader.skip(1); // TAG_BUFFER
        if (!body_tag)
            return std::unexpected(body_tag.error());

        return ProduceRequest{RequestHeader{*api_key, *api_version, *correlation_id},
                              std::move(topics)};
    }
    default:
        return std::unexpected(make_error_code(std::errc::function_not_supported));
    }
}
