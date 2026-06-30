#include "broker/record_handler.hpp"

#include <algorithm>

auto RecordHandler::handle_list_offsets(const ListOffsetsRequest& r) -> ListOffsetsResponse {
    std::vector<ListOffsetsTopicResponse> topic_responses;
    topic_responses.reserve(r.topics.size());
    for (const auto& topic_req : r.topics) {
        const auto* info = find_topic_by_name(topic_req.topic_name);
        std::vector<ListOffsetsPartitionResponse> parts;
        parts.reserve(topic_req.partitions.size());
        for (const auto& part_req : topic_req.partitions) {
            int16_t error_code = 0;
            int64_t offset = -1;
            if (info && std::ranges::find(info->partitions, part_req.partition_index) !=
                            info->partitions.end()) {
                if (part_req.timestamp == -2) {
                    offset = 0;
                } else if (part_req.timestamp == -1) {
                    auto& ctx =
                        get_or_create_context(topic_req.topic_name, part_req.partition_index);
                    offset = ctx.current_offset();
                } else {
                    error_code = -1;
                }
            } else {
                error_code = 3;
            }
            parts.push_back({.partition_index = part_req.partition_index,
                             .error_code = error_code,
                             .offset = offset,
                             .timestamp = -1});
        }
        topic_responses.push_back(
            {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
    }
    return ListOffsetsResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .topics = std::move(topic_responses),
    };
}

auto RecordHandler::handle_produce(const ProduceRequest& r) -> ProduceResponse {
    std::vector<ProduceTopicResponse> topic_responses;
    topic_responses.reserve(r.topics.size());
    for (const auto& topic_req : r.topics) {
        const auto* info = find_topic_by_name(topic_req.topic_name);
        std::vector<ProducePartitionResponse> parts;
        parts.reserve(topic_req.partitions.size());
        for (const auto& part_req : topic_req.partitions) {
            if (info && std::ranges::find(info->partitions, part_req.partition_index) !=
                            info->partitions.end()) {
                int16_t error_code = 0;
                int64_t base_offset = 0;
                int64_t log_append_time_ms = -1;
                int64_t log_start_offset = 0;
                if (!part_req.records.empty()) {
                    auto& ctx =
                        get_or_create_context(topic_req.topic_name, part_req.partition_index);
                    auto result = ctx.produce(part_req.records);
                    base_offset = result.base_offset;
                    log_append_time_ms = result.log_append_time_ms;
                    if (base_offset < 0) {
                        error_code = 56;
                        log_start_offset = -1;
                    }
                }
                parts.push_back(
                    ProducePartitionResponse{.partition_index = part_req.partition_index,
                                             .error_code = error_code,
                                             .base_offset = base_offset,
                                             .log_append_time_ms = log_append_time_ms,
                                             .log_start_offset = log_start_offset});
            } else {
                parts.push_back(
                    ProducePartitionResponse{.partition_index = part_req.partition_index,
                                             .error_code = 3,
                                             .base_offset = -1,
                                             .log_append_time_ms = -1,
                                             .log_start_offset = -1});
            }
        }
        topic_responses.push_back(
            {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
    }
    return ProduceResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .responses = std::move(topic_responses),
    };
}

auto RecordHandler::handle_fetch(const FetchRequest& r) -> FetchResponse {
    std::vector<FetchTopicResponse> topic_responses;
    topic_responses.reserve(r.topics.size());
    for (const auto& topic_req : r.topics) {
        const auto* info = find_topic_by_uuid(topic_req.topic_id);
        if (info) {
            std::vector<FetchPartitionResponse> parts;
            parts.reserve(topic_req.partitions.size());
            for (const auto& part_req : topic_req.partitions) {
                auto& ctx = get_or_create_context(info->name, part_req.partition_index);
                std::vector<uint8_t> records;
                int splice_fd = -1;
                size_t splice_offset = 0;
                size_t splice_len = 0;

                auto offset = part_req.fetch_offset > 0 ? part_req.fetch_offset : 0;
                auto mb = part_req.max_bytes > 0 ? part_req.max_bytes : 1'048'576;

                auto si = ctx.splice_info(offset, mb);
                if (si.fd >= 0) {
                    splice_fd = si.fd;
                    splice_offset = si.file_offset;
                    splice_len = si.length;
                } else if (part_req.fetch_offset > 0 || part_req.max_bytes > 0) {
                    records = ctx.fetch(part_req.fetch_offset, part_req.max_bytes);
                } else {
                    records = ctx.fetch();
                }
                parts.push_back({.partition_index = part_req.partition_index,
                                 .error_code = 0,
                                 .records = std::move(records),
                                 .splice_fd = splice_fd,
                                 .splice_offset = splice_offset,
                                 .splice_len = splice_len});
            }
            topic_responses.push_back(
                {.topic_id = topic_req.topic_id, .partitions = std::move(parts)});
        } else {
            topic_responses.push_back(FetchTopicResponse{
                .topic_id = topic_req.topic_id,
                .partitions =
                    {
                        FetchPartitionResponse{
                            .partition_index = 0,
                            .error_code = 100,
                            .records = {},
                        },
                    },
            });
        }
    }
    return FetchResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .error_code = 0,
        .session_id = 0,
        .responses = std::move(topic_responses),
    };
}

auto RecordHandler::find_topic_by_name(const std::string& name) const
    -> const ClusterMetadata::TopicInfo* {
    auto it = metadata_.name_to_topic.find(name);
    if (it == metadata_.name_to_topic.end()) {
        return nullptr;
    }
    return &metadata_.topics[it->second];
}

auto RecordHandler::find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
    -> const ClusterMetadata::TopicInfo* {
    auto it = metadata_.uuid_to_topic.find(id);
    if (it == metadata_.uuid_to_topic.end()) {
        return nullptr;
    }
    return &metadata_.topics[it->second];
}

auto RecordHandler::get_or_create_context(const std::string& topic_name,
                                          int32_t partition) -> broker::PartitionContext& {
    auto key = topic_name + ":" + std::to_string(partition);
    std::lock_guard lock(*mutex_);
    auto [it, inserted] =
        partition_contexts_.try_emplace(key,
                                        std::make_unique<broker::PartitionContext>(
                                            log_root_, topic_name, partition, segment_bytes_));
    return *it->second;
}
