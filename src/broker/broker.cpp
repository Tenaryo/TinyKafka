#include "broker/broker.hpp"

#include <algorithm>

#include "protocol/api_registry.hpp"
#include "protocol/response.hpp"
#include "storage/log_reader.hpp"
#include "util/overloaded.hpp"

auto Broker::build_topic_metadata(const std::string& topic_name) const -> TopicMetadata {
    auto it = metadata_.name_to_topic.find(topic_name);
    if (it == metadata_.name_to_topic.end()) {
        return TopicMetadata{
            .error_code = 3,
            .topic_name = topic_name,
            .topic_id = {},
            .is_internal = false,
            .authorized_operations = 0,
            .partitions = {},
        };
    }

    const auto& info = metadata_.topics[it->second];
    std::vector<PartitionMetadata> partitions;
    partitions.reserve(info.partitions.size());
    for (auto pid : info.partitions) {
        partitions.push_back(PartitionMetadata{
            .error_code = 0,
            .partition_index = pid,
            .leader_id = 1,
            .leader_epoch = 0,
            .replica_nodes = {1},
            .isr_nodes = {1},
            .eligible_leader_replicas = {},
            .last_known_elr = {},
            .offline_replicas = {},
        });
    }

    return TopicMetadata{
        .error_code = 0,
        .topic_name = topic_name,
        .topic_id = info.uuid,
        .is_internal = false,
        .authorized_operations = 0,
        .partitions = std::move(partitions),
    };
}

auto Broker::find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
    -> const ClusterMetadata::TopicInfo* {
    auto it = metadata_.uuid_to_topic.find(id);
    if (it == metadata_.uuid_to_topic.end())
        return nullptr;
    return &metadata_.topics[it->second];
}

auto Broker::handle(const Request& req) -> Response {
    return std::visit(
        overloaded{
            [](const ApiVersionsRequest& r) -> Response {
                int16_t error_code =
                    (r.header.api_version >= 0 && r.header.api_version <= 4) ? 0 : 35;
                return ApiVersionsResponse{
                    .correlation_id = r.header.correlation_id,
                    .error_code = error_code,
                    .api_keys = {kSupportedApis.begin(), kSupportedApis.end()},
                    .throttle_time_ms = 0,
                };
            },
            [this](const DescribeTopicPartitionsRequest& r) -> Response {
                std::vector<TopicMetadata> topics;
                topics.reserve(r.topic_names.size());
                for (const auto& name : r.topic_names) {
                    topics.push_back(build_topic_metadata(name));
                }
                std::ranges::sort(topics, {}, &TopicMetadata::topic_name);
                return DescribeTopicPartitionsResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .topics = std::move(topics),
                };
            },
            [this](const ProduceRequest& r) -> Response {
                std::vector<ProduceTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());
                for (const auto& topic_req : r.topics) {
                    std::vector<ProducePartitionResponse> parts;
                    parts.reserve(topic_req.partitions.size());
                    for (const auto& part_req : topic_req.partitions) {
                        parts.push_back(ProducePartitionResponse{
                            .partition_index = part_req.partition_index,
                            .error_code = 3,
                            .base_offset = -1,
                            .log_append_time_ms = -1,
                            .log_start_offset = -1,
                        });
                    }
                    topic_responses.push_back(
                        {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
                }
                return ProduceResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .responses = std::move(topic_responses),
                };
            },
            [this](const FetchRequest& r) -> Response {
                std::vector<FetchTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());
                for (const auto& topic_req : r.topics) {
                    const auto* info = find_topic_by_uuid(topic_req.topic_id);
                    if (info) {
                        std::vector<FetchPartitionResponse> parts;
                        parts.reserve(topic_req.partitions.size());
                        for (const auto& part_req : topic_req.partitions) {
                            auto records = storage::read_topic_log(
                                log_root_, info->name, part_req.partition_index);
                            parts.push_back({.partition_index = part_req.partition_index,
                                             .error_code = 0,
                                             .records = std::move(records)});
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
            },
        },
        req);
}
