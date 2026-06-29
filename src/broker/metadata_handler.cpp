#include "broker/metadata_handler.hpp"

#include <algorithm>

#include "protocol/api_registry.hpp"

auto MetadataHandler::handle_api_versions(const ApiVersionsRequest& r) -> ApiVersionsResponse {
    int16_t error_code = (r.header.api_version >= 0 && r.header.api_version <= 4) ? 0 : 35;
    return ApiVersionsResponse{
        .correlation_id = r.header.correlation_id,
        .error_code = error_code,
        .api_keys = {kSupportedApis.begin(), kSupportedApis.end()},
        .throttle_time_ms = 0,
    };
}

auto MetadataHandler::handle_describe_topic_partitions(const DescribeTopicPartitionsRequest& r)
    -> DescribeTopicPartitionsResponse {
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
}

auto MetadataHandler::handle_metadata(const MetadataRequest& r) -> MetadataResponse {
    std::vector<MetadataTopicResponse> topics;
    if (r.topics.empty()) {
        for (const auto& info : metadata_.topics) {
            auto tm = build_topic_metadata(info.name);
            topics.push_back(MetadataTopicResponse{
                .error_code = tm.error_code,
                .topic_name = tm.topic_name,
                .topic_id = tm.topic_id,
                .is_internal = tm.is_internal,
                .partitions = std::move(tm.partitions),
                .topic_authorized_operations = tm.authorized_operations,
            });
        }
    } else {
        for (const auto& name : r.topics) {
            auto tm = build_topic_metadata(name);
            topics.push_back(MetadataTopicResponse{
                .error_code = tm.error_code,
                .topic_name = tm.topic_name,
                .topic_id = tm.topic_id,
                .is_internal = tm.is_internal,
                .partitions = std::move(tm.partitions),
                .topic_authorized_operations = tm.authorized_operations,
            });
        }
    }
    return MetadataResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .brokers = {{.node_id = 1, .host = "localhost", .port = 9092, .rack = {}}},
        .cluster_id = "TinyKafka",
        .controller_id = 1,
        .topics = std::move(topics),
        .cluster_authorized_operations = 0,
    };
}

auto MetadataHandler::build_topic_metadata(const std::string& topic_name) const -> TopicMetadata {
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
        partitions.push_back(PartitionMetadata{.error_code = 0,
                                               .partition_index = pid,
                                               .leader_id = 1,
                                               .leader_epoch = 0,
                                               .replica_nodes = {1},
                                               .isr_nodes = {1},
                                               .eligible_leader_replicas = {},
                                               .last_known_elr = {},
                                               .offline_replicas = {}});
    }

    return TopicMetadata{.error_code = 0,
                         .topic_name = topic_name,
                         .topic_id = info.uuid,
                         .is_internal = false,
                         .authorized_operations = 0,
                         .partitions = std::move(partitions)};
}
