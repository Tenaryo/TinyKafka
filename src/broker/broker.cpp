#include "broker/broker.hpp"

#include "protocol/api_registry.hpp"
#include "protocol/response.hpp"
#include "util/overloaded.hpp"

auto Broker::build_topic_metadata(const std::string& topic_name) const -> TopicMetadata {
    auto it = metadata_.topics.find(topic_name);
    if (it == metadata_.topics.end()) {
        return TopicMetadata{
            .error_code = 3,
            .topic_name = topic_name,
            .topic_id = {},
            .is_internal = false,
            .authorized_operations = 0,
            .partitions = {},
        };
    }

    std::vector<PartitionMetadata> partitions;
    partitions.reserve(it->second.partitions.size());
    for (auto pid : it->second.partitions) {
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
        .topic_id = it->second.uuid,
        .is_internal = false,
        .authorized_operations = 0,
        .partitions = std::move(partitions),
    };
}

auto Broker::handle(const Request& req) -> Response {
    return std::visit(overloaded{
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
                              return DescribeTopicPartitionsResponse{
                                  .correlation_id = r.header.correlation_id,
                                  .throttle_time_ms = 0,
                                  .topics = std::move(topics),
                              };
                          },
                      },
                      req);
}
