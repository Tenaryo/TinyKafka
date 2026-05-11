#include "broker/broker.hpp"

#include <algorithm>

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

auto Broker::find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
    -> const ClusterMetadata::TopicInfo* {
    for (const auto& [name, info] : metadata_.topics) {
        if (info.uuid == id)
            return &info;
    }
    return nullptr;
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
                              std::ranges::sort(topics, {}, &TopicMetadata::topic_name);
                              return DescribeTopicPartitionsResponse{
                                  .correlation_id = r.header.correlation_id,
                                  .throttle_time_ms = 0,
                                  .topics = std::move(topics),
                              };
                          },
                          [this](const FetchRequest& r) -> Response {
                              std::vector<FetchTopicResponse> topic_responses;
                              topic_responses.reserve(r.topic_ids.size());
                              for (const auto& tid : r.topic_ids) {
                                  const auto* info = find_topic_by_uuid(tid);
                                  if (info) {
                                      std::vector<FetchPartitionResponse> parts;
                                      parts.reserve(info->partitions.size());
                                      for (int32_t pid : info->partitions) {
                                          parts.push_back(
                                              {.partition_index = pid, .error_code = 0});
                                      }
                                      topic_responses.push_back(
                                          {.topic_id = tid, .partitions = std::move(parts)});
                                  } else {
                                      topic_responses.push_back(FetchTopicResponse{
                                          .topic_id = tid,
                                          .partitions =
                                              {
                                                  FetchPartitionResponse{
                                                      .partition_index = 0,
                                                      .error_code = 100,
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
