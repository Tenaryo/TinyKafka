#pragma once

#include "cluster/metadata.hpp"
#include "protocol/request.hpp"
#include "protocol/response.hpp"

class MetadataHandler {
  public:
    explicit MetadataHandler(const ClusterMetadata& metadata) : metadata_(metadata) {}

    static auto handle_api_versions(const ApiVersionsRequest& r) -> ApiVersionsResponse;
    auto handle_describe_topic_partitions(const DescribeTopicPartitionsRequest& r)
        -> DescribeTopicPartitionsResponse;
    auto handle_metadata(const MetadataRequest& r) -> MetadataResponse;
  private:
    [[nodiscard]] auto build_topic_metadata(const std::string& topic_name) const -> TopicMetadata;

    const ClusterMetadata& metadata_;
};
