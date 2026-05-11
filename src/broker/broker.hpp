#pragma once

#include "cluster/metadata.hpp"
#include "protocol/request.hpp"
#include "protocol/response.hpp"

class Broker {
  public:
    explicit Broker(ClusterMetadata metadata = {}) : metadata_(std::move(metadata)) {}

    auto handle(const Request& req) -> Response;
  private:
    [[nodiscard]] auto build_topic_metadata(const std::string& topic_name) const -> TopicMetadata;
    [[nodiscard]] auto find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
        -> const ClusterMetadata::TopicInfo*;

    ClusterMetadata metadata_;
};
