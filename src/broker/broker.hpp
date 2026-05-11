#pragma once

#include <string>

#include "cluster/metadata.hpp"
#include "protocol/request.hpp"
#include "protocol/response.hpp"

class Broker {
  public:
    explicit Broker(ClusterMetadata metadata = {}, std::string log_root = {})
        : metadata_(std::move(metadata)), log_root_(std::move(log_root)) {}

    auto handle(const Request& req) -> Response;
  private:
    [[nodiscard]] auto build_topic_metadata(const std::string& topic_name) const -> TopicMetadata;
    [[nodiscard]] auto find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
        -> const ClusterMetadata::TopicInfo*;
    [[nodiscard]] auto
    find_topic_by_name(const std::string& name) const -> const ClusterMetadata::TopicInfo*;

    ClusterMetadata metadata_;
    std::string log_root_;
};
