#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "broker/group_coordinator.hpp"
#include "broker/partition_context.hpp"
#include "cluster/metadata.hpp"
#include "protocol/request.hpp"
#include "protocol/response.hpp"

class Broker {
  public:
    explicit Broker(ClusterMetadata metadata = {},
                    std::string log_root = {},
                    size_t segment_bytes = 0)
        : metadata_(std::move(metadata)), log_root_(std::move(log_root)),
          segment_bytes_(segment_bytes) {}

    auto handle(const Request& req) -> Response;
  private:
    [[nodiscard]] auto build_topic_metadata(const std::string& topic_name) const -> TopicMetadata;
    [[nodiscard]] auto find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
        -> const ClusterMetadata::TopicInfo*;
    [[nodiscard]] auto
    find_topic_by_name(const std::string& name) const -> const ClusterMetadata::TopicInfo*;

    auto get_or_create_context(const std::string& topic_name,
                               int32_t partition) -> broker::PartitionContext&;

    ClusterMetadata metadata_;
    std::string log_root_;
    size_t segment_bytes_{0};
    std::mutex contexts_mutex_;
    std::unordered_map<std::string, std::unique_ptr<broker::PartitionContext>> partition_contexts_;
    GroupCoordinator coordinator_;
};
