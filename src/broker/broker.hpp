#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "broker/group_coordinator.hpp"
#include "broker/metadata_handler.hpp"
#include "broker/partition_context.hpp"
#include "broker/record_handler.hpp"
#include "cluster/metadata.hpp"
#include "protocol/request.hpp"
#include "protocol/response.hpp"

class Broker {
  public:
    explicit Broker(const ClusterMetadata& metadata,
                    std::string log_root,
                    size_t segment_bytes,
                    io_uring* ring,
                    GroupCoordinator& coordinator,
                    std::unordered_map<std::string, std::unique_ptr<broker::PartitionContext>>&
                        partition_contexts)
        : metadata_(metadata), log_root_(std::move(log_root)), metadata_handler_(metadata_),
          record_handler_(partition_contexts, metadata_, log_root_, segment_bytes, ring),
          coordinator_(&coordinator) {}

    auto handle(const Request& req) -> Response;
  private:
    const ClusterMetadata& metadata_; // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::string log_root_;
    MetadataHandler metadata_handler_;
    RecordHandler record_handler_;
    GroupCoordinator* coordinator_;
};
