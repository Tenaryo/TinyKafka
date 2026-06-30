#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
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
    explicit Broker(ClusterMetadata metadata = {},
                    std::string log_root = {},
                    size_t segment_bytes = 0,
                    io_uring* ring = nullptr)
        : metadata_(std::move(metadata)), log_root_(std::move(log_root)),
          metadata_handler_(metadata_), record_handler_(contexts_mutex_,
                                                        partition_contexts_,
                                                        metadata_,
                                                        log_root_,
                                                        segment_bytes,
                                                        ring) {}

    auto handle(const Request& req) -> Response;
  private:
    ClusterMetadata metadata_;
    std::string log_root_;
    std::mutex contexts_mutex_;
    std::unordered_map<std::string, std::unique_ptr<broker::PartitionContext>> partition_contexts_;
    MetadataHandler metadata_handler_;
    RecordHandler record_handler_;
    GroupCoordinator coordinator_;
};
