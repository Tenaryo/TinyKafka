#pragma once

#include <coroutine>
#include <liburing.h>
#include <mutex>
#include <string>
#include <unordered_map>

#include "broker/partition_context.hpp"
#include "cluster/metadata.hpp"
#include "net/task.hpp"
#include "protocol/request.hpp"
#include "protocol/response.hpp"

class RecordHandler {
  public:
    RecordHandler(std::mutex& mutex,
                  std::unordered_map<std::string, std::unique_ptr<broker::PartitionContext>>&
                      partition_contexts,
                  const ClusterMetadata& metadata,
                  std::string log_root,
                  size_t segment_bytes,
                  io_uring* ring = nullptr)
        : mutex_(&mutex), partition_contexts_(partition_contexts), metadata_(metadata),
          log_root_(std::move(log_root)), segment_bytes_(segment_bytes), ring_(ring) {}

    auto handle_produce(const ProduceRequest& r) -> net::Task<ProduceResponse>;
    auto handle_fetch(const FetchRequest& r) -> FetchResponse;
    auto handle_list_offsets(const ListOffsetsRequest& r) -> ListOffsetsResponse;

  private:
    [[nodiscard]] auto
    find_topic_by_name(const std::string& name) const -> const ClusterMetadata::TopicInfo*;
    [[nodiscard]] auto find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
        -> const ClusterMetadata::TopicInfo*;
    auto get_or_create_context(const std::string& topic_name,
                                int32_t partition) -> broker::PartitionContext&;

    std::mutex* mutex_;
    std::unordered_map<std::string, std::unique_ptr<broker::PartitionContext>>&
        partition_contexts_;
    const ClusterMetadata& metadata_;
    std::string log_root_;
    size_t segment_bytes_;
    io_uring* ring_{nullptr};
};
