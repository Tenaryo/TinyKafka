#pragma once

#include <cstdint>
#include <mutex>
#include <span>
#include <string>

#include "storage/log_writer.hpp"

namespace broker {

class PartitionContext {
  public:
    PartitionContext(std::string log_root, std::string topic_name, int32_t partition)
        : log_root_(std::move(log_root)), topic_name_(std::move(topic_name)),
          partition_(partition) {}

    [[nodiscard]] auto produce(std::span<const uint8_t> records) -> int64_t {
        std::lock_guard lock(mutex_);
        auto offset = next_offset_;
        auto ec = storage::write_topic_log(log_root_, topic_name_, partition_, records);
        if (ec) {
            return -1;
        }
        ++next_offset_;
        return offset;
    }
  private:
    std::string log_root_;
    std::string topic_name_;
    int32_t partition_;
    int64_t next_offset_{0};
    std::mutex mutex_;
};

} // namespace broker
