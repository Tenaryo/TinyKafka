#pragma once

#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "storage/log_reader.hpp"
#include "storage/log_writer.hpp"
#include "util/record_batch.hpp"

namespace broker {

class PartitionContext {
  public:
    PartitionContext(std::string log_root, std::string topic_name, int32_t partition)
        : log_root_(std::move(log_root)), topic_name_(std::move(topic_name)),
          partition_(partition) {}

    [[nodiscard]] auto produce(std::span<const uint8_t> record_batch_data) -> int64_t {
        std::lock_guard lock(mutex_);

        auto values = util::parse_record_batch(record_batch_data);
        if (!values) {
            return -1;
        }

        auto base_offset = next_offset_;
        for (const auto& value : *values) {
            auto ec = storage::write_topic_log(log_root_, topic_name_, partition_, value);
            if (ec) {
                next_offset_ = base_offset;
                return -1;
            }
            ++next_offset_;
        }
        return base_offset;
    }

    [[nodiscard]] auto fetch() -> std::vector<uint8_t> {
        std::lock_guard lock(mutex_);
        return storage::read_topic_log(log_root_, topic_name_, partition_);
    }

    [[nodiscard]] auto current_offset() const -> int64_t {
        std::lock_guard lock(mutex_);
        return next_offset_;
    }
  private:
    std::string log_root_;
    std::string topic_name_;
    int32_t partition_;
    int64_t next_offset_{0};
    mutable std::mutex mutex_;
};

} // namespace broker
