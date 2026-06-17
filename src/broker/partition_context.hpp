#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "storage/log_reader.hpp"
#include "storage/log_writer.hpp"
#include "util/record_batch.hpp"

namespace broker {

struct ProduceResult {
    int64_t base_offset = -1;
    int64_t log_append_time_ms = -1;
};

class PartitionContext {
  public:
    PartitionContext(std::string log_root, std::string topic_name, int32_t partition)
        : log_root_(std::move(log_root)), topic_name_(std::move(topic_name)),
          partition_(partition) {}

    [[nodiscard]] auto produce(std::span<const uint8_t> record_batch_data) -> ProduceResult {
        std::lock_guard lock(mutex_);

        auto values = util::parse_record_batch(record_batch_data);
        if (!values) {
            return {};
        }

        auto now = std::chrono::system_clock::now();
        auto append_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        auto base_offset = next_offset_;
        for (const auto& value : *values) {
            auto ec = storage::write_topic_log(log_root_, topic_name_, partition_, value);
            if (ec) {
                next_offset_ = base_offset;
                return {};
            }
            ++next_offset_;
        }
        return {.base_offset = base_offset, .log_append_time_ms = append_time};
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
