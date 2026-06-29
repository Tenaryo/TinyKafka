#pragma once

#include <cstdint>
#include <format>
#include <string>

namespace storage {

class PartitionLog {
  public:
    PartitionLog(std::string log_root, std::string topic_name, int32_t partition)
        : log_root_(std::move(log_root)), topic_name_(std::move(topic_name)),
          partition_(partition) {}

    [[nodiscard]] auto dir_path() const -> std::string {
        return std::format("{}/{}-{}", log_root_, topic_name_, partition_);
    }

    [[nodiscard]] auto segment_path(int64_t base_offset) const -> std::string {
        return std::format("{}/{:020d}.log", dir_path(), base_offset);
    }
  private:
    std::string log_root_;
    std::string topic_name_;
    int32_t partition_;
};

} // namespace storage
