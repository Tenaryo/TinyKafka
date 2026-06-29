#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "storage/log_reader.hpp"
#include "util/record_batch.hpp"

namespace broker {

struct SparseIndexEntry {
    int64_t offset;
    size_t file_position;
};

constexpr ptrdiff_t kSparseIndexInterval = 1000;

struct ProduceResult {
    int64_t base_offset = -1;
    int64_t log_append_time_ms = -1;
};

class PartitionContext {
  public:
    PartitionContext(std::string log_root,
                     std::string topic_name,
                     int32_t partition,
                     size_t segment_bytes = 0)
        : log_root_(std::move(log_root)), topic_name_(std::move(topic_name)), partition_(partition),
          segment_bytes_(segment_bytes) {}

    [[nodiscard]] auto produce(std::span<const uint8_t> record_batch_data) -> ProduceResult {
        std::lock_guard lock(mutex_);

        auto values = util::parse_record_batch(record_batch_data);
        if (!values) {
            return {};
        }

        auto now = std::chrono::system_clock::now();
        auto append_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        auto dir = std::format("{}/{}-{}", log_root_, topic_name_, partition_);
        std::error_code dir_ec;
        std::filesystem::create_directories(dir, dir_ec);
        if (dir_ec) {
            return {};
        }

        auto path = std::format("{}/{:020d}.log", dir, current_segment_base_offset_);
        std::ofstream file(path, std::ios::binary | std::ios::app);
        if (!file) {
            return {};
        }

        auto base_offset = next_offset_;
        for (const auto& value : *values) {
            if (segment_bytes_ > 0 && current_segment_bytes_ + value.size() > segment_bytes_) {
                file.close();
                current_segment_base_offset_ = next_offset_;
                current_segment_bytes_ = 0;
                current_segment_index_.clear();
                path = std::format("{}/{:020d}.log", dir, current_segment_base_offset_);
                file.open(path, std::ios::binary | std::ios::app);
                if (!file) {
                    next_offset_ = base_offset;
                    return {};
                }
            }

            if (next_offset_ % kSparseIndexInterval == 0) {
                current_segment_index_.push_back(
                    {.offset = next_offset_, .file_position = current_segment_bytes_});
            }

            file.write(reinterpret_cast<const char*>(value.data()),
                       static_cast<std::streamsize>(value.size()));
            if (!file) {
                next_offset_ = base_offset;
                return {};
            }
            current_segment_bytes_ += value.size();
            ++next_offset_;
        }
        return {.base_offset = base_offset, .log_append_time_ms = append_time};
    }

    [[nodiscard]] auto fetch() -> std::vector<uint8_t> {
        std::lock_guard lock(mutex_);
        return storage::read_topic_log(log_root_, topic_name_, partition_);
    }

    [[nodiscard]] auto fetch(int64_t offset, int32_t max_bytes) -> std::vector<uint8_t> {
        std::lock_guard lock(mutex_);

        if (max_bytes <= 0) {
            return {};
        }

        const SparseIndexEntry* best = nullptr;
        for (const auto& entry : current_segment_index_) {
            if (entry.offset <= offset) {
                best = &entry;
            } else {
                break;
            }
        }

        size_t start_position = best ? best->file_position : 0;
        int64_t segment_base = best ? current_segment_base_offset_ : 0;

        auto path =
            std::format("{}/{}-{}/{:020d}.log", log_root_, topic_name_, partition_, segment_base);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) {
            return {};
        }

        file.seekg(static_cast<std::streamoff>(start_position), std::ios::beg);
        if (!file) {
            return {};
        }

        std::vector<uint8_t> result(static_cast<size_t>(max_bytes));
        file.read(reinterpret_cast<char*>(result.data()), max_bytes);
        auto read_bytes = file.gcount();
        result.resize(static_cast<size_t>(read_bytes));
        return result;
    }

    [[nodiscard]] auto current_offset() const -> int64_t {
        std::lock_guard lock(mutex_);
        return next_offset_;
    }

    [[nodiscard]] auto segment_index() const -> std::vector<SparseIndexEntry> {
        std::lock_guard lock(mutex_);
        return current_segment_index_;
    }
  private:
    std::string log_root_;
    std::string topic_name_;
    int32_t partition_;
    int64_t next_offset_{0};
    int64_t current_segment_base_offset_{0};
    size_t current_segment_bytes_{0};
    size_t segment_bytes_{0};
    std::vector<SparseIndexEntry> current_segment_index_;
    mutable std::mutex mutex_;
};

} // namespace broker
