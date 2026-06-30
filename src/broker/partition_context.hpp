#pragma once

#include <chrono>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <format>
#include <liburing.h>
#include <mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "storage/log_reader.hpp"
#include "util/arena.hpp"
#include "storage/partition_log.hpp"
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
                     size_t segment_bytes = 0,
                     io_uring* ring = nullptr)
        : log_root_(std::move(log_root)), topic_name_(std::move(topic_name)), partition_(partition),
          log_(log_root_, topic_name_, partition_), segment_bytes_(segment_bytes), ring_(ring) {}

    [[nodiscard]] auto produce(std::span<const uint8_t> record_batch_data) -> ProduceResult {
        std::lock_guard lock(mutex_);

        auto count_result = util::record_batch_count(record_batch_data);
        if (!count_result) {
            return {};
        }

        auto record_count = *count_result;
        auto now = std::chrono::system_clock::now();
        auto append_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        auto dir = log_.dir_path();
        std::error_code dir_ec;
        std::filesystem::create_directories(dir, dir_ec);
        if (dir_ec) {
            return {};
        }

        size_t blob_size = record_batch_data.size();

        if (segment_bytes_ > 0 && current_segment_bytes_ + blob_size > segment_bytes_) {
            if (write_fd_ >= 0) {
                ::close(write_fd_);
                write_fd_ = -1;
            }
            current_segment_base_offset_ = next_offset_;
            current_segment_bytes_ = 0;
            current_segment_index_.clear();
        }

        if (write_fd_ < 0) {
            auto path = log_.segment_path(current_segment_base_offset_);
            write_fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            if (write_fd_ < 0) return {};
        }

        if (next_offset_ % kSparseIndexInterval == 0) {
            current_segment_index_.push_back(
                {.offset = next_offset_, .file_position = current_segment_bytes_});
        }

        if (ring_) {
            io_uring_sqe* sqe = io_uring_get_sqe(ring_);
            if (sqe) {
                io_uring_prep_write(sqe, write_fd_, record_batch_data.data(),
                                    static_cast<unsigned int>(blob_size), 0);
                io_uring_submit(ring_);
                io_uring_cqe* cqe = nullptr;
                io_uring_wait_cqe(ring_, &cqe);
                if (cqe->res < 0) {
                    io_uring_cqe_seen(ring_, cqe);
                    return {};
                }
                io_uring_cqe_seen(ring_, cqe);
            }
        } else {
            ssize_t w [[maybe_unused]] = ::write(write_fd_, record_batch_data.data(), blob_size);
        }

        auto base_offset = next_offset_;
        current_segment_bytes_ += blob_size;
        next_offset_ += record_count;
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

        constexpr size_t kMaxFetchChunk = 10 * 1024 * 1024;

        const SparseIndexEntry* best = nullptr;
        for (const auto& entry : current_segment_index_) {
            if (entry.offset <= offset) {
                best = &entry;
            } else {
                break;
            }
        }

        size_t start_position = best ? best->file_position : 0;
        int64_t entry_offset = best ? best->offset : 0;
        int64_t segment_base = best ? current_segment_base_offset_ : 0;

        auto path = log_.segment_path(segment_base);
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            return {};
        }

        auto file_size = static_cast<size_t>(::lseek(fd, 0, SEEK_END));
        if (file_size <= 0) {
            ::close(fd);
            return {};
        }

        size_t read_limit = static_cast<size_t>(max_bytes) * 2;
        if (read_limit > kMaxFetchChunk) {
            read_limit = kMaxFetchChunk;
        }

        size_t available = file_size - start_position;
        size_t read_size = std::min(read_limit, available);
        if (read_size == 0) {
            ::close(fd);
            return {};
        }

        std::vector<uint8_t> raw_data(read_size);
        auto actual = ::pread(fd, raw_data.data(), read_size,
                               static_cast<off_t>(start_position));
        ::close(fd);
        if (actual < 0) {
            return {};
        }
        raw_data.resize(static_cast<size_t>(actual));

        auto records = util::parse_record_batch(raw_data);
        if (!records || records->empty()) {
            if (raw_data.size() > static_cast<size_t>(max_bytes)) {
                raw_data.resize(static_cast<size_t>(max_bytes));
            }
            return raw_data;
        }

        int64_t skip_count = offset - entry_offset;
        util::Arena arena;
        std::vector<uint8_t> result;
        result.reserve(static_cast<size_t>(max_bytes));
        for (size_t i = 0; i < records->size(); ++i) {
            if (static_cast<int64_t>(i) < skip_count) {
                continue;
            }
            result.insert(result.end(), (*records)[i].begin(), (*records)[i].end());
            if (result.size() >= static_cast<size_t>(max_bytes)) {
                break;
            }
        }

        if (result.size() > static_cast<size_t>(max_bytes)) {
            result.resize(static_cast<size_t>(max_bytes));
        }
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

    [[nodiscard]] auto file_fd() -> int {
        if (splice_fd_ < 0 && write_fd_ >= 0) {
            auto path = log_.segment_path(current_segment_base_offset_);
            splice_fd_ = ::open(path.c_str(), O_RDONLY);
        }
        return splice_fd_;
    }

    struct SpliceInfo {
        int fd = -1;
        size_t file_offset = 0;
        size_t length = 0;
    };

    [[nodiscard]] auto splice_info(int64_t fetch_offset, int32_t max_bytes) -> SpliceInfo {
        std::lock_guard lock(mutex_);
        auto fd = write_fd_ >= 0 ? write_fd_ : -1;
        if (fd < 0) return {};

        const SparseIndexEntry* best = nullptr;
        for (const auto& entry : current_segment_index_) {
            if (entry.offset <= fetch_offset) {
                best = &entry;
            } else {
                break;
            }
        }
        size_t offset = best ? best->file_position : 0;
        size_t available = current_segment_bytes_ - offset;
        if (available < 65536) return {};
        size_t len = std::min(available, static_cast<size_t>(max_bytes));
        return {fd, offset, len};
    }

    [[nodiscard]] auto file_position() const -> size_t {
        return current_segment_bytes_;
    }

    [[nodiscard]] auto segment_base_offset() const -> int64_t {
        return current_segment_base_offset_;
    }
  private:
    std::string log_root_;
    std::string topic_name_;
    int32_t partition_;
    storage::PartitionLog log_;
    int64_t next_offset_{0};
    int64_t current_segment_base_offset_{0};
    size_t current_segment_bytes_{0};
    size_t segment_bytes_{0};
    std::vector<SparseIndexEntry> current_segment_index_;
    io_uring* ring_{nullptr};
    int write_fd_{-1};
    int splice_fd_{-1};
    mutable std::mutex mutex_;
};

} // namespace broker
