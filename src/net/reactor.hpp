#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <liburing.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/broker.hpp"
#include "broker/metrics.hpp"
#include "cluster/metadata.hpp"
#include "shard/cross_reactor_queue.hpp"
#include "shard/shard_router.hpp"

namespace config {
struct Config;
}

namespace net {

class WorkerReactor {
  public:
    WorkerReactor(const config::Config& config,
                  const ClusterMetadata& metadata,
                  GroupCoordinator& coordinator,
                  const shard::ShardRouter& shard_router,
                  std::vector<shard::CrossReactorQueues*>& all_queues,
                  size_t reactor_id);
    ~WorkerReactor();

    WorkerReactor(const WorkerReactor&) = delete;
    WorkerReactor& operator=(const WorkerReactor&) = delete;
    WorkerReactor(WorkerReactor&&) = delete;
    WorkerReactor& operator=(WorkerReactor&&) = delete;

    void run();

    [[nodiscard]] auto active_connections() const -> size_t { return connections_.size(); }
  private:
    struct Connection {
        std::vector<uint8_t> read_buf;
        size_t expected_len = 0;
        bool have_header = false;
        std::vector<uint8_t> write_buf;
        size_t write_offset = 0;
        std::deque<std::vector<uint8_t>> write_queue_;
        std::vector<uint8_t> resp_pool;
        int splice_fd = -1;
        unsigned int splice_len = 0;
        bool splice_pending = false;
    };

    void handle_accept();
    void handle_read(int fd);
    void handle_write(int fd);
    void advance_write_queue(int fd);
    void close_connection(int fd);
    void log_metrics();

    void drain_cross_reactor_requests();
    void drain_cross_reactor_responses();
    void harvest_io_completions();

    template <typename T> auto route_partition(const T& request) -> size_t;

    auto
    send_local_response(int fd, Response&& resp, const std::vector<uint8_t>& serialized) -> bool;

    static constexpr auto kMetricsInterval = std::chrono::seconds(30);
    static constexpr int kMaxEvents = 1024;
    static constexpr int kEpollTimeoutMs = 1;

    int epoll_fd_;
    int server_fd_;
    io_uring ring_{};
    size_t reactor_id_;
    size_t reactor_count_;

    const ClusterMetadata& metadata_;
    const shard::ShardRouter& shard_router_;
    GroupCoordinator& coordinator_;
    std::vector<shard::CrossReactorQueues*>& all_queues_;
    shard::CrossReactorQueues own_queues_;

    std::unordered_map<std::string, std::unique_ptr<broker::PartitionContext>> partition_contexts_;
    Broker broker_;
    uint32_t max_message_bytes_;
    size_t max_write_buffer_bytes_;
    broker::BrokerMetrics metrics_;
    std::chrono::steady_clock::time_point last_metrics_log_;
    std::unordered_map<int, Connection> connections_;
    bool running_{true};
};

} // namespace net
