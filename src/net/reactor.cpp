#include "net/reactor.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <format>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include "config/config.hpp"
#include "logging/logger.hpp"
#include "net/server.hpp"
#include "net/socket.hpp"
#include "protocol/parser.hpp"
#include "protocol/request.hpp"
#include "protocol/serializer.hpp"
#include "util/endian.hpp"
#include "util/overloaded.hpp"

namespace net {

WorkerReactor::WorkerReactor(const config::Config& config,
                             const ClusterMetadata& metadata,
                             GroupCoordinator& coordinator,
                             const shard::ShardRouter& shard_router,
                             std::vector<shard::CrossReactorQueues*>& all_queues,
                             size_t reactor_id)
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)), reactor_id_(reactor_id),
      reactor_count_(shard_router.reactor_count()), metadata_(metadata),
      shard_router_(shard_router), coordinator_(coordinator), all_queues_(all_queues),
      my_queues_(*all_queues[reactor_id]), broker_(metadata_,
                                                   config.log_root,
                                                   config.segment_bytes,
                                                   &ring_,
                                                   coordinator_,
                                                   partition_contexts_),
      max_message_bytes_(config.max_message_bytes),
      max_write_buffer_bytes_(config.max_write_buffer_bytes),
      last_metrics_log_(std::chrono::steady_clock::now()) {
    ::io_uring_queue_init(256, &ring_, 0);

    auto server = Server::create(config.port);
    if (!server) {
        logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                       ": Failed to create server: " + server.error().message());
        return;
    }
    server_fd_ = server->take_fd();

    if (epoll_fd_ < 0) [[unlikely]] {
        logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                       ": epoll_create1 failed: " + std::to_string(errno));
        return;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) [[unlikely]] {
        logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                       ": epoll_ctl ADD server failed: " + std::to_string(errno));
    }

    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ >= 0) {
        my_queues_.set_wakeup_fd(wakeup_fd_);
        epoll_event wev{};
        wev.events = EPOLLIN;
        wev.data.fd = wakeup_fd_;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_fd_, &wev);
    }
}

WorkerReactor::~WorkerReactor() {
    ::io_uring_queue_exit(&ring_);
    for (auto& [fd, _] : connections_) {
        ::close(fd);
    }
    for (auto& [_, ctx] : partition_contexts_) {
        ctx.reset();
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
    if (server_fd_ >= 0) {
        ::close(server_fd_);
    }
    if (wakeup_fd_ >= 0) {
        ::close(wakeup_fd_);
    }
}

void WorkerReactor::run() {
    std::array<epoll_event, kMaxEvents> events{};
    logging::info("WorkerReactor " + std::to_string(reactor_id_) + " started");

    while (running_) {
        int nfds = ::epoll_wait(epoll_fd_, events.data(), events.size(), -1);
        if (nfds < 0) [[unlikely]] {
            if (errno == EINTR) {
                continue;
            }
            logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                           ": epoll_wait failed: " + std::to_string(errno));
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events.at(static_cast<size_t>(i)).data.fd;
            uint32_t evts = events.at(static_cast<size_t>(i)).events;

            if (fd == server_fd_) {
                handle_accept();
            } else if (fd == wakeup_fd_) {
                uint64_t dummy = 0;
                [[maybe_unused]] auto _ = ::read(wakeup_fd_, &dummy, sizeof(dummy));
            } else if ((evts & (EPOLLERR | EPOLLHUP)) != 0) {
                metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
                close_connection(fd);
            } else {
                if ((evts & EPOLLIN) != 0) {
                    handle_read(fd);
                }
                if ((evts & EPOLLOUT) != 0) {
                    handle_write(fd);
                }
            }
        }

        drain_cross_reactor_requests();
        drain_cross_reactor_responses();
        harvest_io_completions();

        auto now = std::chrono::steady_clock::now();
        if (now - last_metrics_log_ >= kMetricsInterval) {
            log_metrics();
            last_metrics_log_ = now;
        }
    }

    logging::info("WorkerReactor " + std::to_string(reactor_id_) + " stopped");
}

void WorkerReactor::handle_accept() {
    int client_fd = ::accept(server_fd_, nullptr, nullptr);
    if (client_fd < 0) [[unlikely]] {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                           ": accept failed: " + std::to_string(errno));
        }
        return;
    }

    auto flags = ::fcntl(client_fd, F_GETFL, 0);
    if (flags < 0) [[unlikely]] {
        logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                       ": fcntl F_GETFL failed: " + std::to_string(errno));
        ::close(client_fd);
        return;
    }
    ::fcntl(client_fd, F_SETFL, flags | O_NONBLOCK); // NOLINT(cppcoreguidelines-pro-type-vararg)

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = client_fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) < 0) [[unlikely]] {
        logging::error("WorkerReactor " + std::to_string(reactor_id_) +
                       ": epoll_ctl ADD client failed: " + std::to_string(errno));
        ::close(client_fd);
        return;
    }

    connections_[client_fd] = Connection{};
}

namespace {

struct PartitionKey {
    std::string topic;
    int32_t partition;
};

[[nodiscard]] auto lookup_topic_name(const ClusterMetadata& metadata,
                                     const std::array<uint8_t, 16>& uuid) -> const std::string* {
    auto it = metadata.uuid_to_topic.find(uuid);
    if (it == metadata.uuid_to_topic.end()) {
        return nullptr;
    }
    return &metadata.topics[it->second].name;
}

[[nodiscard]] auto extract_partition_key(const Request& req, const ClusterMetadata& metadata)
    -> std::optional<PartitionKey> {
    return std::visit(
        Overloaded{
            [](const ProduceRequest& r) -> std::optional<PartitionKey> {
                if (!r.topics.empty() && !r.topics[0].partitions.empty()) {
                    return PartitionKey{r.topics[0].topic_name,
                                        r.topics[0].partitions[0].partition_index};
                }
                return std::nullopt;
            },
            [&metadata](const FetchRequest& r) -> std::optional<PartitionKey> {
                if (!r.topics.empty() && !r.topics[0].partitions.empty()) {
                    const auto* name = lookup_topic_name(metadata, r.topics[0].topic_id);
                    if (name != nullptr) {
                        return PartitionKey{*name, r.topics[0].partitions[0].partition_index};
                    }
                }
                return std::nullopt;
            },
            [](const ListOffsetsRequest& r) -> std::optional<PartitionKey> {
                if (!r.topics.empty() && !r.topics[0].partitions.empty()) {
                    return PartitionKey{r.topics[0].topic_name,
                                        r.topics[0].partitions[0].partition_index};
                }
                return std::nullopt;
            },
            [](const auto&) -> std::optional<PartitionKey> { return std::nullopt; },
        },
        req);
}

} // namespace

void WorkerReactor::handle_read(int fd) {
    auto* conn_it = &connections_[fd];
    auto& conn = *conn_it;

    while (true) {
        if (!conn.have_header) {
            if (conn.read_buf.size() < 4) {
                size_t old_size = conn.read_buf.size();
                conn.read_buf.resize(4);
                auto result = recv_all(fd, std::span{conn.read_buf}.subspan(old_size));
                conn.read_buf.resize(old_size + (result ? *result : 0));
                if (!result) {
                    break;
                }
                if (*result == 0) {
                    close_connection(fd);
                    return;
                }
                if (conn.read_buf.size() < 4) {
                    break;
                }
            }
            conn.expected_len = static_cast<size_t>(
                decode_int32_be(std::span<const uint8_t, 4>{conn.read_buf.data(), 4}));
            if (conn.expected_len > max_message_bytes_) {
                logging::error("Message too large: " + std::to_string(conn.expected_len));
                metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
                close_connection(fd);
                return;
            }
            conn.have_header = true;
        }

        size_t total_needed = 4 + conn.expected_len;
        if (conn.read_buf.size() < total_needed) {
            size_t old_size = conn.read_buf.size();
            conn.read_buf.resize(total_needed);
            auto result = recv_all(fd, std::span{conn.read_buf}.subspan(old_size));
            conn.read_buf.resize(old_size + (result ? *result : 0));
            if (!result) {
                break;
            }
            if (*result == 0) {
                close_connection(fd);
                return;
            }
            if (conn.read_buf.size() < total_needed) {
                break;
            }
        }

        auto body_span = std::span{conn.read_buf}.subspan(4);
        auto req = parse_request(body_span);
        if (!req) {
            logging::error("Parse failed: " + req.error().message());
            metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
            close_connection(fd);
            return;
        }

        auto partition_key = extract_partition_key(*req, metadata_);
        if (partition_key.has_value()) {
            size_t target = shard_router_.route(partition_key->topic, partition_key->partition);
            if (target != reactor_id_) {
                shard::ForwardedRequest fwd;
                fwd.client_fd = fd;
                fwd.source_reactor_id = reactor_id_;
                fwd.request = std::move(*req);
                all_queues_[target]->push_request(std::move(fwd));

                metrics_.requests_total.fetch_add(1, std::memory_order_relaxed);
                metrics_.bytes_received.fetch_add(total_needed, std::memory_order_relaxed);

                conn.have_header = false;
                conn.read_buf.clear();
                return;
            }
        }

        auto resp = broker_.handle(*req);
        auto& resp_buf = conn.resp_pool;
        resp_buf = serialize(resp);

        if (auto* fetch_resp = std::get_if<FetchResponse>(&resp)) {
            for (auto& tr : fetch_resp->responses) {
                for (auto& pr : tr.partitions) {
                    if (pr.splice_fd >= 0) {
                        conn.splice_fd = pr.splice_fd;
                        conn.splice_len = static_cast<unsigned int>(pr.splice_len);
                    }
                }
            }
        }

        if (resp_buf.size() > max_write_buffer_bytes_) [[unlikely]] {
            logging::error("Response exceeds max_write_buffer_bytes (fd=" + std::to_string(fd) +
                           ", size=" + std::to_string(resp_buf.size()) + ")");
            metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
            close_connection(fd);
            return;
        }

        metrics_.requests_total.fetch_add(1, std::memory_order_relaxed);
        metrics_.bytes_received.fetch_add(total_needed, std::memory_order_relaxed);

        bool was_idle = !conn.write_offset && conn.write_buf.empty();
        if (was_idle) {
            conn.write_buf = std::move(resp_buf);
            conn.write_offset = 0;
        } else {
            conn.write_queue_.push_back(std::move(resp_buf));
        }
        conn.have_header = false;
        conn.read_buf.clear();

        if (was_idle) {
            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLOUT;
            ev.data.fd = fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
        }
        return;
    }
}

void WorkerReactor::handle_write(int fd) {
    auto& conn = connections_[fd];
    auto remaining = std::span{conn.write_buf}.subspan(conn.write_offset);
    auto result = send_all(fd, remaining);

    if (result) {
        conn.write_offset += *result;
        metrics_.bytes_sent.fetch_add(*result, std::memory_order_relaxed);
    } else {
        metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
        close_connection(fd);
        return;
    }

    if (conn.write_offset >= conn.write_buf.size()) {
        if (conn.splice_fd >= 0 && !conn.splice_pending) {
            ::io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
            if (sqe != nullptr) [[likely]] {
                ::io_uring_prep_splice(sqe, conn.splice_fd, 0, fd, -1, conn.splice_len, 0);
                ::io_uring_sqe_set_data(
                    sqe,
                    reinterpret_cast<void*>( // NOLINT(performance-no-int-to-ptr)
                        static_cast<uintptr_t>(fd) << 1));
                ::io_uring_submit(&ring_);
                conn.splice_pending = true;
                conn.splice_fd = -1;
                conn.splice_len = 0;
                return;
            }
        }
        if (!conn.splice_pending) {
            advance_write_queue(fd);
        }
    }
}

void WorkerReactor::advance_write_queue(int fd) {
    auto& conn = connections_[fd];
    if (!conn.write_queue_.empty()) {
        conn.write_buf = std::move(conn.write_queue_.front());
        conn.write_queue_.pop_front();
        conn.write_offset = 0;
    } else {
        conn.write_buf.clear();
        conn.write_offset = 0;

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    }
}

void WorkerReactor::close_connection(int fd) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    connections_.erase(fd);
}

void WorkerReactor::drain_cross_reactor_requests() {
    shard::ForwardedRequest fwd;
    while (my_queues_.try_pop_request(fwd)) {
        auto resp = broker_.handle(fwd.request);
        auto serialized = serialize(resp);

        int splice_fd = -1;
        unsigned splice_len = 0;
        if (auto* fetch_resp = std::get_if<FetchResponse>(&resp)) {
            for (auto& tr : fetch_resp->responses) {
                for (auto& pr : tr.partitions) {
                    if (pr.splice_fd >= 0) {
                        splice_fd = pr.splice_fd;
                        splice_len = static_cast<unsigned int>(pr.splice_len);
                    }
                }
            }
        }

        shard::ForwardedResponse fwd_resp;
        fwd_resp.client_fd = fwd.client_fd;
        fwd_resp.data = std::move(serialized);
        fwd_resp.splice_fd = splice_fd;
        fwd_resp.splice_len = splice_len;

        all_queues_[fwd.source_reactor_id]->push_response(std::move(fwd_resp));
    }
}

void WorkerReactor::drain_cross_reactor_responses() {
    shard::ForwardedResponse fwd_resp;
    while (my_queues_.try_pop_response(fwd_resp)) {
        auto it = connections_.find(fwd_resp.client_fd);
        if (it == connections_.end()) {
            continue;
        }
        auto& conn = it->second;

        if (fwd_resp.splice_fd >= 0) {
            conn.splice_fd = fwd_resp.splice_fd;
            conn.splice_len = fwd_resp.splice_len;
        }

        bool was_idle = !conn.write_offset && conn.write_buf.empty();
        if (was_idle) {
            conn.write_buf = std::move(fwd_resp.data);
            conn.write_offset = 0;
        } else {
            conn.write_queue_.push_back(std::move(fwd_resp.data));
        }

        if (was_idle) {
            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLOUT;
            ev.data.fd = fwd_resp.client_fd;
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fwd_resp.client_fd, &ev);
        }
    }
}

void WorkerReactor::harvest_io_completions() {
    ::io_uring_cqe* cqe = nullptr;
    while (::io_uring_peek_cqe(&ring_, &cqe) == 0) {
        auto user_data = reinterpret_cast<uintptr_t>(::io_uring_cqe_get_data(cqe));

        if (user_data == 0) {
            if (cqe->res < 0) [[unlikely]] {
                metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
            }
        } else if ((user_data & 1) != 0) {
            delete reinterpret_cast<std::vector<uint8_t>*>( // NOLINT(performance-no-int-to-ptr)
                user_data & ~static_cast<uintptr_t>(1));
            if (cqe->res < 0) [[unlikely]] {
                metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            int fd = static_cast<int>(user_data >> 1);
            auto it = connections_.find(fd);
            if (it != connections_.end()) {
                auto& conn = it->second;
                if (cqe->res >= 0) {
                    metrics_.bytes_sent.fetch_add(static_cast<size_t>(cqe->res),
                                                  std::memory_order_relaxed);
                } else {
                    metrics_.errors_total.fetch_add(1, std::memory_order_relaxed);
                }
                conn.splice_pending = false;
                advance_write_queue(fd);
            }
        }

        ::io_uring_cqe_seen(&ring_, cqe);
        cqe = nullptr;
    }
}

void WorkerReactor::log_metrics() {
    auto s = metrics_.snapshot();
    logging::info(
        std::format("WorkerReactor {} metrics: requests_total={} bytes_received={} bytes_sent={} "
                    "errors_total={} active_connections={}",
                    reactor_id_,
                    s.requests_total,
                    s.bytes_received,
                    s.bytes_sent,
                    s.errors_total,
                    connections_.size()));
}

} // namespace net
