#include "net/reactor.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <format>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "config/config.hpp"
#include "logging/logger.hpp"
#include "net/socket.hpp"
#include "protocol/parser.hpp"
#include "protocol/serializer.hpp"
#include "util/endian.hpp"

namespace net {

EpollReactor::EpollReactor(const config::Config& config, ClusterMetadata metadata, int server_fd)
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)), server_fd_(server_fd),
      broker_(std::move(metadata), config.log_root, config.segment_bytes),
      max_message_bytes_(config.max_message_bytes),
      max_write_buffer_bytes_(config.max_write_buffer_bytes),
      last_metrics_log_(std::chrono::steady_clock::now()) {
    ::io_uring_queue_init(64, &ring_, 0);
    if (epoll_fd_ < 0) [[unlikely]] {
        logging::error("epoll_create1 failed: " + std::to_string(errno));
        return;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) [[unlikely]] {
        logging::error("epoll_ctl ADD server failed: " + std::to_string(errno));
    }
}

EpollReactor::~EpollReactor() {
    ::io_uring_queue_exit(&ring_);
    for (auto& [fd, _] : connections_) {
        ::close(fd);
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
    }
    if (server_fd_ >= 0) {
        ::close(server_fd_);
    }
}

void EpollReactor::run() {
    std::array<epoll_event, 1024> events{};
    logging::info("Reactor started");
    while (true) {
        int nfds = ::epoll_wait(epoll_fd_, events.data(), events.size(), -1);
        if (nfds < 0) [[unlikely]] {
            if (errno == EINTR) {
                continue;
            }
            logging::error("epoll_wait failed: " + std::to_string(errno));
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events.at(size_t(i)).data.fd;
            uint32_t evts = events.at(size_t(i)).events;

            if (fd == server_fd_) {
                handle_accept();
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

        auto now = std::chrono::steady_clock::now();
        if (now - last_metrics_log_ >= kMetricsInterval) {
            log_metrics();
            last_metrics_log_ = now;
        }
    }
}

void EpollReactor::handle_accept() {
    int client_fd = ::accept(server_fd_, nullptr, nullptr);
    if (client_fd < 0) [[unlikely]] {
        logging::error("accept failed: " + std::to_string(errno));
        return;
    }

    auto flags = ::fcntl(client_fd, F_GETFL, 0);
    if (flags < 0) [[unlikely]] {
        logging::error("fcntl F_GETFL failed: " + std::to_string(errno));
        ::close(client_fd);
        return;
    }
    ::fcntl(client_fd, F_SETFL, flags | O_NONBLOCK); // NOLINT(cppcoreguidelines-pro-type-vararg)

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = client_fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev) < 0) [[unlikely]] {
        logging::error("epoll_ctl ADD client failed: " + std::to_string(errno));
        ::close(client_fd);
        return;
    }

    connections_[client_fd] = Connection{};
}

void EpollReactor::handle_read(int fd) {
    auto& conn = connections_[fd];

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

void EpollReactor::handle_write(int fd) {
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
        if (conn.splice_fd >= 0) {
            ::io_uring_sqe* sqe = ::io_uring_get_sqe(&ring_);
            if (sqe) {
                ::io_uring_prep_splice(sqe, conn.splice_fd, 0, fd, -1, conn.splice_len, 0);
                ::io_uring_submit(&ring_);
                ::io_uring_cqe* cqe = nullptr;
                ::io_uring_wait_cqe(&ring_, &cqe);
                [[maybe_unused]] auto res = cqe->res;
                ::io_uring_cqe_seen(&ring_, cqe);
                metrics_.bytes_sent.fetch_add(static_cast<size_t>(res > 0 ? res : 0),
                                               std::memory_order_relaxed);
            }
            conn.splice_fd = -1;
            conn.splice_len = 0;
        }
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
}

void EpollReactor::close_connection(int fd) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    connections_.erase(fd);
}

auto EpollReactor::snapshot() const -> broker::BrokerMetrics::Snapshot {
    return metrics_.snapshot();
}

void EpollReactor::log_metrics() {
    auto s = metrics_.snapshot();
    logging::info(
        std::format("metrics: requests_total={} bytes_received={} bytes_sent={} errors_total={} "
                    "active_connections={}",
                    s.requests_total,
                    s.bytes_received,
                    s.bytes_sent,
                    s.errors_total,
                    connections_.size()));
}

} // namespace net
