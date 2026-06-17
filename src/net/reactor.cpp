#include "net/reactor.hpp"

#include <array>
#include <cerrno>

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
      broker_(std::move(metadata), config.log_root), max_message_bytes_(config.max_message_bytes) {
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
            close_connection(fd);
            return;
        }

        auto resp = broker_.handle(*req);
        conn.write_buf = serialize(resp);
        conn.write_offset = 0;
        conn.have_header = false;
        conn.read_buf.clear();

        epoll_event ev{};
        ev.events = EPOLLOUT;
        ev.data.fd = fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
        return;
    }
}

void EpollReactor::handle_write(int fd) {
    auto& conn = connections_[fd];
    auto remaining = std::span{conn.write_buf}.subspan(conn.write_offset);
    auto result = send_all(fd, remaining);

    if (result) {
        conn.write_offset += *result;
    } else {
        close_connection(fd);
        return;
    }

    if (conn.write_offset >= conn.write_buf.size()) {
        conn.write_buf.clear();
        conn.write_offset = 0;

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    }
}

void EpollReactor::close_connection(int fd) {
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    connections_.erase(fd);
}

} // namespace net
