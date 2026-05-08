#pragma once

#include <cerrno>
#include <cstdint>
#include <expected>
#include <span>
#include <system_error>
#include <unistd.h>

#include <netinet/in.h>
#include <sys/socket.h>

#include "protocol.hpp"

inline std::expected<void, std::error_code> send_all(int fd, std::span<const std::uint8_t> data) {
    size_t sent = 0;
    while (sent < data.size()) {
        auto n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n < 0) {
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }
        sent += static_cast<size_t>(n);
    }
    return {};
}

inline std::expected<void, std::error_code> handle_client(int fd) {
    uint8_t buf[1024];
    auto n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
        return std::unexpected(std::error_code(errno, std::generic_category()));
    }

    auto correlation_id = decode_int32_be(std::span<const uint8_t, 4>{buf + 8, 4});
    auto response = build_response(correlation_id);
    return send_all(fd, response);
}

class Server {
  public:
    static auto create(uint16_t port) -> std::expected<Server, std::error_code> {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }

        int reuse = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
            ::close(fd);
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            ::close(fd);
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }

        if (::listen(fd, 5) != 0) {
            ::close(fd);
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }

        return Server{fd};
    }

    ~Server() {
        if (server_fd_ >= 0) {
            ::close(server_fd_);
        }
    }

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    Server(Server&& other) noexcept : server_fd_(other.server_fd_) { other.server_fd_ = -1; }

    Server& operator=(Server&& other) noexcept {
        if (this != &other) {
            if (server_fd_ >= 0) {
                ::close(server_fd_);
            }
            server_fd_ = other.server_fd_;
            other.server_fd_ = -1;
        }
        return *this;
    }

    auto run() -> std::expected<void, std::error_code> {
        sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);

        int client_fd =
            ::accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
        if (client_fd < 0) {
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }

        auto result = handle_client(client_fd);
        ::close(client_fd);
        return result;
    }
  private:
    explicit Server(int fd) : server_fd_(fd) {}

    int server_fd_;
};
