#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <system_error>
#include <unistd.h>

class Server {
  public:
    static auto create(std::uint16_t port) -> std::expected<Server, std::error_code>;

    ~Server();

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

    [[nodiscard]] auto accept() const -> std::expected<int, std::error_code>;

    [[nodiscard]] auto server_fd() const -> int { return server_fd_; }

    auto take_fd() -> int {
        int fd = server_fd_;
        server_fd_ = -1;
        return fd;
    }
  private:
    explicit Server(int fd) : server_fd_(fd) {}
    int server_fd_;
};
