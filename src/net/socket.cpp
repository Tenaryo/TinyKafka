#include "net/socket.hpp"

#include <cerrno>
#include <cstddef>
#include <sys/socket.h>
#include <unistd.h>

auto send_all(int fd, std::span<const std::uint8_t> data) -> std::expected<void, std::error_code> {
    std::size_t sent = 0;
    while (sent < data.size()) {
        auto n = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (n < 0) {
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }
        sent += static_cast<std::size_t>(n);
    }
    return {};
}

auto recv_all(int fd, std::span<std::uint8_t> buf) -> std::expected<std::size_t, std::error_code> {
    std::size_t received = 0;
    while (received < buf.size()) {
        auto n = ::read(fd, buf.data() + received, buf.size() - received);
        if (n < 0) {
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }
        if (n == 0) {
            return received;
        }
        received += static_cast<std::size_t>(n);
    }
    return received;
}
