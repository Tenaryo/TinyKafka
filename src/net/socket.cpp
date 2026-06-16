#include "net/socket.hpp"

#include <cerrno>
#include <cstddef>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

auto send_all(int fd, std::span<const uint8_t> data) -> std::expected<size_t, std::error_code> {
    size_t sent = 0;
    while (sent < data.size()) {
        auto n = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return sent;
            }
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }
        sent += static_cast<size_t>(n);
    }
    return sent;
}

auto recv_all(int fd, std::span<uint8_t> buf) -> std::expected<size_t, std::error_code> {
    std::size_t received = 0;
    while (received < buf.size()) {
        auto n = ::read(fd, buf.data() + received, buf.size() - received);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return received;
            }
            return std::unexpected(std::error_code(errno, std::generic_category()));
        }
        if (n == 0) {
            return received;
        }
        received += static_cast<std::size_t>(n);
    }
    return received;
}
