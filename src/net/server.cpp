#include "server.hpp"

#include <cerrno>
#include <unistd.h>

#include <netinet/in.h>
#include <sys/socket.h>

auto Server::create(std::uint16_t port) -> std::expected<Server, std::error_code> {
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

Server::~Server() {
    if (server_fd_ >= 0) {
        ::close(server_fd_);
    }
}

auto Server::accept() -> std::expected<int, std::error_code> {
    sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    int client_fd =
        ::accept(server_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
    if (client_fd < 0) {
        return std::unexpected(std::error_code(errno, std::generic_category()));
    }

    return client_fd;
}
