#include <cstdlib>
#include <iostream>
#include <unistd.h>

#include "broker/broker.hpp"
#include "net/server.hpp"
#include "protocol/parser.hpp"
#include "protocol/serializer.hpp"

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    auto server = Server::create(9092);
    if (!server) {
        std::cerr << "Failed to start server: " << server.error().message() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << "Waiting for a client to connect...\n";

    auto client = server->accept();
    if (!client) {
        std::cerr << "Accept failed: " << client.error().message() << '\n';
        return EXIT_FAILURE;
    }
    int client_fd = *client;

    std::array<std::uint8_t, 1024> buf{};
    auto n = ::read(client_fd, buf.data(), buf.size());
    if (n < 0) {
        std::cerr << "Read failed: " << errno << '\n';
        ::close(client_fd);
        return EXIT_FAILURE;
    }

    auto req = parse_request(std::span{buf}.first(static_cast<std::size_t>(n)));
    if (!req) {
        ::close(client_fd);
        return EXIT_FAILURE;
    }

    Broker broker;
    auto resp = broker.handle(*req);
    auto bytes = serialize(resp);

    auto result = send_all(client_fd, bytes);
    ::close(client_fd);

    if (!result) {
        std::cerr << "Send failed: " << result.error().message() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << "Client connected\n";
    return EXIT_SUCCESS;
}
