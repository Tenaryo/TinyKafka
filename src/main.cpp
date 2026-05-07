#include <cstdlib>
#include <iostream>

#include "server.hpp"

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    auto server = Server::create(9092);
    if (!server) {
        std::cerr << "Failed to start server: " << server.error().message() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << "Waiting for a client to connect...\n";

    auto result = server->run();
    if (!result) {
        std::cerr << "Server error: " << result.error().message() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << "Client connected\n";
    return EXIT_SUCCESS;
}
