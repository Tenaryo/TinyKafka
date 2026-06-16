#include <cstdlib>
#include <iostream>
#include <thread>
#include <unistd.h>
#include <vector>

#include "broker/broker.hpp"
#include "cluster/metadata.hpp"
#include "net/server.hpp"
#include "net/socket.hpp"
#include "protocol/parser.hpp"
#include "protocol/serializer.hpp"
#include "util/endian.hpp"

int main() {
    constexpr std::string_view kLogRoot = "/tmp/kraft-combined-logs";

    auto metadata = parse_cluster_metadata_file(
        "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");

    auto server = Server::create(9092);
    if (!server) {
        std::cerr << "Failed to start server: " << server.error().message() << '\n';
        return EXIT_FAILURE;
    }

    std::cout << "Waiting for clients to connect...\n";

    while (true) {
        auto client = server->accept();
        if (!client) {
            std::cerr << "Accept failed: " << client.error().message() << '\n';
            continue;
        }

        int client_fd = *client;

        std::thread([client_fd, &metadata, kLogRoot] {
            Broker broker(metadata, std::string(kLogRoot));
            std::vector<std::uint8_t> buf;

            while (true) {
                std::array<std::uint8_t, 4> len_buf{};
                auto len_result = recv_all(client_fd, len_buf);
                if (!len_result) {
                    std::cerr << "Read error: " << len_result.error().message() << '\n';
                    break;
                }
                if (*len_result == 0) {
                    break;
                }

                auto message_length = static_cast<std::size_t>(
                    decode_int32_be(std::span<const std::uint8_t, 4>{len_buf}));
                if (message_length > 1'048'576) {
                    std::cerr << "Message too large: " << message_length << '\n';
                    break;
                }

                buf.resize(message_length);
                auto body_result = recv_all(client_fd, buf);
                if (!body_result || *body_result != message_length) {
                    break;
                }

                auto req = parse_request(buf);
                if (!req) {
                    std::cerr << "Parse failed: " << req.error().message() << '\n';
                    break;
                }

                auto resp = broker.handle(*req);
                auto bytes = serialize(resp);

                auto send_result = send_all(client_fd, bytes);
                if (!send_result) {
                    std::cerr << "Send failed: " << send_result.error().message() << '\n';
                    break;
                }
            }

            ::close(client_fd);
            std::cout << "Client disconnected\n";
        }).detach();
    }
}
