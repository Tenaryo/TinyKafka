#include <cstdlib>

#include "cluster/metadata.hpp"
#include "config/config.hpp"
#include "logging/logger.hpp"
#include "net/reactor.hpp"
#include "net/server.hpp"

int main(int argc, char** argv) {
    auto config = config::Config::load(argc, argv);

    auto metadata = parse_cluster_metadata_file(config.log_root +
                                                "/__cluster_metadata-0/00000000000000000000.log");

    auto server = Server::create(config.port);
    if (!server) {
        logging::error("Failed to start server: " + server.error().message());
        return EXIT_FAILURE;
    }

    logging::info("Starting server on port " + std::to_string(config.port));
    net::EpollReactor reactor(config, std::move(metadata), server->take_fd());
    reactor.run();

    return 0;
}
