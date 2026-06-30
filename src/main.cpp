#include <cstdlib>
#include <thread>
#include <vector>

#include "cluster/metadata.hpp"
#include "config/config.hpp"
#include "logging/logger.hpp"
#include "net/reactor.hpp"
#include "shard/shard_router.hpp"

int main(int argc, char** argv) {
    try {
        auto config = config::Config::load(argc, argv);

        auto metadata = parse_cluster_metadata_file(
            config.log_root + "/__cluster_metadata-0/00000000000000000000.log");

        shard::ShardRouter shard_router(config.reactor_count);
        GroupCoordinator coordinator;

        std::vector<shard::CrossReactorQueues> all_queues(config.reactor_count);
        std::vector<shard::CrossReactorQueues*> queue_ptrs;
        queue_ptrs.reserve(config.reactor_count);
        for (size_t i = 0; i < config.reactor_count; ++i) {
            queue_ptrs.push_back(&all_queues[i]);
        }

        std::vector<std::thread> threads;
        threads.reserve(config.reactor_count);
        for (size_t i = 0; i < config.reactor_count; ++i) {
            threads.emplace_back([&, i] {
                net::WorkerReactor reactor(
                    config, metadata, coordinator, shard_router, queue_ptrs, i);
                reactor.run();
            });
        }

        for (auto& t : threads) {
            t.join();
        }

    } catch (const std::exception& e) {
        logging::error(std::string("Fatal: ") + e.what());
        return EXIT_FAILURE;
    }

    return 0;
}
