#pragma once

#include <atomic>
#include <cstdint>

namespace broker {

struct BrokerMetrics {
    std::atomic<uint64_t> requests_total{0};
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> errors_total{0};

    struct Snapshot {
        uint64_t requests_total;
        uint64_t bytes_received;
        uint64_t bytes_sent;
        uint64_t errors_total;
    };

    [[nodiscard]] auto snapshot() const -> Snapshot {
        return Snapshot{
            .requests_total = requests_total.load(),
            .bytes_received = bytes_received.load(),
            .bytes_sent = bytes_sent.load(),
            .errors_total = errors_total.load(),
        };
    }
};

} // namespace broker
