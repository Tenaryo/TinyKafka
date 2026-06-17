#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "broker/metrics.hpp"

TEST(MetricsTest, DefaultCountersAreZero) {
    broker::BrokerMetrics m;
    auto s = m.snapshot();
    EXPECT_EQ(s.requests_total, 0U);
    EXPECT_EQ(s.bytes_received, 0U);
    EXPECT_EQ(s.bytes_sent, 0U);
    EXPECT_EQ(s.errors_total, 0U);
}

TEST(MetricsTest, IncrementsSingleThreaded) {
    broker::BrokerMetrics m;
    m.requests_total.fetch_add(1, std::memory_order_relaxed);
    m.requests_total.fetch_add(2, std::memory_order_relaxed);
    m.bytes_received.fetch_add(100, std::memory_order_relaxed);
    m.bytes_sent.fetch_add(50, std::memory_order_relaxed);
    m.errors_total.fetch_add(3, std::memory_order_relaxed);

    auto s = m.snapshot();
    EXPECT_EQ(s.requests_total, 3U);
    EXPECT_EQ(s.bytes_received, 100U);
    EXPECT_EQ(s.bytes_sent, 50U);
    EXPECT_EQ(s.errors_total, 3U);
}

TEST(MetricsTest, SnapshotReturnsConsistentView) {
    broker::BrokerMetrics m;
    m.requests_total.store(42, std::memory_order_relaxed);
    m.bytes_sent.store(1024, std::memory_order_relaxed);

    auto s = m.snapshot();
    EXPECT_EQ(s.requests_total, 42U);
    EXPECT_EQ(s.bytes_sent, 1024U);
    EXPECT_EQ(s.bytes_received, 0U);
    EXPECT_EQ(s.errors_total, 0U);
}

TEST(MetricsTest, IncrementsMultiThreaded) {
    broker::BrokerMetrics m;

    constexpr int kThreads = 4;
    constexpr int kIncrementsPerThread = 1000;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&m] {
            for (int i = 0; i < kIncrementsPerThread; ++i) {
                m.requests_total.fetch_add(1, std::memory_order_relaxed);
                m.bytes_received.fetch_add(10, std::memory_order_relaxed);
                m.bytes_sent.fetch_add(5, std::memory_order_relaxed);
                m.errors_total.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto total = kThreads * kIncrementsPerThread;
    auto s = m.snapshot();
    EXPECT_EQ(s.requests_total, static_cast<uint64_t>(total));
    EXPECT_EQ(s.bytes_received, static_cast<uint64_t>(total * 10));
    EXPECT_EQ(s.bytes_sent, static_cast<uint64_t>(total * 5));
    EXPECT_EQ(s.errors_total, static_cast<uint64_t>(total));
}
