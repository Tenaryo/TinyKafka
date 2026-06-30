// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

#include <gtest/gtest.h>

#include "shard/cross_reactor_queue.hpp"
#include "shard/shard_router.hpp"

TEST(ShardRouterTest, RoutesSameTopicPartitionToSameReactor) {
    shard::ShardRouter router(4);
    auto a = router.route("topic-a", 0);
    auto b = router.route("topic-a", 0);
    EXPECT_EQ(a, b);
}

TEST(ShardRouterTest, DifferentPartitionsMayRouteDifferently) {
    shard::ShardRouter router(4);
    auto a = router.route("topic-a", 0);
    auto b = router.route("topic-a", 1);
    EXPECT_LT(a, 4u);
    EXPECT_LT(b, 4u);
}

TEST(ShardRouterTest, AllRoutesWithinBounds) {
    shard::ShardRouter router(8);
    for (int32_t p = 0; p < 100; ++p) {
        auto r = router.route("test-topic", p);
        EXPECT_LT(r, 8u) << "partition " << p;
    }
}

TEST(ShardRouterTest, SameInputProducesSameOutput) {
    shard::ShardRouter router1(4);
    shard::ShardRouter router2(4);
    for (int32_t p = 0; p < 50; ++p) {
        EXPECT_EQ(router1.route("x", p), router2.route("x", p));
    }
}

TEST(ShardRouterTest, ReactorCount) {
    shard::ShardRouter router(3);
    EXPECT_EQ(router.reactor_count(), 3u);
}

TEST(CrossReactorQueueTest, PushPopRequestRoundtrip) {
    shard::CrossReactorQueues q;
    shard::ForwardedRequest fwd;
    fwd.client_fd = 42;
    fwd.source_reactor_id = 1;
    fwd.request = ApiVersionsRequest{{18, 0, 100}};

    q.push_request(std::move(fwd));

    shard::ForwardedRequest out;
    ASSERT_TRUE(q.try_pop_request(out));
    EXPECT_EQ(out.client_fd, 42);
    EXPECT_EQ(out.source_reactor_id, 1u);
}

TEST(CrossReactorQueueTest, EmptyPopReturnsFalse) {
    shard::CrossReactorQueues q;
    shard::ForwardedRequest out;
    EXPECT_FALSE(q.try_pop_request(out));
}

TEST(CrossReactorQueueTest, PushPopResponseRoundtrip) {
    shard::CrossReactorQueues q;
    shard::ForwardedResponse resp;
    resp.client_fd = 7;
    resp.data = {0x01, 0x02, 0x03};
    resp.splice_fd = -1;
    resp.splice_len = 0;

    q.push_response(std::move(resp));

    shard::ForwardedResponse out;
    ASSERT_TRUE(q.try_pop_response(out));
    EXPECT_EQ(out.client_fd, 7);
    EXPECT_EQ(out.data.size(), 3u);
    EXPECT_EQ(out.splice_fd, -1);
}

TEST(CrossReactorQueueTest, EmptyResponsePopReturnsFalse) {
    shard::CrossReactorQueues q;
    shard::ForwardedResponse out;
    EXPECT_FALSE(q.try_pop_response(out));
}

TEST(CrossReactorQueueTest, RequestAndResponseQueuesAreIndependent) {
    shard::CrossReactorQueues q;
    shard::ForwardedRequest req;
    req.client_fd = 1;
    req.source_reactor_id = 0;
    req.request = ApiVersionsRequest{{18, 0, 1}};
    q.push_request(std::move(req));

    shard::ForwardedResponse resp;
    resp.client_fd = 2;
    resp.data = {0xFF};
    q.push_response(std::move(resp));

    shard::ForwardedRequest out_req;
    ASSERT_TRUE(q.try_pop_request(out_req));
    EXPECT_EQ(out_req.client_fd, 1);

    shard::ForwardedResponse out_resp;
    ASSERT_TRUE(q.try_pop_response(out_resp));
    EXPECT_EQ(out_resp.client_fd, 2);
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
