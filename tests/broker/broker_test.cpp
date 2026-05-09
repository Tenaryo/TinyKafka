#include <cstdint>
#include <gtest/gtest.h>

#include "broker/broker.hpp"

TEST(BrokerTest, HandlesValidVersion) {
    RequestHeader header{18, 0, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker{}.handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.correlation_id, 42);
    EXPECT_EQ(r.error_code, 0);
}

TEST(BrokerTest, HandlesUnsupportedVersion) {
    RequestHeader header{18, 26442, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker{}.handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.correlation_id, 42);
    EXPECT_EQ(r.error_code, 35);
}
