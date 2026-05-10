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

TEST(BrokerTest, ReturnsApiKeysForValidVersion) {
    RequestHeader header{18, 4, 42};
    ApiVersionsRequest req{header};

    auto resp = Broker{}.handle(req);
    auto& r = std::get<ApiVersionsResponse>(resp);
    EXPECT_EQ(r.error_code, 0);
    ASSERT_FALSE(r.api_keys.empty());
    auto it18 = std::ranges::find_if(r.api_keys, [](const auto& e) {
        return e.api_key == 18 && e.min_version == 0 && e.max_version == 4;
    });
    EXPECT_NE(it18, r.api_keys.end());
    auto it75 = std::ranges::find_if(r.api_keys, [](const auto& e) {
        return e.api_key == 75 && e.min_version == 0 && e.max_version == 0;
    });
    EXPECT_NE(it75, r.api_keys.end());
    EXPECT_EQ(r.throttle_time_ms, 0);
}
