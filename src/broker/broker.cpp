#include "broker/broker.hpp"

#include "protocol/api_registry.hpp"
#include "util/overloaded.hpp"

auto Broker::handle(const Request& req) -> Response {
    return std::visit(
        overloaded{
            [](const ApiVersionsRequest& r) -> Response {
                int16_t error_code =
                    (r.header.api_version >= 0 && r.header.api_version <= 4) ? 0 : 35;
                return ApiVersionsResponse{
                    .correlation_id = r.header.correlation_id,
                    .error_code = error_code,
                    .api_keys = {kSupportedApis.begin(), kSupportedApis.end()},
                    .throttle_time_ms = 0,
                };
            },
        },
        req);
}
