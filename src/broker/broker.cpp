#include "broker/broker.hpp"

#include "util/overloaded.hpp"

auto Broker::handle(const Request& req) -> Response {
    return std::visit(
        overloaded{
            [](const ApiVersionsRequest& r) -> Response {
                int16_t error_code =
                    (r.header.api_version >= 0 && r.header.api_version <= 4) ? 0 : 35;
                return ApiVersionsResponse{r.header.correlation_id, error_code};
            },
        },
        req);
}
