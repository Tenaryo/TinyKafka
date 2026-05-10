#include "broker/broker.hpp"

#include "protocol/api_registry.hpp"
#include "util/overloaded.hpp"

auto Broker::handle(const Request& req) -> Response {
    return std::visit(overloaded{
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
                          [](const DescribeTopicPartitionsRequest& r) -> Response {
                              std::vector<TopicMetadata> topics;
                              topics.reserve(r.topic_names.size());
                              for (const auto& name : r.topic_names) {
                                  topics.push_back(TopicMetadata{
                                      .error_code = 3,
                                      .topic_name = name,
                                      .topic_id = {},
                                      .is_internal = false,
                                      .authorized_operations = 0,
                                  });
                              }
                              return DescribeTopicPartitionsResponse{
                                  .correlation_id = r.header.correlation_id,
                                  .throttle_time_ms = 0,
                                  .topics = std::move(topics),
                              };
                          },
                      },
                      req);
}
