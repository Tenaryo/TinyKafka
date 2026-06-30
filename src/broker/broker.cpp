#include "broker/broker.hpp"

#include "protocol/response.hpp"
#include "util/overloaded.hpp"

auto Broker::handle(const Request& req) -> net::Task<Response> {
    return std::visit(
        Overloaded{
            [this](const ApiVersionsRequest& r) -> net::Task<Response> {
                co_return metadata_handler_.handle_api_versions(r);
            },
            [](const FindCoordinatorRequest& r) -> net::Task<Response> {
                co_return GroupCoordinator::handle_find_coordinator(r);
            },
            [this](const OffsetCommitRequest& r) -> net::Task<Response> {
                co_return coordinator_.handle_offset_commit(r);
            },
            [this](const OffsetFetchRequest& r) -> net::Task<Response> {
                co_return coordinator_.handle_offset_fetch(r);
            },
            [this](const JoinGroupRequest& r) -> net::Task<Response> {
                co_return coordinator_.handle_join_group(r);
            },
            [this](const HeartbeatRequest& r) -> net::Task<Response> {
                co_return coordinator_.handle_heartbeat(r);
            },
            [this](const LeaveGroupRequest& r) -> net::Task<Response> {
                co_return coordinator_.handle_leave_group(r);
            },
            [this](const SyncGroupRequest& r) -> net::Task<Response> {
                co_return coordinator_.handle_sync_group(r);
            },
            [this](const MetadataRequest& r) -> net::Task<Response> {
                co_return metadata_handler_.handle_metadata(r);
            },
            [this](const ListOffsetsRequest& r) -> net::Task<Response> {
                co_return record_handler_.handle_list_offsets(r);
            },
            [this](const DescribeTopicPartitionsRequest& r) -> net::Task<Response> {
                co_return metadata_handler_.handle_describe_topic_partitions(r);
            },
            [this](const ProduceRequest& r) -> net::Task<Response> {
                co_return co_await record_handler_.handle_produce(r);
            },
            [this](const FetchRequest& r) -> net::Task<Response> {
                co_return record_handler_.handle_fetch(r);
            },
        },
        req);
}
