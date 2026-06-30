#include "broker/broker.hpp"

#include "protocol/response.hpp"
#include "util/overloaded.hpp"

auto Broker::handle(const Request& req) -> Response {
    return std::visit(
        Overloaded{
            [this](const ApiVersionsRequest& r) -> Response {
                return metadata_handler_.handle_api_versions(r);
            },
            [](const FindCoordinatorRequest& r) -> Response {
                return GroupCoordinator::handle_find_coordinator(r);
            },
            [this](const OffsetCommitRequest& r) -> Response {
                return coordinator_.handle_offset_commit(r);
            },
            [this](const OffsetFetchRequest& r) -> Response {
                return coordinator_.handle_offset_fetch(r);
            },
            [this](const JoinGroupRequest& r) -> Response {
                return coordinator_.handle_join_group(r);
            },
            [this](const HeartbeatRequest& r) -> Response {
                return coordinator_.handle_heartbeat(r);
            },
            [this](const LeaveGroupRequest& r) -> Response {
                return coordinator_.handle_leave_group(r);
            },
            [this](const SyncGroupRequest& r) -> Response {
                return coordinator_.handle_sync_group(r);
            },
            [this](const MetadataRequest& r) -> Response {
                return metadata_handler_.handle_metadata(r);
            },
            [this](const ListOffsetsRequest& r) -> Response {
                return record_handler_.handle_list_offsets(r);
            },
            [this](const DescribeTopicPartitionsRequest& r) -> Response {
                return metadata_handler_.handle_describe_topic_partitions(r);
            },
            [this](const ProduceRequest& r) -> Response {
                return record_handler_.handle_produce(r);
            },
            [this](const FetchRequest& r) -> Response { return record_handler_.handle_fetch(r); },
        },
        req);
}
