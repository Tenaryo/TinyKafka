#include "broker/broker.hpp"

#include <algorithm>

#include "protocol/api_registry.hpp"
#include "protocol/response.hpp"
#include "util/overloaded.hpp"

auto Broker::build_topic_metadata(const std::string& topic_name) const -> TopicMetadata {
    auto it = metadata_.name_to_topic.find(topic_name);
    if (it == metadata_.name_to_topic.end()) {
        return TopicMetadata{
            .error_code = 3,
            .topic_name = topic_name,
            .topic_id = {},
            .is_internal = false,
            .authorized_operations = 0,
            .partitions = {},
        };
    }

    const auto& info = metadata_.topics[it->second];
    std::vector<PartitionMetadata> partitions;
    partitions.reserve(info.partitions.size());
    for (auto pid : info.partitions) {
        partitions.push_back(PartitionMetadata{
            .error_code = 0,
            .partition_index = pid,
            .leader_id = 1,
            .leader_epoch = 0,
            .replica_nodes = {1},
            .isr_nodes = {1},
            .eligible_leader_replicas = {},
            .last_known_elr = {},
            .offline_replicas = {},
        });
    }

    return TopicMetadata{
        .error_code = 0,
        .topic_name = topic_name,
        .topic_id = info.uuid,
        .is_internal = false,
        .authorized_operations = 0,
        .partitions = std::move(partitions),
    };
}

auto Broker::find_topic_by_uuid(const std::array<std::uint8_t, 16>& id) const
    -> const ClusterMetadata::TopicInfo* {
    auto it = metadata_.uuid_to_topic.find(id);
    if (it == metadata_.uuid_to_topic.end()) {
        return nullptr;
    }
    return &metadata_.topics[it->second];
}

auto Broker::find_topic_by_name(const std::string& name) const
    -> const ClusterMetadata::TopicInfo* {
    auto it = metadata_.name_to_topic.find(name);
    if (it == metadata_.name_to_topic.end()) {
        return nullptr;
    }
    return &metadata_.topics[it->second];
}

auto Broker::get_or_create_context(const std::string& topic_name,
                                   int32_t partition) -> broker::PartitionContext& {
    auto key = topic_name + ":" + std::to_string(partition);
    std::lock_guard lock(contexts_mutex_);
    auto [it, inserted] = partition_contexts_.try_emplace(
        key, std::make_unique<broker::PartitionContext>(log_root_, topic_name, partition));
    return *it->second;
}

auto Broker::handle(const Request& req) -> Response {
    return std::visit(
        Overloaded{
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
            [](const FindCoordinatorRequest& r) -> Response {
                return FindCoordinatorResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .error_code = 0,
                    .error_message = {},
                    .node_id = 1,
                    .host = "localhost",
                    .port = 9092,
                };
            },
            [this](const OffsetCommitRequest& r) -> Response {
                std::vector<OffsetCommitTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());

                std::lock_guard lock(contexts_mutex_);
                for (const auto& topic_req : r.topics) {
                    std::vector<OffsetCommitPartitionResponse> parts;
                    parts.reserve(topic_req.partitions.size());
                    for (const auto& part_req : topic_req.partitions) {
                        group_offsets_[r.group_id][topic_req.topic_name][part_req.partition_index] =
                            part_req.committed_offset;
                        parts.push_back(
                            {.partition_index = part_req.partition_index, .error_code = 0});
                    }
                    topic_responses.push_back(
                        {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
                }

                return OffsetCommitResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .topics = std::move(topic_responses),
                };
            },
            [this](const OffsetFetchRequest& r) -> Response {
                std::vector<OffsetFetchTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());

                std::lock_guard lock(contexts_mutex_);
                for (const auto& topic_req : r.topics) {
                    std::vector<OffsetFetchPartitionResponse> parts;
                    parts.reserve(topic_req.partition_indexes.size());
                    for (auto partition_idx : topic_req.partition_indexes) {
                        int64_t offset = -1;
                        auto group_it = group_offsets_.find(r.group_id);
                        if (group_it != group_offsets_.end()) {
                            auto topic_it = group_it->second.find(topic_req.topic_name);
                            if (topic_it != group_it->second.end()) {
                                auto part_it = topic_it->second.find(partition_idx);
                                if (part_it != topic_it->second.end()) {
                                    offset = part_it->second;
                                }
                            }
                        }
                        parts.push_back({.partition_index = partition_idx,
                                         .committed_offset = offset,
                                         .error_code = 0});
                    }
                    topic_responses.push_back(
                        {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
                }

                return OffsetFetchResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .topics = std::move(topic_responses),
                };
            },
            [this](const JoinGroupRequest& r) -> Response {
                std::lock_guard lock(contexts_mutex_);

                std::string member = r.member_id;
                if (member.empty()) {
                    member = "member-" + std::to_string(++next_member_id_);
                }

                auto& members = group_members_[r.group_id];
                bool found = false;
                for (const auto& m : members) {
                    if (m.member_id == member) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    std::vector<uint8_t> metadata;
                    if (!r.protocols.empty()) {
                        metadata = r.protocols[0].metadata;
                    }
                    members.push_back({.member_id = member, .metadata = std::move(metadata)});
                }

                auto& gen = group_generations_[r.group_id];
                if (gen == 0) {
                    gen = 1;
                } else {
                    ++gen;
                }

                std::string leader = members[0].member_id;
                std::string proto_name;
                if (!r.protocols.empty()) {
                    proto_name = r.protocols[0].name;
                }

                return JoinGroupResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .error_code = 0,
                    .generation_id = gen,
                    .protocol_name = proto_name,
                    .leader = leader,
                    .member_id = member,
                    .members = members,
                };
            },
            [this](const SyncGroupRequest& r) -> Response {
                std::lock_guard lock(contexts_mutex_);

                int16_t error_code = 0;
                std::vector<uint8_t> assignment;

                auto gen_it = group_generations_.find(r.group_id);
                if (gen_it == group_generations_.end() || gen_it->second != r.generation_id) {
                    error_code = 82;
                } else if (!r.assignments.empty()) {
                    for (const auto& a : r.assignments) {
                        member_assignments_[r.group_id][a.member_id] = a.assignment;
                    }
                } else {
                    auto assign_it = member_assignments_.find(r.group_id);
                    if (assign_it != member_assignments_.end()) {
                        auto mem_it = assign_it->second.find(r.member_id);
                        if (mem_it != assign_it->second.end()) {
                            assignment = mem_it->second;
                        }
                    }
                }

                return SyncGroupResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .error_code = error_code,
                    .protocol_type = {},
                    .protocol_name = {},
                    .assignment = std::move(assignment),
                };
            },
            [this](const MetadataRequest& r) -> Response {
                std::vector<MetadataTopicResponse> topics;
                if (r.topics.empty()) {
                    for (const auto& info : metadata_.topics) {
                        auto tm = build_topic_metadata(info.name);
                        topics.push_back(MetadataTopicResponse{
                            .error_code = tm.error_code,
                            .topic_name = tm.topic_name,
                            .topic_id = tm.topic_id,
                            .is_internal = tm.is_internal,
                            .partitions = std::move(tm.partitions),
                            .topic_authorized_operations = tm.authorized_operations,
                        });
                    }
                } else {
                    for (const auto& name : r.topics) {
                        auto tm = build_topic_metadata(name);
                        topics.push_back(MetadataTopicResponse{
                            .error_code = tm.error_code,
                            .topic_name = tm.topic_name,
                            .topic_id = tm.topic_id,
                            .is_internal = tm.is_internal,
                            .partitions = std::move(tm.partitions),
                            .topic_authorized_operations = tm.authorized_operations,
                        });
                    }
                }
                return MetadataResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .brokers = {{.node_id = 1, .host = "localhost", .port = 9092, .rack = {}}},
                    .cluster_id = "TinyKafka",
                    .controller_id = 1,
                    .topics = std::move(topics),
                    .cluster_authorized_operations = 0,
                };
            },
            [this](const ListOffsetsRequest& r) -> Response {
                std::vector<ListOffsetsTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());
                for (const auto& topic_req : r.topics) {
                    const auto* info = find_topic_by_name(topic_req.topic_name);
                    std::vector<ListOffsetsPartitionResponse> parts;
                    parts.reserve(topic_req.partitions.size());
                    for (const auto& part_req : topic_req.partitions) {
                        int16_t error_code = 0;
                        int64_t offset = -1;
                        if (info && std::ranges::find(info->partitions, part_req.partition_index) !=
                                        info->partitions.end()) {
                            if (part_req.timestamp == -2) {
                                offset = 0;
                            } else if (part_req.timestamp == -1) {
                                auto& ctx = get_or_create_context(topic_req.topic_name,
                                                                  part_req.partition_index);
                                offset = ctx.current_offset();
                            } else {
                                error_code = -1;
                            }
                        } else {
                            error_code = 3;
                        }
                        parts.push_back({.partition_index = part_req.partition_index,
                                         .error_code = error_code,
                                         .offset = offset,
                                         .timestamp = -1});
                    }
                    topic_responses.push_back(
                        {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
                }
                return ListOffsetsResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .topics = std::move(topic_responses),
                };
            },
            [this](const DescribeTopicPartitionsRequest& r) -> Response {
                std::vector<TopicMetadata> topics;
                topics.reserve(r.topic_names.size());
                for (const auto& name : r.topic_names) {
                    topics.push_back(build_topic_metadata(name));
                }
                std::ranges::sort(topics, {}, &TopicMetadata::topic_name);
                return DescribeTopicPartitionsResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .topics = std::move(topics),
                };
            },
            [this](const ProduceRequest& r) -> Response {
                std::vector<ProduceTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());
                for (const auto& topic_req : r.topics) {
                    const auto* info = find_topic_by_name(topic_req.topic_name);
                    std::vector<ProducePartitionResponse> parts;
                    parts.reserve(topic_req.partitions.size());
                    for (const auto& part_req : topic_req.partitions) {
                        if (info && std::ranges::find(info->partitions, part_req.partition_index) !=
                                        info->partitions.end()) {
                            int16_t error_code = 0;
                            int64_t base_offset = 0;
                            int64_t log_append_time_ms = -1;
                            int64_t log_start_offset = 0;
                            if (!part_req.records.empty()) {
                                auto& ctx = get_or_create_context(topic_req.topic_name,
                                                                  part_req.partition_index);
                                auto result = ctx.produce(part_req.records);
                                base_offset = result.base_offset;
                                log_append_time_ms = result.log_append_time_ms;
                                if (base_offset < 0) {
                                    error_code = 56;
                                    log_start_offset = -1;
                                }
                            }
                            parts.push_back(ProducePartitionResponse{
                                .partition_index = part_req.partition_index,
                                .error_code = error_code,
                                .base_offset = base_offset,
                                .log_append_time_ms = log_append_time_ms,
                                .log_start_offset = log_start_offset,
                            });
                        } else {
                            parts.push_back(ProducePartitionResponse{
                                .partition_index = part_req.partition_index,
                                .error_code = 3,
                                .base_offset = -1,
                                .log_append_time_ms = -1,
                                .log_start_offset = -1,
                            });
                        }
                    }
                    topic_responses.push_back(
                        {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
                }
                return ProduceResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .responses = std::move(topic_responses),
                };
            },
            [this](const FetchRequest& r) -> Response {
                std::vector<FetchTopicResponse> topic_responses;
                topic_responses.reserve(r.topics.size());
                for (const auto& topic_req : r.topics) {
                    const auto* info = find_topic_by_uuid(topic_req.topic_id);
                    if (info) {
                        std::vector<FetchPartitionResponse> parts;
                        parts.reserve(topic_req.partitions.size());
                        for (const auto& part_req : topic_req.partitions) {
                            auto& ctx = get_or_create_context(info->name, part_req.partition_index);
                            parts.push_back({.partition_index = part_req.partition_index,
                                             .error_code = 0,
                                             .records = ctx.fetch()});
                        }
                        topic_responses.push_back(
                            {.topic_id = topic_req.topic_id, .partitions = std::move(parts)});
                    } else {
                        topic_responses.push_back(FetchTopicResponse{
                            .topic_id = topic_req.topic_id,
                            .partitions =
                                {
                                    FetchPartitionResponse{
                                        .partition_index = 0,
                                        .error_code = 100,
                                        .records = {},
                                    },
                                },
                        });
                    }
                }
                return FetchResponse{
                    .correlation_id = r.header.correlation_id,
                    .throttle_time_ms = 0,
                    .error_code = 0,
                    .session_id = 0,
                    .responses = std::move(topic_responses),
                };
            },
        },
        req);
}
