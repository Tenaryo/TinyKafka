#include "broker/group_coordinator.hpp"

auto GroupCoordinator::handle_find_coordinator(const FindCoordinatorRequest& r)
    -> FindCoordinatorResponse {
    return FindCoordinatorResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .error_code = 0,
        .error_message = {},
        .node_id = 1,
        .host = "localhost",
        .port = 9092,
    };
}

auto GroupCoordinator::handle_join_group(const JoinGroupRequest& r) -> JoinGroupResponse {
    std::lock_guard lock(mutex_);

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

    group_states_[r.group_id] = GroupState::AwaitingSync;

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
}

auto GroupCoordinator::handle_sync_group(const SyncGroupRequest& r) -> SyncGroupResponse {
    std::lock_guard lock(mutex_);

    int16_t error_code = 0;
    std::vector<uint8_t> assignment;

    auto gen_it = group_generations_.find(r.group_id);
    if (gen_it == group_generations_.end() || gen_it->second != r.generation_id) {
        error_code = 82;
    } else if (!r.assignments.empty()) {
        for (const auto& a : r.assignments) {
            member_assignments_[r.group_id][a.member_id] = a.assignment;
        }
        group_states_[r.group_id] = GroupState::Stable;
        auto self_it = member_assignments_[r.group_id].find(r.member_id);
        if (self_it != member_assignments_[r.group_id].end()) {
            assignment = self_it->second;
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
}

auto GroupCoordinator::handle_heartbeat(const HeartbeatRequest& r) -> HeartbeatResponse {
    std::lock_guard lock(mutex_);

    int16_t error_code = 0;
    auto gen_it = group_generations_.find(r.group_id);
    if (gen_it == group_generations_.end() || gen_it->second != r.generation_id) {
        error_code = 82;
    }

    auto state_it = group_states_.find(r.group_id);
    if (error_code == 0 &&
        (state_it == group_states_.end() || state_it->second != GroupState::Stable)) {
        error_code = 82;
    }

    return HeartbeatResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .error_code = error_code,
    };
}

auto GroupCoordinator::handle_offset_commit(const OffsetCommitRequest& r) -> OffsetCommitResponse {
    std::lock_guard lock(mutex_);

    std::vector<OffsetCommitTopicResponse> topic_responses;
    topic_responses.reserve(r.topics.size());

    for (const auto& topic_req : r.topics) {
        std::vector<OffsetCommitPartitionResponse> parts;
        parts.reserve(topic_req.partitions.size());
        for (const auto& part_req : topic_req.partitions) {
            group_offsets_[r.group_id][topic_req.topic_name][part_req.partition_index] =
                part_req.committed_offset;
            parts.push_back({.partition_index = part_req.partition_index, .error_code = 0});
        }
        topic_responses.push_back(
            {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
    }

    return OffsetCommitResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .topics = std::move(topic_responses),
    };
}

auto GroupCoordinator::handle_offset_fetch(const OffsetFetchRequest& r) -> OffsetFetchResponse {
    std::lock_guard lock(mutex_);

    std::vector<OffsetFetchTopicResponse> topic_responses;
    topic_responses.reserve(r.topics.size());

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
            parts.push_back(
                {.partition_index = partition_idx, .committed_offset = offset, .error_code = 0});
        }
        topic_responses.push_back(
            {.topic_name = topic_req.topic_name, .partitions = std::move(parts)});
    }

    return OffsetFetchResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .topics = std::move(topic_responses),
    };
}
