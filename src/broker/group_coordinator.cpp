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

    auto& meta = groups_[r.group_id];
    bool found = false;
    for (const auto& m : meta.members) {
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
        meta.members.push_back({.member_id = member,
                                .protocol_metadata = std::move(metadata),
                                .assignment = {},
                                .last_heartbeat = std::chrono::steady_clock::now()});
    } else {
        for (auto& m : meta.members) {
            if (m.member_id == member) {
                m.last_heartbeat = std::chrono::steady_clock::now();
                break;
            }
        }
    }

    meta.session_timeout_ms = r.session_timeout_ms;

    if (meta.generation == 0) {
        meta.generation = 1;
    } else {
        ++meta.generation;
    }

    meta.state = GroupState::AwaitingSync;

    std::string leader = meta.members[0].member_id;
    std::string proto_name;
    if (!r.protocols.empty()) {
        proto_name = r.protocols[0].name;
    }

    std::vector<JoinGroupMember> response_members;
    response_members.reserve(meta.members.size());
    for (const auto& m : meta.members) {
        response_members.push_back({.member_id = m.member_id, .metadata = m.protocol_metadata});
    }

    return JoinGroupResponse{
        .correlation_id = r.header.correlation_id,
        .throttle_time_ms = 0,
        .error_code = 0,
        .generation_id = meta.generation,
        .protocol_name = proto_name,
        .leader = leader,
        .member_id = member,
        .members = response_members,
    };
}

auto GroupCoordinator::handle_sync_group(const SyncGroupRequest& r) -> SyncGroupResponse {
    std::lock_guard lock(mutex_);

    int16_t error_code = 0;
    std::vector<uint8_t> assignment;

    auto it = groups_.find(r.group_id);
    if (it == groups_.end() || it->second.generation != r.generation_id) {
        error_code = 82;
    } else if (!r.assignments.empty()) {
        auto& meta = it->second;
        for (const auto& a : r.assignments) {
            bool updated = false;
            for (auto& m : meta.members) {
                if (m.member_id == a.member_id) {
                    m.assignment = a.assignment;
                    updated = true;
                    break;
                }
            }
            if (!updated) {
                meta.members.push_back({.member_id = a.member_id,
                                        .protocol_metadata = {},
                                        .assignment = a.assignment,
                                        .last_heartbeat = std::chrono::steady_clock::now()});
            }
        }
        meta.state = GroupState::Stable;
        for (const auto& m : meta.members) {
            if (m.member_id == r.member_id) {
                assignment = m.assignment;
                break;
            }
        }
    } else {
        auto& meta = it->second;
        for (const auto& m : meta.members) {
            if (m.member_id == r.member_id) {
                assignment = m.assignment;
                break;
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
    auto it = groups_.find(r.group_id);
    if (it == groups_.end() || it->second.generation != r.generation_id) {
        error_code = 82;
    }

    if (error_code == 0 && (it == groups_.end() || it->second.state != GroupState::Stable)) {
        error_code = 82;
    }

    if (error_code == 0) {
        auto now = std::chrono::steady_clock::now();
        auto& meta = it->second;

        for (auto& m : meta.members) {
            if (m.member_id == r.member_id) {
                m.last_heartbeat = now;
                break;
            }
        }

        auto timeout = std::chrono::milliseconds(meta.session_timeout_ms);
        auto old_size = meta.members.size();
        std::erase_if(meta.members,
                      [now, timeout](const auto& m) { return now - m.last_heartbeat > timeout; });

        if (meta.members.empty()) {
            meta.state = GroupState::Empty;
        } else if (meta.members.size() < old_size) {
            meta.state = GroupState::AwaitingSync;
            ++meta.generation;
        }
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
