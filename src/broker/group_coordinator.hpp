#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "protocol/request.hpp"
#include "protocol/response.hpp"

enum class GroupState : uint8_t {
    Empty,
    AwaitingSync,
    Stable,
    Dead,
};

struct MemberInfo {
    std::string member_id;
    std::vector<uint8_t> protocol_metadata;
    std::vector<uint8_t> assignment;
    std::chrono::steady_clock::time_point last_heartbeat;
};

struct GroupMetadata {
    GroupState state = GroupState::Empty;
    int32_t generation = 0;
    int32_t session_timeout_ms = 30000;
    std::vector<MemberInfo> members;
    std::unordered_map<std::string, std::unordered_map<int32_t, int64_t>> committed_offsets;
};

class GroupCoordinator {
  public:
    GroupCoordinator() = default;

    static auto handle_find_coordinator(const FindCoordinatorRequest& r) -> FindCoordinatorResponse;
    auto handle_join_group(const JoinGroupRequest& r) -> JoinGroupResponse;
    auto handle_sync_group(const SyncGroupRequest& r) -> SyncGroupResponse;
    auto handle_heartbeat(const HeartbeatRequest& r) -> HeartbeatResponse;
    auto handle_leave_group(const LeaveGroupRequest& r) -> LeaveGroupResponse;
    auto handle_offset_commit(const OffsetCommitRequest& r) -> OffsetCommitResponse;
    auto handle_offset_fetch(const OffsetFetchRequest& r) -> OffsetFetchResponse;
  private:
    std::mutex mutex_;
    std::unordered_map<std::string, GroupMetadata> groups_;
    int32_t next_member_id_{0};
};
