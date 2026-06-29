#pragma once

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

class GroupCoordinator {
  public:
    GroupCoordinator() = default;

    static auto handle_find_coordinator(const FindCoordinatorRequest& r) -> FindCoordinatorResponse;
    auto handle_join_group(const JoinGroupRequest& r) -> JoinGroupResponse;
    auto handle_sync_group(const SyncGroupRequest& r) -> SyncGroupResponse;
    auto handle_heartbeat(const HeartbeatRequest& r) -> HeartbeatResponse;
    auto handle_offset_commit(const OffsetCommitRequest& r) -> OffsetCommitResponse;
    auto handle_offset_fetch(const OffsetFetchRequest& r) -> OffsetFetchResponse;
  private:
    std::mutex mutex_;
    std::unordered_map<std::string,
                       std::unordered_map<std::string, std::unordered_map<int32_t, int64_t>>>
        group_offsets_;
    std::unordered_map<std::string, std::vector<JoinGroupMember>> group_members_;
    std::unordered_map<std::string, int32_t> group_generations_;
    std::unordered_map<std::string, GroupState> group_states_;
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<uint8_t>>>
        member_assignments_;
    int32_t next_member_id_{0};
};
