#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <expected>
#include <filesystem>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

struct UuidHash {
    auto operator()(const std::array<uint8_t, 16>& uuid) const noexcept -> size_t {
        uint64_t v = 0;
        std::memcpy(&v, uuid.data(), sizeof(v));
        return std::hash<uint64_t>{}(v);
    }
};

struct ClusterMetadata {
    struct TopicInfo {
        std::string name;
        std::array<uint8_t, 16> uuid{};
        std::vector<int32_t> partitions;
    };

    std::vector<TopicInfo> topics;
    std::unordered_map<std::string, size_t> name_to_topic;
    std::unordered_map<std::array<uint8_t, 16>, size_t, UuidHash> uuid_to_topic;
};

auto parse_cluster_metadata(std::span<const uint8_t> data)
    -> std::expected<ClusterMetadata, std::error_code>;

auto parse_cluster_metadata_file(const std::filesystem::path& path) -> ClusterMetadata;
