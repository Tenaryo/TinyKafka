#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

struct ClusterMetadata {
    struct TopicInfo {
        std::array<uint8_t, 16> uuid{};
        std::vector<int32_t> partitions;
    };

    std::unordered_map<std::string, TopicInfo> topics;
};

auto parse_cluster_metadata(std::span<const uint8_t> data) noexcept
    -> std::expected<ClusterMetadata, std::error_code>;

auto parse_cluster_metadata_file(const std::filesystem::path& path) noexcept -> ClusterMetadata;
