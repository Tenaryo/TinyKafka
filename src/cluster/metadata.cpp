#include "cluster/metadata.hpp"

#include <fstream>
#include <vector>

#include "util/byte_reader.hpp"
#include "util/endian.hpp"

namespace {

constexpr uint32_t kTopicRecordKey = 2;
constexpr uint32_t kPartitionRecordKey = 3;
constexpr size_t kRecordBatchHeaderSize = 61;
constexpr size_t kUuidSize = 16;

struct RawTopicRecord {
    std::string name;
    std::array<uint8_t, kUuidSize> uuid{};
};

struct RawPartitionRecord {
    int32_t partition_id = 0;
    std::array<uint8_t, kUuidSize> uuid{};
};

} // namespace

auto parse_cluster_metadata(std::span<const uint8_t> data)
    -> std::expected<ClusterMetadata, std::error_code> {
    ByteReader reader(data);

    std::vector<RawTopicRecord> topic_records;
    std::vector<RawPartitionRecord> partition_records;

    while (reader.remaining() >= kRecordBatchHeaderSize) {
        auto skip_base = reader.skip(8);
        if (!skip_base) {
            break;
        }

        auto batch_len = reader.read_int32();
        if (!batch_len || *batch_len < 0) {
            break;
        }
        const auto batch_body_size = static_cast<size_t>(*batch_len);
        size_t batch_end = reader.offset() + batch_body_size;

        auto skip_leader_epoch = reader.skip(4);
        if (!skip_leader_epoch) {
            break;
        }
        auto magic_result = reader.read_int8();
        if (!magic_result || *magic_result != 2) {
            break;
        }
        auto skip_crc = reader.skip(4);
        if (!skip_crc) {
            break;
        }
        auto skip_attrs = reader.skip(2);
        if (!skip_attrs) {
            break;
        }
        auto skip_offset_delta = reader.skip(4);
        if (!skip_offset_delta) {
            break;
        }
        auto skip_base_ts = reader.skip(8);
        if (!skip_base_ts) {
            break;
        }
        auto skip_max_ts = reader.skip(8);
        if (!skip_max_ts) {
            break;
        }
        auto skip_pid = reader.skip(8);
        if (!skip_pid) {
            break;
        }
        auto skip_epoch = reader.skip(2);
        if (!skip_epoch) {
            break;
        }
        auto skip_seq = reader.skip(4);
        if (!skip_seq) {
            break;
        }

        auto record_count = reader.read_int32();
        if (!record_count || *record_count < 0) {
            break;
        }

        for (int32_t i = 0; i < *record_count; ++i) {
            if (reader.offset() >= batch_end) {
                break;
            }

            auto rec_len = reader.read_signed_varint();
            if (!rec_len || *rec_len < 0) {
                continue;
            }
            size_t rec_end = reader.offset() + static_cast<size_t>(*rec_len);

            auto attr_result = reader.read_int8();
            if (!attr_result) {
                continue;
            }
            auto skip_ts = reader.skip_varint();
            if (!skip_ts) {
                continue;
            }
            auto skip_od = reader.skip_varint();
            if (!skip_od) {
                continue;
            }

            auto key_len = reader.read_signed_varint();
            if (!key_len) {
                continue;
            }
            if (*key_len > 0) {
                auto skip_key = reader.skip(static_cast<size_t>(*key_len));
                if (!skip_key) {
                    continue;
                }
            }

            auto value_len = reader.read_signed_varint();
            if (!value_len) {
                continue;
            }
            if (*value_len < 3) {
                [[maybe_unused]] auto hc = reader.read_signed_varint();
                continue;
            }

            const auto vlen = static_cast<size_t>(*value_len);
            auto value_bytes = reader.read_bytes(vlen);
            if (!value_bytes) {
                continue;
            }

            auto val_span = *value_bytes;
            if (val_span.size() < 3) {
                [[maybe_unused]] auto hc = reader.read_signed_varint();
                continue;
            }

            ByteReader val_reader(val_span);
            auto val_frame_ver = val_reader.read_varint();
            if (!val_frame_ver) {
                continue;
            }
            auto val_api_key = val_reader.read_varint();
            if (!val_api_key) {
                continue;
            }
            auto val_version = val_reader.read_varint();
            if (!val_version) {
                continue;
            }

            if (*val_api_key == kTopicRecordKey) {
                auto name_result = val_reader.read_compact_string();
                if (!name_result) {
                    continue;
                }
                auto uuid_result = val_reader.read_bytes(kUuidSize);
                if (!uuid_result) {
                    continue;
                }

                RawTopicRecord rec;
                rec.name = std::move(*name_result);
                std::memcpy(rec.uuid.data(), uuid_result->data(), kUuidSize);
                topic_records.push_back(std::move(rec));
            } else if (*val_api_key == kPartitionRecordKey) {
                if (val_reader.remaining() < 4 + kUuidSize) {
                    continue;
                }
                auto pid_result = val_reader.read_int32();
                if (!pid_result) {
                    continue;
                }
                auto uuid_result = val_reader.read_bytes(kUuidSize);
                if (!uuid_result) {
                    continue;
                }

                RawPartitionRecord rec;
                rec.partition_id = *pid_result;
                std::memcpy(rec.uuid.data(), uuid_result->data(), kUuidSize);
                partition_records.push_back(rec);
            }

            auto header_count = reader.read_signed_varint();
            for (int32_t h = 0; h < *header_count; ++h) {
                auto hdr_key_len = reader.read_signed_varint();
                if (!hdr_key_len) {
                    continue;
                }
                if (*hdr_key_len > 0) {
                    auto skip_hdr_key = reader.skip(static_cast<size_t>(*hdr_key_len));
                    if (!skip_hdr_key) {
                        continue;
                    }
                }
                auto hdr_val_len = reader.read_signed_varint();
                if (!hdr_val_len) {
                    continue;
                }
                if (*hdr_val_len > 0) {
                    auto skip_hdr_val = reader.skip(static_cast<size_t>(*hdr_val_len));
                    if (!skip_hdr_val) {
                        continue;
                    }
                }
            }

            if (reader.offset() < rec_end) {
                auto skip_rest = reader.skip(rec_end - reader.offset());
                if (!skip_rest) {
                    continue;
                }
            }
        }
    }

    ClusterMetadata meta;
    for (auto& tr : topic_records) {
        meta.name_to_topic[tr.name] = meta.topics.size();
        meta.uuid_to_topic[tr.uuid] = meta.topics.size();
        meta.topics.push_back({.name = std::move(tr.name), .uuid = tr.uuid, .partitions = {}});
    }
    for (auto& pr : partition_records) {
        auto it = meta.uuid_to_topic.find(pr.uuid);
        if (it != meta.uuid_to_topic.end()) {
            meta.topics[it->second].partitions.push_back(pr.partition_id);
        }
    }

    return meta;
}

auto parse_cluster_metadata_file(const std::filesystem::path& path) -> ClusterMetadata {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        return {};
    }

    auto size = file.tellg();
    if (size <= 0) {
        return {};
    }

    file.seekg(0);
    std::vector<uint8_t> data(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(data.data()), size);

    auto result = parse_cluster_metadata(data);
    if (!result) {
        return {};
    }

    return std::move(*result);
}
