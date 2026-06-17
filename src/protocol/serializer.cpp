#include "protocol/serializer.hpp"

#include <cstring>

#include "util/byte_writer.hpp"
#include "util/overloaded.hpp"
#include "util/varint.hpp"

namespace {

constexpr size_t kResponseHeaderV1Size = 5;

void write_compact_int32_array(ByteWriter& writer, const std::vector<int32_t>& arr) {
    writer.write_varint(static_cast<uint32_t>(arr.size()) + 1);
    for (int32_t v : arr) {
        writer.write_int32(v);
    }
}

size_t compact_int32_array_size(const std::vector<int32_t>& arr) {
    return varint_encoded_size(static_cast<uint32_t>(arr.size()) + 1) + arr.size() * 4;
}

size_t partition_entry_size(const PartitionMetadata& p) {
    return 14 + compact_int32_array_size(p.replica_nodes) + compact_int32_array_size(p.isr_nodes) +
           compact_int32_array_size(p.eligible_leader_replicas) +
           compact_int32_array_size(p.last_known_elr) +
           compact_int32_array_size(p.offline_replicas) + 1;
}

} // namespace

auto serialize(const Response& resp) -> std::vector<std::uint8_t> {
    return std::visit(
        Overloaded{
            [](const ApiVersionsResponse& r) -> std::vector<std::uint8_t> {
                const uint32_t count = static_cast<uint32_t>(r.api_keys.size()) + 1;
                const size_t varint_len = varint_encoded_size(count);
                const size_t body_size = 4 + 2 + varint_len + r.api_keys.size() * 7 + 4 + 1;
                std::vector<uint8_t> buf(4 + body_size);
                ByteWriter writer(buf);

                writer.write_int32(static_cast<int32_t>(body_size));
                writer.write_int32(r.correlation_id);
                writer.write_int16(r.error_code);
                writer.write_varint(count);

                for (const auto& entry : r.api_keys) {
                    writer.write_int16(entry.api_key);
                    writer.write_int16(entry.min_version);
                    writer.write_int16(entry.max_version);
                    writer.write_int8(0x00); // TAG_BUFFER
                }

                writer.write_int32(r.throttle_time_ms);
                writer.write_int8(0x00); // body TAG_BUFFER

                return buf;
            },
            [](const DescribeTopicPartitionsResponse& r) -> std::vector<std::uint8_t> {
                size_t body_size = kResponseHeaderV1Size + 4 + 1;
                for (const auto& t : r.topics) {
                    uint32_t name_varint = static_cast<uint32_t>(t.topic_name.size()) + 1;
                    body_size +=
                        2 + varint_encoded_size(name_varint) + t.topic_name.size() + 16 + 1 + 4 + 1;
                    if (t.partitions.empty()) {
                        body_size += 1;
                    } else {
                        uint32_t part_count = static_cast<uint32_t>(t.partitions.size()) + 1;
                        body_size += varint_encoded_size(part_count);
                        for (const auto& p : t.partitions) {
                            body_size += partition_entry_size(p);
                        }
                    }
                }
                body_size += 2;

                std::vector<uint8_t> buf(4 + body_size);
                ByteWriter writer(buf);

                writer.write_int32(static_cast<int32_t>(body_size));
                writer.write_int32(r.correlation_id);
                writer.write_int8(0x00); // header TAG_BUFFER
                writer.write_int32(r.throttle_time_ms);

                uint32_t topic_count = static_cast<uint32_t>(r.topics.size()) + 1;
                writer.write_varint(topic_count);

                for (const auto& t : r.topics) {
                    writer.write_int16(t.error_code);
                    writer.write_compact_string(t.topic_name);
                    writer.write_bytes(t.topic_id);
                    writer.write_int8(t.is_internal ? 1 : 0);

                    if (t.partitions.empty()) {
                        writer.write_int8(0x01); // compact array, empty
                    } else {
                        writer.write_varint(static_cast<uint32_t>(t.partitions.size()) + 1);
                        for (const auto& p : t.partitions) {
                            writer.write_int16(p.error_code);
                            writer.write_int32(p.partition_index);
                            writer.write_int32(p.leader_id);
                            writer.write_int32(p.leader_epoch);
                            write_compact_int32_array(writer, p.replica_nodes);
                            write_compact_int32_array(writer, p.isr_nodes);
                            write_compact_int32_array(writer, p.eligible_leader_replicas);
                            write_compact_int32_array(writer, p.last_known_elr);
                            write_compact_int32_array(writer, p.offline_replicas);
                            writer.write_int8(0x00); // partition TAG_BUFFER
                        }
                    }

                    writer.write_int32(t.authorized_operations);
                    writer.write_int8(0x00); // topic TAG_BUFFER
                }

                writer.write_int8(0xFF); // next_cursor = null
                writer.write_int8(0x00); // body TAG_BUFFER

                return buf;
            },
            [](const FetchResponse& r) -> std::vector<std::uint8_t> {
                size_t body_size = 4 + 1 + 4 + 2 + 4;
                body_size += varint_encoded_size(static_cast<uint32_t>(r.responses.size()) + 1);
                for (const auto& t : r.responses) {
                    body_size += 16;
                    body_size +=
                        varint_encoded_size(static_cast<uint32_t>(t.partitions.size()) + 1);
                    for (const auto& p : t.partitions) {
                        // partition_index(4) + error_code(2) + hw(8) + lso(8) + lso(8)
                        // + aborted varint(1) + preferred_replica(4) + records + tag(1)
                        body_size += 35;
                        body_size +=
                            varint_encoded_size(static_cast<uint32_t>(p.records.size()) + 1);
                        body_size += p.records.size();
                        body_size += 1; // partition TAG_BUFFER
                    }
                    body_size += 1; // topic TAG_BUFFER
                }
                body_size += 1; // body TAG_BUFFER

                std::vector<uint8_t> buf(4 + body_size);
                ByteWriter writer(buf);

                writer.write_int32(static_cast<int32_t>(body_size));
                writer.write_int32(r.correlation_id);
                writer.write_int8(0x00); // header TAG_BUFFER
                writer.write_int32(r.throttle_time_ms);
                writer.write_int16(r.error_code);
                writer.write_int32(r.session_id);
                writer.write_varint(static_cast<uint32_t>(r.responses.size()) + 1);

                for (const auto& t : r.responses) {
                    writer.write_bytes(t.topic_id);
                    writer.write_varint(static_cast<uint32_t>(t.partitions.size()) + 1);
                    for (const auto& p : t.partitions) {
                        writer.write_int32(p.partition_index);
                        writer.write_int16(p.error_code);
                        writer.write_int64(0);  // high_watermark
                        writer.write_int64(0);  // last_stable_offset
                        writer.write_int64(0);  // log_start_offset
                        writer.write_varint(1); // aborted_transactions: empty
                        writer.write_int32(0);  // preferred_read_replica
                        // records: compact bytes (length+1 as varint)
                        writer.write_varint(static_cast<uint32_t>(p.records.size()) + 1);
                        if (!p.records.empty()) {
                            writer.write_bytes(p.records);
                        }
                        writer.write_int8(0x00); // partition TAG_BUFFER
                    }
                    writer.write_int8(0x00); // topic TAG_BUFFER
                }

                writer.write_int8(0x00); // body TAG_BUFFER
                return buf;
            },
            [](const MetadataResponse& r) -> std::vector<std::uint8_t> {
                size_t body_size = 4 + 1 + 4;

                body_size += varint_encoded_size(static_cast<uint32_t>(r.brokers.size()) + 1);
                for (const auto& b : r.brokers) {
                    uint32_t host_len = static_cast<uint32_t>(b.host.size()) + 1;
                    uint32_t rack_len =
                        b.rack.empty() ? 0 : static_cast<uint32_t>(b.rack.size()) + 1;
                    body_size += 4 + varint_encoded_size(host_len) + b.host.size() + 4 +
                                 varint_encoded_size(rack_len) + b.rack.size() + 1;
                }

                uint32_t cluster_varint =
                    r.cluster_id.empty() ? 0 : static_cast<uint32_t>(r.cluster_id.size()) + 1;
                body_size += varint_encoded_size(cluster_varint) + r.cluster_id.size();
                body_size += 4;

                body_size += varint_encoded_size(static_cast<uint32_t>(r.topics.size()) + 1);
                for (const auto& t : r.topics) {
                    uint32_t name_varint =
                        t.topic_name.empty() ? 0 : static_cast<uint32_t>(t.topic_name.size()) + 1;
                    body_size +=
                        2 + varint_encoded_size(name_varint) + t.topic_name.size() + 16 + 1 + 4;
                    body_size +=
                        varint_encoded_size(static_cast<uint32_t>(t.partitions.size()) + 1);
                    for (const auto& p : t.partitions) {
                        body_size += 2 + 4 + 4 + 4;
                        body_size += compact_int32_array_size(p.replica_nodes);
                        body_size += compact_int32_array_size(p.isr_nodes);
                        body_size += compact_int32_array_size(p.offline_replicas);
                        body_size += 1;
                    }
                    body_size += 1;
                }
                body_size += 4;
                body_size += 1;

                std::vector<uint8_t> buf(4 + body_size);
                ByteWriter writer(buf);

                writer.write_int32(static_cast<int32_t>(body_size));
                writer.write_int32(r.correlation_id);
                writer.write_int8(0x00);
                writer.write_int32(r.throttle_time_ms);

                writer.write_varint(static_cast<uint32_t>(r.brokers.size()) + 1);
                for (const auto& b : r.brokers) {
                    writer.write_int32(b.node_id);
                    writer.write_compact_string(b.host);
                    writer.write_int32(b.port);
                    if (b.rack.empty()) {
                        writer.write_varint(0);
                    } else {
                        writer.write_compact_string(b.rack);
                    }
                    writer.write_int8(0x00);
                }

                if (r.cluster_id.empty()) {
                    writer.write_varint(0);
                } else {
                    writer.write_compact_string(r.cluster_id);
                }
                writer.write_int32(r.controller_id);

                writer.write_varint(static_cast<uint32_t>(r.topics.size()) + 1);
                for (const auto& t : r.topics) {
                    writer.write_int16(t.error_code);
                    if (t.topic_name.empty()) {
                        writer.write_varint(0);
                    } else {
                        writer.write_compact_string(t.topic_name);
                    }
                    writer.write_bytes(t.topic_id);
                    writer.write_int8(t.is_internal ? 1 : 0);
                    writer.write_varint(static_cast<uint32_t>(t.partitions.size()) + 1);
                    for (const auto& p : t.partitions) {
                        writer.write_int16(p.error_code);
                        writer.write_int32(p.partition_index);
                        writer.write_int32(p.leader_id);
                        writer.write_int32(p.leader_epoch);
                        write_compact_int32_array(writer, p.replica_nodes);
                        write_compact_int32_array(writer, p.isr_nodes);
                        write_compact_int32_array(writer, p.offline_replicas);
                        writer.write_int8(0x00);
                    }
                    writer.write_int32(t.topic_authorized_operations);
                    writer.write_int8(0x00);
                }

                writer.write_int32(r.cluster_authorized_operations);
                writer.write_int8(0x00);

                return buf;
            },
            [](const ProduceResponse& r) -> std::vector<std::uint8_t> {
                size_t body_size = 4 + 1;
                body_size += varint_encoded_size(static_cast<uint32_t>(r.responses.size()) + 1);
                for (const auto& t : r.responses) {
                    uint32_t name_varint = static_cast<uint32_t>(t.topic_name.size()) + 1;
                    body_size += varint_encoded_size(name_varint) + t.topic_name.size() + 1;
                    if (t.partitions.empty()) {
                        body_size += 1;
                    } else {
                        uint32_t part_count = static_cast<uint32_t>(t.partitions.size()) + 1;
                        body_size += varint_encoded_size(part_count);
                        body_size += t.partitions.size() * (4 + 2 + 8 + 8 + 8 + 1 + 1 + 1);
                    }
                }
                body_size += 4;
                body_size += 1;

                std::vector<uint8_t> buf(4 + body_size);
                ByteWriter writer(buf);

                writer.write_int32(static_cast<int32_t>(body_size));
                writer.write_int32(r.correlation_id);
                writer.write_int8(0x00); // header TAG_BUFFER

                writer.write_varint(static_cast<uint32_t>(r.responses.size()) + 1);
                for (const auto& t : r.responses) {
                    writer.write_compact_string(t.topic_name);
                    uint32_t part_count = static_cast<uint32_t>(t.partitions.size()) + 1;
                    writer.write_varint(part_count);
                    for (const auto& p : t.partitions) {
                        writer.write_int32(p.partition_index);
                        writer.write_int16(p.error_code);
                        writer.write_int64(p.base_offset);
                        writer.write_int64(p.log_append_time_ms);
                        writer.write_int64(p.log_start_offset);
                        writer.write_int8(0x01); // record_errors: empty (varint 1)
                        writer.write_int8(0x00); // error_message: null (varint 0)
                        writer.write_int8(0x00); // partition tagged_fields
                    }
                    writer.write_int8(0x00); // topic tagged_fields
                }

                writer.write_int32(r.throttle_time_ms);
                writer.write_int8(0x00); // body tagged_fields

                return buf;
            },
        },
        resp);
}
