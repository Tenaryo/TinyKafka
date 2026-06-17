#!/usr/bin/env python3
"""Generate a metadata log file with a topic and partition record batch v2."""
import struct
import sys


def push_be16(buf, v):
    buf.extend(struct.pack(">h", v))


def push_be32(buf, v):
    buf.extend(struct.pack(">i", v))


def push_be64(buf, v):
    buf.extend(struct.pack(">q", v))


def push_signed_varint(buf, val):
    encoded = ((val << 1) ^ (val >> 31)) & 0xFFFFFFFF
    while encoded > 0x7F:
        buf.append((encoded & 0x7F) | 0x80)
        encoded >>= 7
    buf.append(encoded & 0x7F)


def push_unsigned_varint(buf, val):
    while val > 0x7F:
        buf.append((val & 0x7F) | 0x80)
        val >>= 7
    buf.append(val & 0x7F)


def make_topic_record(name, uuid):
    v = bytearray()
    push_unsigned_varint(v, 1)  # frame_version
    push_unsigned_varint(v, 2)  # type = topic
    push_unsigned_varint(v, 0)  # version
    push_unsigned_varint(v, len(name) + 1)
    v.extend(name.encode())
    v.extend(uuid)
    v.append(0x00)  # tagged fields
    return v


def make_partition_record(partition_id, uuid):
    v = bytearray()
    push_unsigned_varint(v, 1)  # frame_version
    push_unsigned_varint(v, 3)  # type = partition
    push_unsigned_varint(v, 0)  # version
    push_be32(v, partition_id)
    v.extend(uuid)
    push_unsigned_varint(v, 1)  # replicas
    push_unsigned_varint(v, 1)  # isr
    push_unsigned_varint(v, 1)  # removing
    push_unsigned_varint(v, 1)  # adding
    push_be32(v, 0)  # leader
    push_be32(v, 0)  # leader_epoch
    push_be32(v, 0)  # partition_epoch
    push_unsigned_varint(v, 1)  # directories
    v.append(0x00)  # tagged fields
    return v


def make_record(value):
    r = bytearray()
    body_size = 1 + 1 + 1 + 1 + 1 + len(value) + 1
    push_signed_varint(r, body_size)
    r.append(0x00)  # attributes
    push_signed_varint(r, 0)  # timestamp_delta
    push_signed_varint(r, 0)  # offset_delta
    push_signed_varint(r, -1)  # key_len = null
    push_signed_varint(r, len(value))
    r.extend(value)
    push_signed_varint(r, 0)  # header_count
    return r


def build_record_batch(records):
    buf = bytearray()
    push_be64(buf, 0)  # baseOffset
    batch_len_pos = len(buf)
    push_be32(buf, 0)  # batchLength placeholder
    push_be32(buf, 0)  # leaderEpoch
    buf.append(0x02)  # magic = 2
    push_be32(buf, 0)  # crc
    push_be16(buf, 0)  # attributes
    push_be32(buf, len(records) - 1)  # lastOffsetDelta
    push_be64(buf, 0)  # baseTimestamp
    push_be64(buf, 0)  # maxTimestamp
    push_be64(buf, 0)  # producerId
    push_be16(buf, 0)  # producerEpoch
    push_be32(buf, 0)  # baseSequence

    for value in records:
        rec = make_record(value)
        buf.extend(rec)

    batch_len = len(buf) - batch_len_pos - 4
    struct.pack_into(">i", buf, batch_len_pos, batch_len)
    return buf


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <output_path> [topic_name]", file=sys.stderr)
        sys.exit(1)

    output_path = sys.argv[1]
    topic = sys.argv[2] if len(sys.argv) > 2 else "bench"

    uuid = bytes.fromhex("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6")

    topic_rec = make_topic_record(topic, uuid)
    part_rec = make_partition_record(0, uuid)
    batch = build_record_batch([topic_rec, part_rec])

    with open(output_path, "wb") as f:
        f.write(batch)

    print(f"[setup_metadata] wrote metadata for topic '{topic}' to {output_path}")


if __name__ == "__main__":
    main()
