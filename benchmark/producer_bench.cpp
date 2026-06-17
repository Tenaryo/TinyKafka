#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <iostream>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

struct Config {
    int messages = 10000;
    int size = 100;
    int batch_size = 1;
    std::string topic = "bench";
    std::string csv_path;
    uint16_t port = 9092;
};

void push_be16(std::vector<uint8_t>& buf, int16_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

void push_be32(std::vector<uint8_t>& buf, int32_t v) {
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(v & 0xFF));
}

void push_be64(std::vector<uint8_t>& buf, int64_t v) {
    for (int i = 7; i >= 0; --i)
        buf.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
}

void push_signed_varint(std::vector<uint8_t>& buf, int32_t val) {
    uint32_t encoded =
        static_cast<uint32_t>((static_cast<uint32_t>(val) << 1) ^ static_cast<uint32_t>(val >> 31));
    while (encoded > 0x7F) {
        buf.push_back(static_cast<uint8_t>((encoded & 0x7F) | 0x80));
        encoded >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(encoded & 0x7F));
}

void push_unsigned_varint(std::vector<uint8_t>& buf, uint32_t val) {
    while (val > 0x7F) {
        buf.push_back(static_cast<uint8_t>((val & 0x7F) | 0x80));
        val >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(val & 0x7F));
}

auto make_record_batch(const std::vector<uint8_t>& value) -> std::vector<uint8_t> {
    auto now = std::chrono::system_clock::now();
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    std::vector<uint8_t> buf;
    push_be64(buf, 0); // baseOffset
    size_t batch_len_pos = buf.size();
    push_be32(buf, 0);   // batchLength placeholder
    push_be32(buf, 0);   // leaderEpoch
    buf.push_back(0x02); // magic = 2
    push_be32(buf, 0);   // crc
    push_be16(buf, 0);   // attributes
    push_be32(buf, 0);   // lastOffsetDelta
    push_be64(buf, ts);  // baseTimestamp
    push_be64(buf, ts);  // maxTimestamp
    push_be64(buf, 0);   // producerId
    push_be16(buf, 0);   // producerEpoch
    push_be32(buf, 0);   // baseSequence

    int32_t body_size = 1 + 1 + 1 + 1 + 1 + static_cast<int32_t>(value.size()) + 1;
    push_signed_varint(buf, body_size);
    buf.push_back(0x00);         // attributes
    push_signed_varint(buf, 0);  // timestampDelta
    push_signed_varint(buf, 0);  // offsetDelta
    push_signed_varint(buf, -1); // keyLen = null
    push_signed_varint(buf, static_cast<int32_t>(value.size()));
    buf.insert(buf.end(), value.begin(), value.end());
    push_signed_varint(buf, 0); // headerCount

    int32_t batch_len = static_cast<int32_t>(buf.size() - batch_len_pos - 4);
    buf[batch_len_pos] = static_cast<uint8_t>((batch_len >> 24) & 0xFF);
    buf[batch_len_pos + 1] = static_cast<uint8_t>((batch_len >> 16) & 0xFF);
    buf[batch_len_pos + 2] = static_cast<uint8_t>((batch_len >> 8) & 0xFF);
    buf[batch_len_pos + 3] = static_cast<uint8_t>(batch_len & 0xFF);

    return buf;
}

auto build_produce_request(int32_t correlation_id,
                           const std::string& topic,
                           const std::vector<uint8_t>& record_batch) -> std::vector<uint8_t> {
    std::vector<uint8_t> buf;
    push_be32(buf, 0); // message_length placeholder
    size_t body_start = buf.size();

    push_be16(buf, 0);  // api_key = Produce
    push_be16(buf, 11); // api_version = 11
    push_be32(buf, correlation_id);
    push_be16(buf, -1);   // client_id = null
    buf.push_back(0x00);  // header TAG_BUFFER
    buf.push_back(0x00);  // transactional_id = null (varint 0)
    push_be16(buf, 1);    // acks = 1
    push_be32(buf, 5000); // timeout_ms

    buf.push_back(0x02); // topics array: 1 element (varint)
    push_unsigned_varint(buf, static_cast<uint32_t>(topic.size()) + 1);
    for (char c : topic)
        buf.push_back(static_cast<uint8_t>(c));

    buf.push_back(0x02); // partitions array: 1 element
    push_be32(buf, 0);   // partition_index = 0
    push_unsigned_varint(buf, static_cast<uint32_t>(record_batch.size()) + 1);
    buf.insert(buf.end(), record_batch.begin(), record_batch.end());
    buf.push_back(0x00); // partition TAG_BUFFER
    buf.push_back(0x00); // topic TAG_BUFFER
    buf.push_back(0x00); // body TAG_BUFFER

    int32_t msg_len = static_cast<int32_t>(buf.size() - body_start);
    buf[0] = static_cast<uint8_t>((msg_len >> 24) & 0xFF);
    buf[1] = static_cast<uint8_t>((msg_len >> 16) & 0xFF);
    buf[2] = static_cast<uint8_t>((msg_len >> 8) & 0xFF);
    buf[3] = static_cast<uint8_t>(msg_len & 0xFF);

    return buf;
}

auto read_response(int fd) -> bool {
    std::array<uint8_t, 4> len_buf{};
    size_t total = 0;
    while (total < 4) {
        auto n = ::read(fd, len_buf.data() + total, 4 - total);
        if (n <= 0)
            return false;
        total += static_cast<size_t>(n);
    }
    int32_t resp_len = (static_cast<int32_t>(len_buf[0]) << 24) |
                       (static_cast<int32_t>(len_buf[1]) << 16) |
                       (static_cast<int32_t>(len_buf[2]) << 8) | static_cast<int32_t>(len_buf[3]);
    if (resp_len <= 0)
        return false;

    std::vector<uint8_t> body(static_cast<size_t>(resp_len));
    total = 0;
    while (total < body.size()) {
        auto n = ::read(fd, body.data() + total, body.size() - total);
        if (n <= 0)
            return false;
        total += static_cast<size_t>(n);
    }
    return true;
}

auto percentile(std::vector<double> sorted, double p) -> double {
    if (sorted.empty())
        return 0;
    auto idx = static_cast<size_t>(std::ceil(p / 100.0 * static_cast<double>(sorted.size()))) - 1;
    if (idx >= sorted.size())
        idx = sorted.size() - 1;
    return sorted[idx];
}

Config parse_args(int argc, char** argv) {
    Config c;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg.starts_with("--messages=")) {
            c.messages = std::stoi(std::string{arg.substr(11)});
        } else if (arg.starts_with("--size=")) {
            c.size = std::stoi(std::string{arg.substr(7)});
        } else if (arg.starts_with("--batch-size=")) {
            c.batch_size = std::stoi(std::string{arg.substr(13)});
        } else if (arg.starts_with("--topic=")) {
            c.topic = std::string{arg.substr(8)};
        } else if (arg.starts_with("--csv=")) {
            c.csv_path = std::string{arg.substr(6)};
        } else if (arg.starts_with("--port=")) {
            c.port = static_cast<uint16_t>(std::stoi(std::string{arg.substr(7)}));
        }
    }
    return c;
}

} // namespace

int main(int argc, char** argv) {
    auto config = parse_args(argc, argv);

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket failed\n";
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(config.port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::cerr << "connect failed\n";
        ::close(fd);
        return 1;
    }

    std::vector<uint8_t> payload(static_cast<size_t>(config.size), 0x42);
    auto record_batch = make_record_batch(payload);

    std::vector<double> latencies_us;
    latencies_us.reserve(static_cast<size_t>(config.messages));

    using Clock = std::chrono::high_resolution_clock;
    auto total_start = Clock::now();

    for (int i = 0; i < config.messages; ++i) {
        auto req = build_produce_request(i, config.topic, record_batch);
        auto msg_start = Clock::now();

        auto sent = ::send(fd, req.data(), req.size(), 0);
        if (sent < 0) {
            std::cerr << "send failed at message " << i << "\n";
            break;
        }

        if (!read_response(fd)) {
            std::cerr << "read response failed at message " << i << "\n";
            break;
        }

        auto elapsed =
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - msg_start).count();
        latencies_us.push_back(static_cast<double>(elapsed));
    }

    auto total_end = Clock::now();
    ::close(fd);

    auto total_s = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                           total_end - total_start)
                                           .count()) /
                   1'000'000.0;

    auto count = static_cast<int>(latencies_us.size());
    auto throughput_msg = static_cast<double>(count) / total_s;
    auto throughput_mb = throughput_msg * static_cast<double>(config.size) / (1024.0 * 1024.0);

    std::ranges::sort(latencies_us);

    auto p50 = percentile(latencies_us, 50.0);
    auto p99 = percentile(latencies_us, 99.0);
    auto p999 = percentile(latencies_us, 99.9);
    auto avg_lat = latencies_us.empty()
                       ? 0.0
                       : std::reduce(latencies_us.begin(), latencies_us.end(), 0.0) /
                             static_cast<double>(latencies_us.size());

    std::string header = std::format("| {:>26} | {:>16} |", "Metric", "Value");
    std::string sep = std::format("|{}|{}|", std::string(28, '-'), std::string(18, '-'));
    auto row = [](std::string_view metric, std::string_view value) {
        return std::format("| {:>26} | {:>16} |", metric, value);
    };

    std::cout << header << '\n'
              << sep << '\n'
              << row("Messages sent", std::to_string(count)) << '\n'
              << row("Message size (B)", std::to_string(config.size)) << '\n'
              << row("Batch size", std::to_string(config.batch_size)) << '\n'
              << row("Total time (s)", std::format("{:.4f}", total_s)) << '\n'
              << row("Throughput (msg/s)", std::format("{:.1f}", throughput_msg)) << '\n'
              << row("Throughput (MB/s)", std::format("{:.2f}", throughput_mb)) << '\n'
              << sep << '\n'
              << row("Avg latency (us)", std::format("{:.1f}", avg_lat)) << '\n'
              << row("P50 latency (us)", std::format("{:.1f}", p50)) << '\n'
              << row("P99 latency (us)", std::format("{:.1f}", p99)) << '\n'
              << row("P999 latency (us)", std::format("{:.1f}", p999)) << '\n';

    if (!config.csv_path.empty()) {
        std::ofstream csv(config.csv_path);
        if (csv) {
            csv << "messages,size,batch_size,time_s,throughput_msg_s,throughput_mb_s,avg_us,p50_us,"
                   "p99_us,p999_us\n";
            csv << count << ',' << config.size << ',' << config.batch_size << ',' << total_s << ','
                << throughput_msg << ',' << throughput_mb << ',' << avg_lat << ',' << p50 << ','
                << p99 << ',' << p999 << '\n';
        }
    }

    return 0;
}
