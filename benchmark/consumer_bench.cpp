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

auto build_fetch_request(int32_t correlation_id,
                         const std::array<uint8_t, 16>& topic_uuid,
                         int32_t partition,
                         int64_t fetch_offset,
                         int32_t max_bytes) -> std::vector<uint8_t> {
    std::vector<uint8_t> buf;
    push_be32(buf, 0); // message_length placeholder
    size_t body_start = buf.size();

    push_be16(buf, 1);  // api_key = Fetch
    push_be16(buf, 16); // api_version = 16
    push_be32(buf, correlation_id);
    push_be16(buf, -1);  // client_id = null
    buf.push_back(0x00); // header TAG_BUFFER
    push_be32(buf, 500); // max_wait_ms
    push_be32(buf, 1);   // min_bytes
    push_be32(buf, max_bytes);
    buf.push_back(0x00); // isolation_level
    push_be32(buf, 0);   // session_id
    push_be32(buf, 0);   // session_epoch
    buf.push_back(0x02); // topics: 1 element (varint)
    for (auto b : topic_uuid)
        buf.push_back(b);
    buf.push_back(0x02); // partitions: 1 element (varint)
    push_be32(buf, partition);
    push_be32(buf, -1); // current_leader_epoch
    push_be64(buf, fetch_offset);
    push_be32(buf, -1); // last_fetched_epoch
    push_be64(buf, -1); // log_start_offset
    push_be32(buf, max_bytes);
    buf.push_back(0x00); // partition TAG_BUFFER
    buf.push_back(0x00); // topic TAG_BUFFER
    buf.push_back(0x01); // forgotten_topics empty (varint 1)
    buf.push_back(0x01); // rack_id empty (varint 1)
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

    constexpr std::array<uint8_t, 16> kTopicUuid = {
        0xa1,
        0xb2,
        0xc3,
        0xd4,
        0xe5,
        0xf6,
        0xa7,
        0xb8,
        0xc9,
        0xd0,
        0xe1,
        0xf2,
        0xa3,
        0xb4,
        0xc5,
        0xd6,
    };

    std::vector<double> latencies_us;

    using Clock = std::chrono::high_resolution_clock;
    auto total_start = Clock::now();

    auto req = build_fetch_request(1, kTopicUuid, 0, 0, 1'048'576);
    auto sent = ::send(fd, req.data(), req.size(), 0);
    if (sent < 0) {
        std::cerr << "send failed\n";
        ::close(fd);
        return 1;
    }

    if (!read_response(fd)) {
        std::cerr << "read response failed\n";
        ::close(fd);
        return 1;
    }

    auto total_end = Clock::now();
    ::close(fd);

    auto total_s = static_cast<double>(std::chrono::duration_cast<std::chrono::microseconds>(
                                           total_end - total_start)
                                           .count()) /
                   1'000'000.0;

    auto count = config.messages;
    auto throughput_msg = static_cast<double>(count) / total_s;

    std::string header = std::format("| {:>26} | {:>16} |", "Metric", "Value");
    std::string sep = std::format("|{}|{}|", std::string(28, '-'), std::string(18, '-'));
    auto row = [](std::string_view metric, std::string_view value) {
        return std::format("| {:>26} | {:>16} |", metric, value);
    };

    std::cout << header << '\n'
              << sep << '\n'
              << row("Messages (loaded)", std::to_string(count)) << '\n'
              << row("Total time (s)", std::format("{:.4f}", total_s)) << '\n'
              << row("Throughput (msg/s)", std::format("{:.1f}", throughput_msg)) << '\n';

    if (!config.csv_path.empty()) {
        std::ofstream csv(config.csv_path);
        if (csv) {
            csv << "type,messages,time_s,throughput_msg_s,avg_us,p50_us,p99_us,p999_us\n";
            csv << "consumer," << count << ',' << total_s << ',' << throughput_msg << ",0,0,0,0\n";
        }
    }

    return 0;
}
