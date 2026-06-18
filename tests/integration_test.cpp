#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <span>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>

namespace {

constexpr int kServerPort = 9092;
constexpr int kMaxRetries = 50;
constexpr int kRetryDelayUs = 100'000;
constexpr int32_t kTestCorrelationId = 1870644833;

std::string find_server_binary() {
    std::filesystem::path test_dir = std::filesystem::canonical("/proc/self/exe").parent_path();
    std::filesystem::path build_dir = test_dir.parent_path();
    auto server_path = build_dir / "kafka";
    if (!std::filesystem::exists(server_path)) {
        throw std::runtime_error("Server binary not found at: " + server_path.string());
    }
    return server_path.string();
}

auto make_unique_temp_dir() -> std::string {
    static std::atomic<int> counter{0};
    auto path = std::filesystem::temp_directory_path() /
                ("tinytk_int_" + std::to_string(getpid()) + "_" + std::to_string(++counter));
    std::filesystem::create_directories(path);
    return path.string();
}

int connect_to_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(kServerPort);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

template <size_t N> std::array<uint8_t, N> read_exactly(int fd) {
    std::array<uint8_t, N> buf{};
    size_t total = 0;
    while (total < N) {
        auto n = read(fd, buf.data() + total, N - total);
        if (n <= 0) {
            throw std::runtime_error("Failed to read response bytes");
        }
        total += static_cast<size_t>(n);
    }
    return buf;
}

int32_t decode_int32_be_response(std::span<const uint8_t, 4> data) {
    return (static_cast<int32_t>(data[0]) << 24) | (static_cast<int32_t>(data[1]) << 16) |
           (static_cast<int32_t>(data[2]) << 8) | static_cast<int32_t>(data[3]);
}

auto build_request_header(int32_t correlation_id,
                          int16_t api_key,
                          int16_t api_version) -> std::array<uint8_t, 35> {
    const char client_id[] = "kafka-cli";
    static_assert(sizeof(client_id) == 10);

    std::array<uint8_t, 35> buf{};
    buf[0] = 0x00;
    buf[1] = 0x00;
    buf[2] = 0x00;
    buf[3] = 0x1F;

    buf[4] = static_cast<uint8_t>((api_key >> 8) & 0xFF);
    buf[5] = static_cast<uint8_t>(api_key & 0xFF);

    buf[6] = static_cast<uint8_t>((api_version >> 8) & 0xFF);
    buf[7] = static_cast<uint8_t>(api_version & 0xFF);

    buf[8] = static_cast<uint8_t>((correlation_id >> 24) & 0xFF);
    buf[9] = static_cast<uint8_t>((correlation_id >> 16) & 0xFF);
    buf[10] = static_cast<uint8_t>((correlation_id >> 8) & 0xFF);
    buf[11] = static_cast<uint8_t>(correlation_id & 0xFF);

    buf[12] = 0x00;
    buf[13] = 0x09;
    for (size_t i = 0; i < 9; ++i) {
        buf[14 + i] = static_cast<uint8_t>(client_id[i]);
    }

    buf[23] = 0x00;

    buf[24] = 0x0a;
    for (size_t i = 0; i < 9; ++i) {
        buf[25 + i] = static_cast<uint8_t>(client_id[i]);
    }

    buf[34] = 0x00;
    return buf;
}

class ServerProcess {
  public:
    explicit ServerProcess(std::string log_root) : log_root_(std::move(log_root)) {
        auto bin_path = find_server_binary();

        pid_ = fork();
        if (pid_ < 0) {
            throw std::runtime_error("fork failed");
        }

        if (pid_ == 0) {
            std::string arg = "--log.dirs=" + log_root_;
            execl(bin_path.c_str(), bin_path.c_str(), arg.c_str(), nullptr);
            _exit(127);
        }
    }

    ~ServerProcess() {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            int status{};
            waitpid(pid_, &status, 0);
        }
        std::error_code ec;
        std::filesystem::remove_all(log_root_, ec);
    }

    ServerProcess(const ServerProcess&) = delete;
    ServerProcess& operator=(const ServerProcess&) = delete;

    auto connect_with_retry() -> int {
        for (int i = 0; i < kMaxRetries; ++i) {
            int sock = connect_to_server();
            if (sock >= 0) {
                return sock;
            }
            usleep(kRetryDelayUs);
        }
        throw std::runtime_error("Server did not become ready in time");
    }
  private:
    pid_t pid_{};
    std::string log_root_;
};

void push_unsigned_varint(std::vector<uint8_t>& buf, uint32_t val) {
    while (val > 0x7F) {
        buf.push_back(static_cast<uint8_t>((val & 0x7F) | 0x80));
        val >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(val & 0x7F));
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

auto make_topic_record_value(std::string_view name,
                             const std::array<uint8_t, 16>& uuid) -> std::vector<uint8_t> {
    std::vector<uint8_t> v;
    push_unsigned_varint(v, 1); // frame_version
    push_unsigned_varint(v, 2); // type = topic
    push_unsigned_varint(v, 0); // version
    push_unsigned_varint(v, static_cast<uint32_t>(name.size()) + 1);
    for (char c : name)
        v.push_back(static_cast<uint8_t>(c));
    for (auto b : uuid)
        v.push_back(b);
    v.push_back(0x00); // tagged fields
    return v;
}

auto make_partition_record_value(int32_t partition_id,
                                 const std::array<uint8_t, 16>& uuid) -> std::vector<uint8_t> {
    std::vector<uint8_t> v;
    push_unsigned_varint(v, 1); // frame_version
    push_unsigned_varint(v, 3); // type = partition
    push_unsigned_varint(v, 0); // version
    push_be32(v, partition_id);
    for (auto b : uuid)
        v.push_back(b);
    push_unsigned_varint(v, 1); // replicas length
    push_unsigned_varint(v, 1); // isr length
    push_unsigned_varint(v, 1); // removing replicas
    push_unsigned_varint(v, 1); // adding replicas
    push_be32(v, 0);            // leader
    push_be32(v, 0);            // leader epoch
    push_be32(v, 0);            // partition epoch
    push_unsigned_varint(v, 1); // directories length
    v.push_back(0x00);          // tagged fields
    return v;
}

auto make_record(const std::vector<uint8_t>& value) -> std::vector<uint8_t> {
    std::vector<uint8_t> r;
    uint32_t body_size = 1 + 1 + 1 + 1 + 0 + 1 + static_cast<uint32_t>(value.size()) + 1;
    push_signed_varint(r, static_cast<int32_t>(body_size));
    r.push_back(0x00);         // attributes
    push_signed_varint(r, 0);  // timestamp_delta
    push_signed_varint(r, 0);  // offset_delta
    push_signed_varint(r, -1); // key_len = -1 (null)
    push_signed_varint(r, static_cast<int32_t>(value.size()));
    r.insert(r.end(), value.begin(), value.end());
    push_signed_varint(r, 0); // header_count
    return r;
}

auto build_record_batch(const std::vector<std::vector<uint8_t>>& record_values)
    -> std::vector<uint8_t> {
    std::vector<uint8_t> buf;
    push_be64(buf, 0); // baseOffset
    size_t batch_len_pos = buf.size();
    push_be32(buf, 0);                                              // batchLength placeholder
    push_be32(buf, 0);                                              // leaderEpoch
    buf.push_back(0x02);                                            // magic
    push_be32(buf, 0);                                              // crc
    push_be16(buf, 0);                                              // attributes
    push_be32(buf, static_cast<int32_t>(record_values.size()) - 1); // lastOffsetDelta
    push_be64(buf, 0);                                              // baseTimestamp
    push_be64(buf, 0);                                              // maxTimestamp
    push_be64(buf, 0);                                              // producerId
    push_be16(buf, 0);                                              // producerEpoch
    push_be32(buf, 0);                                              // baseSequence

    for (const auto& value : record_values) {
        auto rec = make_record(value);
        buf.insert(buf.end(), rec.begin(), rec.end());
    }

    int32_t batch_len = static_cast<int32_t>(buf.size() - batch_len_pos - 4);
    buf[batch_len_pos] = static_cast<uint8_t>((batch_len >> 24) & 0xFF);
    buf[batch_len_pos + 1] = static_cast<uint8_t>((batch_len >> 16) & 0xFF);
    buf[batch_len_pos + 2] = static_cast<uint8_t>((batch_len >> 8) & 0xFF);
    buf[batch_len_pos + 3] = static_cast<uint8_t>(batch_len & 0xFF);

    return buf;
}

} // namespace

TEST(IntegrationTest, ServerHandlesApiVersionsValidVersion) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    auto request = build_request_header(kTestCorrelationId, 18, 4);
    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly<79>(sock);
    close(sock);

    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x4B); // message_size = 75

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    int16_t error_code = static_cast<int16_t>((static_cast<int16_t>(response[8]) << 8) |
                                              static_cast<int16_t>(response[9]));
    EXPECT_EQ(error_code, 0);
    EXPECT_EQ(response[10], 0x0A); // compact array length = 9 (varint: 9+1=10=0x0A)
    EXPECT_EQ(response[11], 0x00);
    EXPECT_EQ(response[12], 0x00); // api_key = 0 (Produce)
    EXPECT_EQ(response[13], 0x00);
    EXPECT_EQ(response[14], 0x00); // min_version = 0
    EXPECT_EQ(response[15], 0x00);
    EXPECT_EQ(response[16], 0x0B); // max_version = 11
    EXPECT_EQ(response[17], 0x00); // TAG_BUFFER (entry 0)
    EXPECT_EQ(response[18], 0x00);
    EXPECT_EQ(response[19], 0x01); // api_key = 1 (Fetch)
    EXPECT_EQ(response[20], 0x00);
    EXPECT_EQ(response[21], 0x00); // min_version = 0
    EXPECT_EQ(response[22], 0x00);
    EXPECT_EQ(response[23], 0x10); // max_version = 16
    EXPECT_EQ(response[24], 0x00); // TAG_BUFFER (entry 1)
    EXPECT_EQ(response[25], 0x00);
    EXPECT_EQ(response[26], 0x02); // api_key = 2 (ListOffsets)
    EXPECT_EQ(response[27], 0x00);
    EXPECT_EQ(response[28], 0x00); // min_version = 0
    EXPECT_EQ(response[29], 0x00);
    EXPECT_EQ(response[30], 0x08); // max_version = 8
    EXPECT_EQ(response[31], 0x00); // TAG_BUFFER (entry 2)
    EXPECT_EQ(response[32], 0x00);
    EXPECT_EQ(response[33], 0x03); // api_key = 3 (Metadata)
    EXPECT_EQ(response[34], 0x00);
    EXPECT_EQ(response[35], 0x00); // min_version = 0
    EXPECT_EQ(response[36], 0x00);
    EXPECT_EQ(response[37], 0x0C); // max_version = 12
    EXPECT_EQ(response[38], 0x00); // TAG_BUFFER (entry 3)
    EXPECT_EQ(response[39], 0x00);
    EXPECT_EQ(response[40], 0x08); // api_key = 8 (OffsetCommit)
    EXPECT_EQ(response[41], 0x00);
    EXPECT_EQ(response[42], 0x00); // min_version = 0
    EXPECT_EQ(response[43], 0x00);
    EXPECT_EQ(response[44], 0x08); // max_version = 8
    EXPECT_EQ(response[45], 0x00); // TAG_BUFFER (entry 4)
    EXPECT_EQ(response[46], 0x00);
    EXPECT_EQ(response[47], 0x09); // api_key = 9 (OffsetFetch)
    EXPECT_EQ(response[48], 0x00);
    EXPECT_EQ(response[49], 0x00); // min_version = 0
    EXPECT_EQ(response[50], 0x00);
    EXPECT_EQ(response[51], 0x08); // max_version = 8
    EXPECT_EQ(response[52], 0x00); // TAG_BUFFER (entry 5)
    EXPECT_EQ(response[53], 0x00);
    EXPECT_EQ(response[54], 0x0A); // api_key = 10 (FindCoordinator)
    EXPECT_EQ(response[55], 0x00);
    EXPECT_EQ(response[56], 0x00); // min_version = 0
    EXPECT_EQ(response[57], 0x00);
    EXPECT_EQ(response[58], 0x04); // max_version = 4
    EXPECT_EQ(response[59], 0x00); // TAG_BUFFER (entry 6)
    EXPECT_EQ(response[60], 0x00);
    EXPECT_EQ(response[61], 0x12); // api_key = 18 (ApiVersions)
    EXPECT_EQ(response[62], 0x00);
    EXPECT_EQ(response[63], 0x00); // min_version = 0
    EXPECT_EQ(response[64], 0x00);
    EXPECT_EQ(response[65], 0x04); // max_version = 4
    EXPECT_EQ(response[66], 0x00); // TAG_BUFFER (entry 7)
    EXPECT_EQ(response[67], 0x00);
    EXPECT_EQ(response[68], 0x4B); // api_key = 75 (DescribeTopicPartitions)
    EXPECT_EQ(response[69], 0x00);
    EXPECT_EQ(response[70], 0x00); // min_version = 0
    EXPECT_EQ(response[71], 0x00);
    EXPECT_EQ(response[72], 0x00); // max_version = 0
    EXPECT_EQ(response[73], 0x00); // TAG_BUFFER (entry 8)
    EXPECT_EQ(response[74], 0x00);
    EXPECT_EQ(response[75], 0x00);
    EXPECT_EQ(response[76], 0x00);
    EXPECT_EQ(response[77], 0x00); // throttle_time_ms = 0
    EXPECT_EQ(response[78], 0x00); // TAG_BUFFER
}

TEST(IntegrationTest, ServerHandlesMultipleRequestsSameConnection) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    constexpr int32_t correlation_ids[] = {100, 200, 300};

    for (int32_t cid : correlation_ids) {
        auto request = build_request_header(cid, 18, 4);
        auto sent = send(sock, request.data(), request.size(), 0);
        ASSERT_GE(sent, 0) << "Failed to send request";

        auto len_prefix = read_exactly<4>(sock);
        int32_t message_size = (static_cast<int32_t>(len_prefix[0]) << 24) |
                               (static_cast<int32_t>(len_prefix[1]) << 16) |
                               (static_cast<int32_t>(len_prefix[2]) << 8) |
                               static_cast<int32_t>(len_prefix[3]);
        EXPECT_GT(message_size, 0) << "message_size must be positive";

        std::vector<uint8_t> body(message_size);
        size_t total = 0;
        while (total < body.size()) {
            auto n = read(sock, body.data() + total, body.size() - total);
            ASSERT_GT(n, 0) << "Failed to read response body";
            total += static_cast<size_t>(n);
        }

        int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                             (static_cast<int32_t>(body[1]) << 16) |
                             (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
        EXPECT_EQ(echoed_cid, cid) << "Correlation ID mismatch";

        int16_t error_code = static_cast<int16_t>((static_cast<int16_t>(body[4]) << 8) |
                                                  static_cast<int16_t>(body[5]));
        EXPECT_EQ(error_code, 0) << "Error code should be 0";
    }

    close(sock);
}

void verify_api_versions_response(int32_t expected_correlation_id,
                                  const std::vector<uint8_t>& body) {
    ASSERT_GE(body.size(), 6u) << "Response body too short";

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, expected_correlation_id) << "Correlation ID mismatch";

    int16_t error_code =
        static_cast<int16_t>((static_cast<int16_t>(body[4]) << 8) | static_cast<int16_t>(body[5]));
    EXPECT_EQ(error_code, 0) << "Error code should be 0";

    size_t offset = 6;
    ASSERT_LT(offset + 1, body.size()) << "Body too short for compact array length";
    uint32_t array_len = static_cast<uint32_t>(body[offset]);
    offset += 1;
    uint32_t entry_count = (array_len > 0) ? array_len - 1 : 0;
    EXPECT_GE(entry_count, 2u) << "Must have at least two api key entries";
    EXPECT_EQ(body.size() - offset, entry_count * 7 + 4 + 1) << "Response body has extra bytes";

    bool found_api18 = false;
    bool found_api75 = false;
    for (uint32_t i = 0; i < entry_count; ++i) {
        ASSERT_LE(offset + 7, body.size()) << "Truncated api key entry";
        int16_t api_key = static_cast<int16_t>((static_cast<int16_t>(body[offset]) << 8) |
                                               static_cast<int16_t>(body[offset + 1]));
        int16_t min_ver = static_cast<int16_t>((static_cast<int16_t>(body[offset + 2]) << 8) |
                                               static_cast<int16_t>(body[offset + 3]));
        int16_t max_ver = static_cast<int16_t>((static_cast<int16_t>(body[offset + 4]) << 8) |
                                               static_cast<int16_t>(body[offset + 5]));
        if (api_key == 18) {
            found_api18 = true;
            EXPECT_EQ(min_ver, 0) << "MinVersion for ApiKey 18 must be 0";
            EXPECT_EQ(max_ver, 4) << "MaxVersion for ApiKey 18 must be 4";
        }
        if (api_key == 75) {
            found_api75 = true;
            EXPECT_EQ(min_ver, 0) << "MinVersion for ApiKey 75 must be 0";
            EXPECT_EQ(max_ver, 0) << "MaxVersion for ApiKey 75 must be 0";
        }
        offset += 7;
    }
    EXPECT_TRUE(found_api18) << "ApiKey 18 not found in response";
    EXPECT_TRUE(found_api75) << "ApiKey 75 not found in response";
}

TEST(IntegrationTest, ServerHandlesTwoConcurrentClients) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    auto client_task = [&](int base_cid) {
        int sock = server.connect_with_retry();
        ASSERT_GE(sock, 0) << "Client " << base_cid << " failed to connect";

        for (int i = 0; i < 3; ++i) {
            int32_t cid = static_cast<int32_t>(base_cid + i);
            auto request = build_request_header(cid, 18, 4);
            auto sent = send(sock, request.data(), request.size(), 0);
            ASSERT_GE(sent, 0) << "Client " << base_cid << " failed to send request " << i;

            auto len_prefix = read_exactly<4>(sock);
            int32_t message_size = (static_cast<int32_t>(len_prefix[0]) << 24) |
                                   (static_cast<int32_t>(len_prefix[1]) << 16) |
                                   (static_cast<int32_t>(len_prefix[2]) << 8) |
                                   static_cast<int32_t>(len_prefix[3]);
            EXPECT_GT(message_size, 0) << "message_size must be positive";

            std::vector<uint8_t> body(message_size);
            size_t total = 0;
            while (total < body.size()) {
                auto n = read(sock, body.data() + total, body.size() - total);
                ASSERT_GT(n, 0) << "Client " << base_cid << " failed to read response body " << i;
                total += static_cast<size_t>(n);
            }

            verify_api_versions_response(cid, body);
        }

        close(sock);
    };

    std::thread t1(client_task, 100);
    std::thread t2(client_task, 400);

    t1.join();
    t2.join();
}

TEST(IntegrationTest, ServerHandlesApiVersionsUnsupportedVersion) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    auto request = build_request_header(kTestCorrelationId, 18, 26442);
    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly<37>(sock);
    close(sock);

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    int16_t error_code = static_cast<int16_t>((static_cast<int16_t>(response[8]) << 8) |
                                              static_cast<int16_t>(response[9]));
    EXPECT_EQ(error_code, 35);
}

TEST(IntegrationTest, ServerHandlesDescribeTopicPartitionsUnknownTopic) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto push_be16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto push_be32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    request.reserve(27);
    push_be32(23);
    push_be16(75);
    push_be16(0);
    push_be32(kTestCorrelationId);
    request.push_back(0xFF);
    request.push_back(0xFF);
    request.push_back(0x00);
    request.push_back(0x02);
    request.push_back(0x04);
    request.push_back('f');
    request.push_back('o');
    request.push_back('o');
    request.push_back(0x00);
    push_be32(0);
    request.push_back(0xFF);
    request.push_back(0x00);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly<45>(sock);
    close(sock);

    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x29); // message_size = 41

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    EXPECT_EQ(response[8], 0x00); // TAG_BUFFER (response header v1)

    EXPECT_EQ(response[9], 0x00);
    EXPECT_EQ(response[10], 0x00);
    EXPECT_EQ(response[11], 0x00);
    EXPECT_EQ(response[12], 0x00); // throttle_time_ms = 0
    EXPECT_EQ(response[13], 0x02); // topics array length = 1
    EXPECT_EQ(response[14], 0x00);
    EXPECT_EQ(response[15], 0x03); // error_code = 3
    EXPECT_EQ(response[16], 0x04); // topic_name length = 3
    EXPECT_EQ(response[17], 'f');
    EXPECT_EQ(response[18], 'o');
    EXPECT_EQ(response[19], 'o');
    EXPECT_EQ(response[36], 0x00); // is_internal = false
    EXPECT_EQ(response[37], 0x01); // partitions array empty
    EXPECT_EQ(response[42], 0x00); // topic TAG_BUFFER
    EXPECT_EQ(response[43], 0xFF); // next_cursor = -1 (null)
    EXPECT_EQ(response[44], 0x00); // body TAG_BUFFER
}

TEST(IntegrationTest, ServerHandlesDescribeTopicPartitionsMultiTopicSorted) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto push_be16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto push_be32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    request.reserve(35);
    push_be32(32);
    push_be16(75);
    push_be16(0);
    push_be32(kTestCorrelationId);
    request.push_back(0xFF);
    request.push_back(0xFF);
    request.push_back(0x00);
    request.push_back(0x03);
    request.push_back(0x06);
    request.push_back('z');
    request.push_back('e');
    request.push_back('b');
    request.push_back('r');
    request.push_back('a');
    request.push_back(0x00);
    request.push_back(0x06);
    request.push_back('a');
    request.push_back('p');
    request.push_back('p');
    request.push_back('l');
    request.push_back('e');
    request.push_back(0x00);
    push_be32(0);
    request.push_back(0xFF);
    request.push_back(0x00);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t message_size = decode_int32_be_response(len_prefix);
    EXPECT_GT(message_size, 0) << "message_size must be positive";

    std::vector<uint8_t> body(message_size);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0) << "Failed to read response body";
        total += static_cast<size_t>(n);
    }

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId);

    EXPECT_EQ(body[4], 0x00);

    size_t offset = 9;
    ASSERT_LT(offset + 1, body.size());
    uint32_t topics_len = static_cast<uint32_t>(body[offset]);
    uint32_t topic_count = (topics_len > 0) ? topics_len - 1 : 0;
    offset += 1;

    std::vector<std::string> response_names;
    for (uint32_t i = 0; i < topic_count; ++i) {
        ASSERT_LT(offset + 1, body.size());
        offset += 2;
        ASSERT_LT(offset, body.size());
        uint32_t name_len = static_cast<uint32_t>(body[offset]) - 1;
        offset += 1;
        ASSERT_LE(offset + name_len, body.size());
        std::string name(body.begin() + static_cast<ptrdiff_t>(offset),
                         body.begin() + static_cast<ptrdiff_t>(offset + name_len));
        response_names.push_back(name);
        offset += name_len + 16 + 1;
        ASSERT_LT(offset + 1, body.size());
        uint32_t parts_len = static_cast<uint32_t>(body[offset]);
        uint32_t part_count = (parts_len > 0) ? parts_len - 1 : 0;
        offset += 1 + part_count * 35;
        offset += 5;
    }

    ASSERT_EQ(response_names.size(), 2u);
    EXPECT_EQ(response_names[0], "apple");
    EXPECT_EQ(response_names[1], "zebra");

    ASSERT_LT(offset + 1, body.size());
    EXPECT_EQ(body[offset], 0xFF);

    close(sock);
}

TEST(IntegrationTest, ApiVersionsResponseContainsFetchApiEntry) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    auto request = build_request_header(kTestCorrelationId, 18, 4);
    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t message_size = decode_int32_be_response(len_prefix);
    EXPECT_GT(message_size, 0) << "message_size must be positive";

    std::vector<uint8_t> body(message_size);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0) << "Failed to read response body";
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId) << "Correlation ID mismatch";

    int16_t error_code =
        static_cast<int16_t>((static_cast<int16_t>(body[4]) << 8) | static_cast<int16_t>(body[5]));
    EXPECT_EQ(error_code, 0) << "Error code should be 0";

    size_t offset = 6;
    ASSERT_LT(offset + 1, body.size()) << "Body too short for compact array length";
    uint32_t array_len = static_cast<uint32_t>(body[offset]);
    offset += 1;
    uint32_t entry_count = (array_len > 0) ? array_len - 1 : 0;
    EXPECT_GE(entry_count, 3u) << "Must have at least three api key entries";

    bool found_fetch = false;
    bool found_apiversions = false;
    bool found_describetopic = false;
    for (uint32_t i = 0; i < entry_count; ++i) {
        ASSERT_LE(offset + 7, body.size()) << "Truncated api key entry";
        int16_t api_key = static_cast<int16_t>((static_cast<int16_t>(body[offset]) << 8) |
                                               static_cast<int16_t>(body[offset + 1]));
        int16_t min_ver = static_cast<int16_t>((static_cast<int16_t>(body[offset + 2]) << 8) |
                                               static_cast<int16_t>(body[offset + 3]));
        int16_t max_ver = static_cast<int16_t>((static_cast<int16_t>(body[offset + 4]) << 8) |
                                               static_cast<int16_t>(body[offset + 5]));
        if (api_key == 1) {
            found_fetch = true;
            EXPECT_EQ(min_ver, 0) << "MinVersion for Fetch must be 0";
            EXPECT_GE(max_ver, 16) << "MaxVersion for Fetch must be at least 16";
        }
        if (api_key == 18) {
            found_apiversions = true;
        }
        if (api_key == 75) {
            found_describetopic = true;
        }
        offset += 7;
    }
    EXPECT_TRUE(found_fetch) << "Fetch API (key=1) entry not found in response";
    EXPECT_TRUE(found_apiversions) << "ApiVersions (key=18) entry not found in response";
    EXPECT_TRUE(found_describetopic)
        << "DescribeTopicPartitions (key=75) entry not found in response";
}

TEST(IntegrationTest, ServerHandlesFetchRequestEmptyTopics) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto push_be16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto push_be32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    request.reserve(40);
    push_be32(36); // message_length
    push_be16(1);  // api_key = 1 (Fetch)
    push_be16(16); // api_version = 16
    push_be32(kTestCorrelationId);
    push_be16(0);            // client_id = null (int16 length)
    request.push_back(0x00); // header TAG_BUFFER
    push_be32(500);          // max_wait_ms
    push_be32(1);            // min_bytes
    push_be32(0x00100000);   // max_bytes = 1MB
    request.push_back(0x00); // isolation_level
    push_be32(0);            // session_id
    push_be32(0);            // session_epoch
    request.push_back(0x01); // topics array = empty
    request.push_back(0x01); // forgotten_topics = empty
    request.push_back(0x01); // rack_id = empty
    request.push_back(0x00); // body TAG_BUFFER

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly<21>(sock);
    close(sock);

    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x11); // message_size = 17

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    EXPECT_EQ(response[8], 0x00); // header TAG_BUFFER

    EXPECT_EQ(response[9], 0x00);
    EXPECT_EQ(response[10], 0x00);
    EXPECT_EQ(response[11], 0x00);
    EXPECT_EQ(response[12], 0x00); // throttle_time_ms = 0

    EXPECT_EQ(response[13], 0x00);
    EXPECT_EQ(response[14], 0x00); // error_code = 0

    EXPECT_EQ(response[15], 0x00);
    EXPECT_EQ(response[16], 0x00);
    EXPECT_EQ(response[17], 0x00);
    EXPECT_EQ(response[18], 0x00); // session_id = 0

    EXPECT_EQ(response[19], 0x01); // responses varint = 1 (0 entries)
    EXPECT_EQ(response[20], 0x00); // body TAG_BUFFER
}

TEST(IntegrationTest, FetchResponseReturnsRecordBatchFromDisk) {
    constexpr std::array<uint8_t, 16> kTestUuid = {
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
    std::vector<uint8_t> record_batch_data = {
        0x01,
        0x02,
        0x03,
        0x04,
        0x05,
        0x06,
        0x07,
        0x08,
    };

    namespace fs = std::filesystem;
    auto root = make_unique_temp_dir();

    fs::create_directories(root + "/__cluster_metadata-0");
    {
        auto topic_val = make_topic_record_value("bar", kTestUuid);
        auto part_val = make_partition_record_value(0, kTestUuid);
        auto batch = build_record_batch({topic_val, part_val});
        std::ofstream f(root + "/__cluster_metadata-0/00000000000000000000.log", std::ios::binary);
        f.write(reinterpret_cast<const char*>(batch.data()),
                static_cast<std::streamsize>(batch.size()));
    }

    fs::create_directories(root + "/bar-0");
    {
        std::ofstream f(root + "/bar-0/00000000000000000000.log", std::ios::binary);
        f.write(reinterpret_cast<const char*>(record_batch_data.data()),
                static_cast<std::streamsize>(record_batch_data.size()));
    }

    ServerProcess server(root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb64 = [&](int64_t v) {
        for (int i = 7; i >= 0; --i)
            request.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    };

    request.reserve(128);
    pb32(0); // message_length placeholder
    size_t body_start = request.size();
    pb16(1);                  // api_key = Fetch
    pb16(16);                 // api_version = 16
    pb32(kTestCorrelationId); // correlation_id
    request.push_back(0x00);
    request.push_back(0x00); // client_id = null
    request.push_back(0x00); // header TAG_BUFFER
    pb32(500);               // max_wait_ms
    pb32(1);                 // min_bytes
    pb32(0x00100000);        // max_bytes
    request.push_back(0x00); // isolation_level
    pb32(0);                 // session_id
    pb32(0);                 // session_epoch
    request.push_back(0x02); // topics array: 1 element
    for (auto b : kTestUuid)
        request.push_back(b);
    request.push_back(0x02); // partitions array: 1 element
    pb32(0);                 // partition_index = 0
    pb32(-1);                // current_leader_epoch
    pb64(0);                 // fetch_offset
    pb32(-1);                // last_fetched_epoch
    pb64(-1);                // log_start_offset
    pb32(0x00100000);        // max_bytes
    request.push_back(0x00); // partition TAG_BUFFER
    request.push_back(0x00); // topic TAG_BUFFER
    request.push_back(0x01); // forgotten_topics = empty
    request.push_back(0x01); // rack_id = empty
    request.push_back(0x00); // body TAG_BUFFER

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0) << "Response message length must be positive";

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0) << "Failed to read response body";
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId) << "Correlation ID mismatch";

    EXPECT_EQ(body[4], 0x00); // header TAG_BUFFER

    int32_t throttle = (static_cast<int32_t>(body[5]) << 24) |
                       (static_cast<int32_t>(body[6]) << 16) |
                       (static_cast<int32_t>(body[7]) << 8) | static_cast<int32_t>(body[8]);
    EXPECT_EQ(throttle, 0);

    int16_t error_code =
        static_cast<int16_t>((static_cast<int16_t>(body[9]) << 8) | static_cast<int16_t>(body[10]));
    EXPECT_EQ(error_code, 0);

    EXPECT_EQ(body[11], 0x00);
    EXPECT_EQ(body[12], 0x00);
    EXPECT_EQ(body[13], 0x00);
    EXPECT_EQ(body[14], 0x00); // session_id = 0

    size_t off = 15;
    ASSERT_LT(off, body.size());
    uint32_t resp_len = static_cast<uint32_t>(body[off++]);
    ASSERT_GE(resp_len, 2u);
    EXPECT_EQ(resp_len - 1, 1u) << "Expected 1 topic response";

    for (size_t i = 0; i < 16; ++i) {
        EXPECT_EQ(body[off + i], kTestUuid[i]);
    }
    off += 16;

    ASSERT_LT(off, body.size());
    uint32_t part_len = static_cast<uint32_t>(body[off++]);
    ASSERT_GE(part_len, 2u);
    EXPECT_EQ(part_len - 1, 1u) << "Expected 1 partition response";

    int32_t part_idx =
        (static_cast<int32_t>(body[off]) << 24) | (static_cast<int32_t>(body[off + 1]) << 16) |
        (static_cast<int32_t>(body[off + 2]) << 8) | static_cast<int32_t>(body[off + 3]);
    EXPECT_EQ(part_idx, 0);
    off += 4;

    int16_t part_err = static_cast<int16_t>((static_cast<int16_t>(body[off]) << 8) |
                                            static_cast<int16_t>(body[off + 1]));
    EXPECT_EQ(part_err, 0);
    off += 2;

    off += 8 + 8 + 8; // high_watermark + last_stable_offset + log_start_offset

    ASSERT_LT(off, body.size());
    uint32_t aborted_len = static_cast<uint32_t>(body[off++]);
    EXPECT_EQ(aborted_len, 1u) << "aborted_transactions should be empty";

    off += 4; // preferred_read_replica

    ASSERT_LT(off, body.size());
    uint32_t records_len = static_cast<uint32_t>(body[off++]);
    ASSERT_GE(records_len, 1u);
    size_t num_records = records_len > 0 ? records_len - 1 : 0;
    EXPECT_EQ(num_records, record_batch_data.size());

    for (size_t i = 0; i < num_records; ++i) {
        ASSERT_LT(off + i, body.size());
        EXPECT_EQ(body[off + i], record_batch_data[i]);
    }
}

TEST(IntegrationTest, ProduceRequestPersistsRecordBatchToDisk) {
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

    std::vector<uint8_t> record_value = {0x01, 0x02, 0x03};
    auto record_batch = build_record_batch({record_value});

    namespace fs = std::filesystem;
    auto root = make_unique_temp_dir();

    fs::create_directories(root + "/__cluster_metadata-0");
    {
        auto topic_val = make_topic_record_value("orders", kTopicUuid);
        auto part_val = make_partition_record_value(0, kTopicUuid);
        auto batch = build_record_batch({topic_val, part_val});
        std::ofstream f(root + "/__cluster_metadata-0/00000000000000000000.log", std::ios::binary);
        f.write(reinterpret_cast<const char*>(batch.data()),
                static_cast<std::streamsize>(batch.size()));
    }

    ServerProcess server(root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    request.reserve(128);
    pb32(0); // message_length placeholder
    size_t body_start = request.size();
    pb16(0);  // api_key = Produce
    pb16(11); // api_version = 11
    pb32(kTestCorrelationId);

    pb16(-1);                // client_id = null
    request.push_back(0x00); // header TAG_BUFFER
    request.push_back(0x00); // transactional_id = null
    pb16(1);                 // acks = 1
    pb32(5000);              // timeout_ms = 5000

    request.push_back(0x02); // topics array: 1 element
    request.push_back(0x07); // topic name varint = 7 (6 bytes)
    request.push_back('o');
    request.push_back('r');
    request.push_back('d');
    request.push_back('e');
    request.push_back('r');
    request.push_back('s');

    request.push_back(0x02); // partitions array: 1 element
    pb32(0);                 // partition_index = 0
    // records: compact bytes (varint length+1, then bytes)
    push_unsigned_varint(request, static_cast<uint32_t>(record_batch.size()) + 1);
    request.insert(request.end(), record_batch.begin(), record_batch.end());
    request.push_back(0x00); // partition TAG_BUFFER
    request.push_back(0x00); // topic TAG_BUFFER
    request.push_back(0x00); // body TAG_BUFFER

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send Produce request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0) << "Response message length must be positive";

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0) << "Failed to read response body";
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId) << "Correlation ID mismatch";

    EXPECT_EQ(body[4], 0x00); // header TAG_BUFFER

    size_t off = 5;
    ASSERT_LT(off, body.size());
    uint32_t resp_len = static_cast<uint32_t>(body[off++]);
    ASSERT_GE(resp_len, 2u) << "topics array must contain at least 1 element";
    EXPECT_EQ(resp_len - 1, 1u) << "Expected 1 topic response";

    ASSERT_LT(off, body.size());
    uint32_t name_len = static_cast<uint32_t>(body[off++]);
    EXPECT_EQ(name_len - 1, 6u) << "Expected topic name length 6";
    std::string resp_name(body.begin() + static_cast<ptrdiff_t>(off),
                          body.begin() + static_cast<ptrdiff_t>(off + name_len - 1));
    EXPECT_EQ(resp_name, "orders");
    off += name_len - 1;

    ASSERT_LT(off, body.size());
    uint32_t part_len = static_cast<uint32_t>(body[off++]);
    ASSERT_GE(part_len, 2u) << "partitions array must contain at least 1 element";
    EXPECT_EQ(part_len - 1, 1u) << "Expected 1 partition response";

    int32_t part_idx =
        (static_cast<int32_t>(body[off]) << 24) | (static_cast<int32_t>(body[off + 1]) << 16) |
        (static_cast<int32_t>(body[off + 2]) << 8) | static_cast<int32_t>(body[off + 3]);
    EXPECT_EQ(part_idx, 0) << "Partition index must be 0";
    off += 4;

    int16_t part_err = static_cast<int16_t>((static_cast<int16_t>(body[off]) << 8) |
                                            static_cast<int16_t>(body[off + 1]));
    EXPECT_EQ(part_err, 0) << "Error code must be 0 (NO_ERROR)";
    off += 2;

    int64_t base_offset =
        (static_cast<int64_t>(body[off]) << 56) | (static_cast<int64_t>(body[off + 1]) << 48) |
        (static_cast<int64_t>(body[off + 2]) << 40) | (static_cast<int64_t>(body[off + 3]) << 32) |
        (static_cast<int64_t>(body[off + 4]) << 24) | (static_cast<int64_t>(body[off + 5]) << 16) |
        (static_cast<int64_t>(body[off + 6]) << 8) | static_cast<int64_t>(body[off + 7]);
    EXPECT_EQ(base_offset, 0) << "base_offset must be 0";
    off += 8;

    int64_t log_append_time =
        (static_cast<int64_t>(body[off]) << 56) | (static_cast<int64_t>(body[off + 1]) << 48) |
        (static_cast<int64_t>(body[off + 2]) << 40) | (static_cast<int64_t>(body[off + 3]) << 32) |
        (static_cast<int64_t>(body[off + 4]) << 24) | (static_cast<int64_t>(body[off + 5]) << 16) |
        (static_cast<int64_t>(body[off + 6]) << 8) | static_cast<int64_t>(body[off + 7]);
    EXPECT_GT(log_append_time, 0) << "log_append_time_ms must be positive";
    off += 8;

    int64_t log_start =
        (static_cast<int64_t>(body[off]) << 56) | (static_cast<int64_t>(body[off + 1]) << 48) |
        (static_cast<int64_t>(body[off + 2]) << 40) | (static_cast<int64_t>(body[off + 3]) << 32) |
        (static_cast<int64_t>(body[off + 4]) << 24) | (static_cast<int64_t>(body[off + 5]) << 16) |
        (static_cast<int64_t>(body[off + 6]) << 8) | static_cast<int64_t>(body[off + 7]);
    EXPECT_EQ(log_start, 0) << "log_start_offset must be 0";

    // Verify disk persistence
    auto log_path = root + "/orders-0/00000000000000000000.log";
    EXPECT_TRUE(fs::exists(log_path));

    std::ifstream log_file(log_path, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(log_file.is_open());
    auto file_sz = log_file.tellg();
    ASSERT_EQ(static_cast<size_t>(file_sz), record_value.size());
    log_file.seekg(0);
    std::vector<uint8_t> disk_buf(static_cast<size_t>(file_sz));
    log_file.read(reinterpret_cast<char*>(disk_buf.data()), file_sz);
    EXPECT_EQ(disk_buf, record_value);
}

TEST(IntegrationTest, ServerHandlesMetadataRequest) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    pb32(0); // message_length placeholder
    size_t body_start = request.size();
    pb16(3);                  // api_key = 3 (Metadata)
    pb16(0);                  // api_version = 0
    pb32(kTestCorrelationId); // correlation_id
    pb16(-1);                 // client_id = null
    request.push_back(0x00);  // header TAG_BUFFER
    pb32(0);                  // topics: empty array
    request.push_back(0x00);  // allow_auto_topic_creation = false

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0) << "Response message length must be positive";

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0) << "Failed to read response body";
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId) << "Correlation ID mismatch";

    EXPECT_EQ(body[4], 0x00); // header TAG_BUFFER

    int32_t throttle = (static_cast<int32_t>(body[5]) << 24) |
                       (static_cast<int32_t>(body[6]) << 16) |
                       (static_cast<int32_t>(body[7]) << 8) | static_cast<int32_t>(body[8]);
    EXPECT_EQ(throttle, 0);
}

TEST(IntegrationTest, ServerHandlesListOffsetsRequest) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb64 = [&](int64_t v) {
        for (int i = 7; i >= 0; --i)
            request.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    };

    pb32(0); // message_length placeholder
    size_t body_start = request.size();
    pb16(2);                  // api_key = 2 (ListOffsets)
    pb16(0);                  // api_version = 0
    pb32(kTestCorrelationId); // correlation_id
    pb16(-1);                 // client_id = null
    request.push_back(0x00);  // header TAG_BUFFER
    pb32(-1);                 // replica_id
    request.push_back(0x00);  // isolation_level
    request.push_back(0x02);  // topics array: 1 element (varint 2)
    request.push_back(0x05);  // topic name varint = 5
    request.push_back('t');
    request.push_back('e');
    request.push_back('s');
    request.push_back('t');
    request.push_back(0x02); // partitions: 1 element (varint 2)
    pb32(0);                 // partition_index = 0
    pb64(-1);                // timestamp = -1 (latest)
    request.push_back(0x00); // partition TAG_BUFFER
    request.push_back(0x00); // topic TAG_BUFFER
    request.push_back(0x00); // body TAG_BUFFER

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0) << "Response message length must be positive";

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0) << "Failed to read response body";
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId);
}

TEST(IntegrationTest, ServerHandlesFindCoordinatorRequest) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    pb32(0); // message_length placeholder
    size_t body_start = request.size();
    pb16(10);                 // api_key = 10
    pb16(0);                  // api_version = 0
    pb32(kTestCorrelationId); // correlation_id
    pb16(-1);                 // client_id = null
    request.push_back(0x00);  // header TAG_BUFFER
    request.push_back(0x07);  // key varint
    request.push_back('m');
    request.push_back('y');
    request.push_back('-');
    request.push_back('g');
    request.push_back('r');
    request.push_back('p');
    request.push_back(0x00); // key_type = 0
    request.push_back(0x00); // body TAG_BUFFER

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0);

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0);
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId);
}

TEST(IntegrationTest, ServerHandlesOffsetCommitRequest) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0);

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb64 = [&](int64_t v) {
        for (int i = 7; i >= 0; --i)
            request.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    };

    pb32(0);
    size_t body_start = request.size();
    pb16(8);
    pb16(0);
    pb32(kTestCorrelationId);
    pb16(-1);
    request.push_back(0x00);
    request.push_back(0x04);
    request.push_back('g');
    request.push_back('r');
    request.push_back('p');
    request.push_back(0x01);
    pb32(1);
    request.push_back(0x02);
    request.push_back(0x05);
    request.push_back('t');
    request.push_back('e');
    request.push_back('s');
    request.push_back('t');
    request.push_back(0x02);
    pb32(0);
    pb64(42);
    request.push_back(0x00);
    request.push_back(0x00);
    request.push_back(0x00);

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0);

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0);

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0);
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId);
}

TEST(IntegrationTest, ServerHandlesOffsetFetchRequest) {
    auto log_root = make_unique_temp_dir();
    ServerProcess server(log_root);

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0);

    std::vector<uint8_t> request;
    auto pb16 = [&](int16_t v) {
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };
    auto pb32 = [&](int32_t v) {
        request.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        request.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        request.push_back(static_cast<uint8_t>(v & 0xFF));
    };

    pb32(0);
    size_t body_start = request.size();
    pb16(9);
    pb16(0);
    pb32(kTestCorrelationId);
    pb16(-1);
    request.push_back(0x00);
    request.push_back(0x02);
    request.push_back('g');
    request.push_back(0x02);
    request.push_back(0x02);
    request.push_back('t');
    request.push_back(0x02);
    pb32(0);
    request.push_back(0x00);
    request.push_back(0x00);

    int32_t message_len = static_cast<int32_t>(request.size() - body_start);
    request[0] = static_cast<uint8_t>((message_len >> 24) & 0xFF);
    request[1] = static_cast<uint8_t>((message_len >> 16) & 0xFF);
    request[2] = static_cast<uint8_t>((message_len >> 8) & 0xFF);
    request[3] = static_cast<uint8_t>(message_len & 0xFF);

    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0);

    auto len_prefix = read_exactly<4>(sock);
    int32_t resp_msg_len = decode_int32_be_response(len_prefix);
    EXPECT_GT(resp_msg_len, 0);

    std::vector<uint8_t> body(resp_msg_len);
    size_t total = 0;
    while (total < body.size()) {
        auto n = read(sock, body.data() + total, body.size() - total);
        ASSERT_GT(n, 0);
        total += static_cast<size_t>(n);
    }
    close(sock);

    int32_t echoed_cid = (static_cast<int32_t>(body[0]) << 24) |
                         (static_cast<int32_t>(body[1]) << 16) |
                         (static_cast<int32_t>(body[2]) << 8) | static_cast<int32_t>(body[3]);
    EXPECT_EQ(echoed_cid, kTestCorrelationId);
}
