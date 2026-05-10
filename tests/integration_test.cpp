#include <array>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

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
    ServerProcess() {
        auto bin_path = find_server_binary();

        pid_ = fork();
        if (pid_ < 0) {
            throw std::runtime_error("fork failed");
        }

        if (pid_ == 0) {
            execl(bin_path.c_str(), bin_path.c_str(), nullptr);
            _exit(127);
        }
    }

    ~ServerProcess() {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            int status{};
            waitpid(pid_, &status, 0);
        }
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
};

} // namespace

TEST(IntegrationTest, ServerHandlesApiVersionsValidVersion) {
    ServerProcess server;

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    auto request = build_request_header(kTestCorrelationId, 18, 4);
    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly<31>(sock);
    close(sock);

    EXPECT_EQ(response[0], 0x00);
    EXPECT_EQ(response[1], 0x00);
    EXPECT_EQ(response[2], 0x00);
    EXPECT_EQ(response[3], 0x1b); // message_size = 27

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    int16_t error_code =
        (static_cast<int16_t>(response[9]) << 8) | static_cast<int16_t>(response[10]);
    EXPECT_EQ(error_code, 0);
    EXPECT_EQ(response[11], 0x03); // compact array length = 2 (varint: 2+1)
    EXPECT_EQ(response[12], 0x00);
    EXPECT_EQ(response[13], 0x12); // api_key = 18
    EXPECT_EQ(response[14], 0x00);
    EXPECT_EQ(response[15], 0x00); // min_version = 0
    EXPECT_EQ(response[16], 0x00);
    EXPECT_EQ(response[17], 0x04); // max_version = 4
    EXPECT_EQ(response[18], 0x00); // TAG_BUFFER (entry 1)
    EXPECT_EQ(response[19], 0x00);
    EXPECT_EQ(response[20], 0x4b); // api_key = 75
    EXPECT_EQ(response[21], 0x00);
    EXPECT_EQ(response[22], 0x00); // min_version = 0
    EXPECT_EQ(response[23], 0x00);
    EXPECT_EQ(response[24], 0x00); // max_version = 0
    EXPECT_EQ(response[25], 0x00); // TAG_BUFFER (entry 2)
    EXPECT_EQ(response[26], 0x00);
    EXPECT_EQ(response[27], 0x00);
    EXPECT_EQ(response[28], 0x00);
    EXPECT_EQ(response[29], 0x00); // throttle_time_ms = 0
    EXPECT_EQ(response[30], 0x00); // TAG_BUFFER
}

TEST(IntegrationTest, ServerHandlesMultipleRequestsSameConnection) {
    ServerProcess server;

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

        int16_t error_code = (static_cast<int16_t>(body[5]) << 8) | static_cast<int16_t>(body[6]);
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

    int16_t error_code = (static_cast<int16_t>(body[5]) << 8) | static_cast<int16_t>(body[6]);
    EXPECT_EQ(error_code, 0) << "Error code should be 0";

    size_t offset = 7;
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
        int16_t api_key =
            (static_cast<int16_t>(body[offset]) << 8) | static_cast<int16_t>(body[offset + 1]);
        int16_t min_ver =
            (static_cast<int16_t>(body[offset + 2]) << 8) | static_cast<int16_t>(body[offset + 3]);
        int16_t max_ver =
            (static_cast<int16_t>(body[offset + 4]) << 8) | static_cast<int16_t>(body[offset + 5]);
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
    ServerProcess server;

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
    ServerProcess server;

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    auto request = build_request_header(kTestCorrelationId, 18, 26442);
    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly<31>(sock);
    close(sock);

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    int16_t error_code =
        (static_cast<int16_t>(response[9]) << 8) | static_cast<int16_t>(response[10]);
    EXPECT_EQ(error_code, 35);
}

TEST(IntegrationTest, ServerHandlesDescribeTopicPartitionsUnknownTopic) {
    ServerProcess server;

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
