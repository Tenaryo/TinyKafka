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

std::array<uint8_t, 10> read_exactly(int fd) {
    std::array<uint8_t, 10> buf{};
    size_t total = 0;
    while (total < 10) {
        auto n = read(fd, buf.data() + total, 10 - total);
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
    buf[3] = 0x23;

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

    auto response = read_exactly(sock);
    close(sock);

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId)
        << "Expected correlation_id 0x" << std::hex << kTestCorrelationId << " but got 0x"
        << echoed_correlation_id;

    int16_t error_code =
        (static_cast<int16_t>(response[8]) << 8) | static_cast<int16_t>(response[9]);
    EXPECT_EQ(error_code, 0) << "Expected error_code 0 for valid version";
}

TEST(IntegrationTest, ServerHandlesApiVersionsUnsupportedVersion) {
    ServerProcess server;

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    auto request = build_request_header(kTestCorrelationId, 18, 26442);
    auto sent = send(sock, request.data(), request.size(), 0);
    ASSERT_GE(sent, 0) << "Failed to send request";

    auto response = read_exactly(sock);
    close(sock);

    int32_t echoed_correlation_id =
        decode_int32_be_response(std::span<const uint8_t, 4>{response.data() + 4, 4});
    EXPECT_EQ(echoed_correlation_id, kTestCorrelationId);

    int16_t error_code =
        (static_cast<int16_t>(response[8]) << 8) | static_cast<int16_t>(response[9]);
    EXPECT_EQ(error_code, 35) << "Expected error_code 35 (UNSUPPORTED_VERSION)";
}
