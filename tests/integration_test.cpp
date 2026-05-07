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

std::array<uint8_t, 8> read_exactly(int fd) {
    std::array<uint8_t, 8> buf{};
    size_t total = 0;
    while (total < 8) {
        auto n = read(fd, buf.data() + total, 8 - total);
        if (n <= 0) {
            throw std::runtime_error("Failed to read response bytes");
        }
        total += static_cast<size_t>(n);
    }
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

TEST(IntegrationTest, ServerRespondsWithCorrectResponseHeader) {
    ServerProcess server;

    int sock = server.connect_with_retry();
    ASSERT_GE(sock, 0) << "Failed to connect to server";

    if (send(sock, "placeholder", 11, 0) < 0) {
        close(sock);
        FAIL() << "Failed to send placeholder request";
    }

    auto response = read_exactly(sock);
    close(sock);

    EXPECT_EQ(response[0], 0x00) << "message_size byte 0";
    EXPECT_EQ(response[1], 0x00) << "message_size byte 1";
    EXPECT_EQ(response[2], 0x00) << "message_size byte 2";
    EXPECT_EQ(response[3], 0x00) << "message_size byte 3";
    EXPECT_EQ(response[4], 0x00) << "correlation_id byte 0";
    EXPECT_EQ(response[5], 0x00) << "correlation_id byte 1";
    EXPECT_EQ(response[6], 0x00) << "correlation_id byte 2";
    EXPECT_EQ(response[7], 0x07) << "correlation_id byte 3";
}
