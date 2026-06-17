#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/broker.hpp"
#include "cluster/metadata.hpp"

namespace config {
struct Config;
}

namespace net {

class EpollReactor {
  public:
    EpollReactor(const config::Config& config, ClusterMetadata metadata, int server_fd);
    ~EpollReactor();

    EpollReactor(const EpollReactor&) = delete;
    EpollReactor& operator=(const EpollReactor&) = delete;
    EpollReactor(EpollReactor&&) = delete;
    EpollReactor& operator=(EpollReactor&&) = delete;

    void run();

    [[nodiscard]] auto epoll_fd() const -> int { return epoll_fd_; }
  private:
    struct Connection {
        std::vector<uint8_t> read_buf;
        size_t expected_len = 0;
        bool have_header = false;
        std::vector<uint8_t> write_buf;
        size_t write_offset = 0;
    };

    void handle_accept();
    void handle_read(int fd);
    void handle_write(int fd);
    void close_connection(int fd);

    int epoll_fd_;
    int server_fd_;
    Broker broker_;
    uint32_t max_message_bytes_;
    size_t max_write_buffer_bytes_;
    std::unordered_map<int, Connection> connections_;
};

} // namespace net
