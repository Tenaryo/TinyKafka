#include <array>
#include <cstdio>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

#include <gtest/gtest.h>

#include "logging/logger.hpp"

namespace {

class ScopedStderrCapture {
  public:
    ScopedStderrCapture() {
        fflush(stderr);
        saved_fd_ = dup(STDERR_FILENO);
        pipe(pipe_fd_);
        dup2(pipe_fd_[1], STDERR_FILENO);
        close(pipe_fd_[1]);
    }

    ~ScopedStderrCapture() {
        fflush(stderr);
        dup2(saved_fd_, STDERR_FILENO);
        close(saved_fd_);
    }

    auto get_output() -> std::string {
        fflush(stderr);
        std::array<char, 65536> buf{};
        auto n = static_cast<size_t>(read(pipe_fd_[0], buf.data(), buf.size() - 1));
        close(pipe_fd_[0]);
        if (n > 0) {
            buf[n] = '\0';
            return buf.data();
        }
        return {};
    }
  private:
    int saved_fd_;
    int pipe_fd_[2];
};

} // namespace

TEST(LoggerTest, LevelLabels) {
    ScopedStderrCapture capture;

    logging::debug("debug msg");
    logging::info("info msg");
    logging::warn("warn msg");
    logging::error("error msg");

    auto output = capture.get_output();
    EXPECT_NE(output.find("[DEBUG]"), std::string::npos);
    EXPECT_NE(output.find("[INFO ]"), std::string::npos);
    EXPECT_NE(output.find("[WARN ]"), std::string::npos);
    EXPECT_NE(output.find("[ERROR]"), std::string::npos);
}

TEST(LoggerTest, TimestampFormat) {
    ScopedStderrCapture capture;

    logging::info("timestamp test");

    auto output = capture.get_output();
    std::regex ts_re(R"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\])");
    EXPECT_TRUE(std::regex_search(output, ts_re));
}

TEST(LoggerTest, RequestIdOptional) {
    ScopedStderrCapture capture;

    logging::info("with id", 42);
    logging::info("without id");

    auto output = capture.get_output();
    EXPECT_NE(output.find("[R:42]"), std::string::npos);

    auto with_r = output.find("[R:42]");
    auto first_line = output.substr(0, with_r);
    EXPECT_EQ(first_line.find("[R:"), std::string::npos);
}

TEST(LoggerTest, ThreadSafety) {
    ScopedStderrCapture capture;

    constexpr int kThreadCount = 4;
    constexpr int kMessagesPerThread = 10;
    std::array<std::thread, kThreadCount> threads;

    for (int t = 0; t < kThreadCount; ++t) {
        threads[static_cast<size_t>(t)] = std::thread([t] {
            for (int i = 0; i < kMessagesPerThread; ++i) {
                logging::info("t" + std::to_string(t));
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    auto output = capture.get_output();
    std::istringstream stream(output);
    std::string line;
    std::regex log_re(R"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\]\[INFO \]\[T:\d+\].*)");
    int line_count = 0;
    while (std::getline(stream, line)) {
        if (line.empty())
            continue;
        ++line_count;
        EXPECT_TRUE(std::regex_match(line, log_re)) << "Malformed log line: " << line;
    }
    EXPECT_EQ(line_count, kThreadCount * kMessagesPerThread);
}
