#include "logging/logger.hpp"

#include <chrono>
#include <format>
#include <iostream>
#include <mutex>
#include <thread>
#include <unistd.h>

namespace logging {

namespace {

std::mutex g_log_mutex; // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto current_timestamp() -> std::string {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() %
        1000;

    std::tm utc_tm{};
    gmtime_r(&time, &utc_tm);

    return std::format("{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:03d}",
                       utc_tm.tm_year + 1900,
                       utc_tm.tm_mon + 1,
                       utc_tm.tm_mday,
                       utc_tm.tm_hour,
                       utc_tm.tm_min,
                       utc_tm.tm_sec,
                       ms);
}

auto thread_id_str() -> std::string { return std::to_string(gettid()); }

void write_log(std::string_view level, std::string_view msg, uint32_t request_id) {
    auto ts = current_timestamp();
    auto tid = thread_id_str();

    std::lock_guard lock(g_log_mutex);

    if (request_id != 0) {
        std::cerr << '[' << ts << "][" << level << "][T:" << tid << "][R:" << request_id << "] "
                  << msg << '\n';
    } else {
        std::cerr << '[' << ts << "][" << level << "][T:" << tid << "] " << msg << '\n';
    }
}

} // namespace

void debug(std::string_view msg, uint32_t request_id) { write_log("DEBUG", msg, request_id); }
void info(std::string_view msg, uint32_t request_id) { write_log("INFO ", msg, request_id); }
void warn(std::string_view msg, uint32_t request_id) { write_log("WARN ", msg, request_id); }
void error(std::string_view msg, uint32_t request_id) { write_log("ERROR", msg, request_id); }

} // namespace logging
