#pragma once

#include <cstdint>
#include <deque>
#include <mutex>
#include <unistd.h>
#include <vector>

#include "protocol/request.hpp"

namespace shard {

struct ForwardedRequest {
    int client_fd{};
    size_t source_reactor_id{};
    Request request;
};

struct ForwardedResponse {
    int client_fd{};
    std::vector<uint8_t> data;
    int splice_fd{-1};
    unsigned splice_len{0};
};

class CrossReactorQueues {
  public:
    void set_wakeup_fd(int fd) { wakeup_fd_ = fd; }

    void push_request(ForwardedRequest req) {
        {
            std::lock_guard lock(request_mutex_);
            requests_.push_back(std::move(req));
        }
        wake();
    }

    [[nodiscard]] auto try_pop_request(ForwardedRequest& out) -> bool {
        std::lock_guard lock(request_mutex_);
        if (requests_.empty()) {
            return false;
        }
        out = std::move(requests_.front());
        requests_.pop_front();
        return true;
    }

    void push_response(ForwardedResponse resp) {
        {
            std::lock_guard lock(response_mutex_);
            responses_.push_back(std::move(resp));
        }
        wake();
    }

    [[nodiscard]] auto try_pop_response(ForwardedResponse& out) -> bool {
        std::lock_guard lock(response_mutex_);
        if (responses_.empty()) {
            return false;
        }
        out = std::move(responses_.front());
        responses_.pop_front();
        return true;
    }
  private:
    void wake() { // NOLINT(readability-make-member-function-const)
        if (wakeup_fd_ >= 0) {
            uint64_t val = 1;
            [[maybe_unused]] auto _ = ::write(wakeup_fd_, &val, sizeof(val));
        }
    }

    std::mutex request_mutex_;
    std::deque<ForwardedRequest> requests_;
    std::mutex response_mutex_;
    std::deque<ForwardedResponse> responses_;
    int wakeup_fd_{-1};
};

} // namespace shard
