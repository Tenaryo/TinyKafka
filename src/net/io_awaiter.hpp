#pragma once

#include <liburing.h>
#include <coroutine>
#include <unistd.h>

namespace net {

class UringWriteAwaiter {
  public:
    UringWriteAwaiter(io_uring* ring, int fd, const void* buf, unsigned int len, off_t offset)
        : ring_(ring), fd_(fd), buf_(buf), len_(len), offset_(offset) {}

    auto await_ready() -> bool {
        sqe_ = io_uring_get_sqe(ring_);
        if (!sqe_) [[unlikely]] {
            result_ = static_cast<int>(::write(fd_, buf_, len_));
            return true;
        }
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) {
        coro_ = handle;
        io_uring_prep_write(sqe_, fd_, buf_, len_, offset_);
        io_uring_sqe_set_data(sqe_, this);
        io_uring_submit(ring_);
    }

    auto await_resume() -> int { return result_; }

    void set_result(int r) { result_ = r; }
    auto coroutine_handle() const -> std::coroutine_handle<> { return coro_; }
    void resume_continuation() { coro_.resume(); }

  private:
    io_uring* ring_;
    int fd_;
    const void* buf_;
    unsigned int len_;
    off_t offset_;
    int result_{0};
    std::coroutine_handle<> coro_;
    io_uring_sqe* sqe_{nullptr};
};

} // namespace net
