#pragma once

#include <liburing.h>
#include <coroutine>

namespace net {

class UringWriteAwaiter {
  public:
    UringWriteAwaiter(io_uring* ring, int fd, const void* buf, unsigned int len, off_t offset)
        : ring_(ring), fd_(fd), buf_(buf), len_(len), offset_(offset) {}

    auto await_ready() -> bool { return false; }

    void await_suspend(std::coroutine_handle<> handle) {
        io_uring_sqe* sqe = io_uring_get_sqe(ring_);
        io_uring_prep_write(sqe, fd_, buf_, len_, offset_);
        io_uring_sqe_set_data(sqe, handle.address());
        io_uring_submit(ring_);
    }

    auto await_resume() -> int { return result_; }

    void set_result(int r) { result_ = r; }

  private:
    io_uring* ring_;
    int fd_;
    const void* buf_;
    unsigned int len_;
    off_t offset_;
    int result_{0};
};

} // namespace net
