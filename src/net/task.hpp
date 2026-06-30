#pragma once

#include <coroutine>
#include <exception>
#include <utility>

namespace net {

struct Task {
    struct promise_type {
        Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }
    };

    explicit Task(std::coroutine_handle<promise_type> h) : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }
    Task(Task&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    Task& operator=(Task&& o) noexcept { if (this != &o) { if (handle_) handle_.destroy(); handle_ = std::exchange(o.handle_, nullptr); } return *this; }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    auto get_handle() const { return handle_; }

  private:
    std::coroutine_handle<promise_type> handle_;
};

} // namespace net
