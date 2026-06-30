#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace net {

template <typename T = void>
struct Task;

namespace detail {

struct TaskPromiseBase {
    std::coroutine_handle<> continuation_;

    auto initial_suspend() noexcept { return std::suspend_never{}; }

    struct FinalAwaiter {
        bool await_ready() noexcept { return false; }
        template <typename Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> h) noexcept {
            auto& promise = h.promise();
            if (promise.continuation_) {
                return promise.continuation_;
            }
            return std::noop_coroutine();
        }
        void await_resume() noexcept {}
    };

    auto final_suspend() noexcept { return FinalAwaiter{}; }
    void unhandled_exception() { std::terminate(); }
};

template <typename T>
struct TaskValue {
    std::optional<T> value_;
};

template <>
struct TaskValue<void> {};

} // namespace detail

template <typename T>
struct Task {
    struct promise_type : detail::TaskPromiseBase, detail::TaskValue<T> {
        auto get_return_object() -> Task {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        void return_value(T v) {
            this->detail::TaskValue<T>::value_ = std::move(v);
        }

        T result() {
            return std::move(*(this->detail::TaskValue<T>::value_));
        }
    };

    explicit Task(std::coroutine_handle<promise_type> h) : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }
    Task(Task&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    auto handle() const { return handle_; }
    bool done() const { return handle_ && handle_.done(); }

    T result() {
        if constexpr (!std::is_void_v<T>) {
            return handle_.promise().result();
        }
    }

    auto take_handle() { return std::exchange(handle_, nullptr); }

    bool await_ready() { return handle_ && handle_.done(); }

    void await_suspend(std::coroutine_handle<> awaiting) {
        handle_.promise().continuation_ = awaiting;
    }

    decltype(auto) await_resume() {
        if constexpr (std::is_void_v<T>) {
            return;
        } else {
            return result();
        }
    }

private:
    std::coroutine_handle<promise_type> handle_;
};

template <>
struct Task<void> {
    struct promise_type : detail::TaskPromiseBase {
        auto get_return_object() -> Task {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        void return_void() {}
    };

    explicit Task(std::coroutine_handle<promise_type> h) : handle_(h) {}
    ~Task() { if (handle_) handle_.destroy(); }
    Task(Task&& o) noexcept : handle_(std::exchange(o.handle_, nullptr)) {}
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            if (handle_) handle_.destroy();
            handle_ = std::exchange(o.handle_, nullptr);
        }
        return *this;
    }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    auto handle() const { return handle_; }
    bool done() const { return handle_ && handle_.done(); }

    auto take_handle() { return std::exchange(handle_, nullptr); }

    bool await_ready() { return handle_ && handle_.done(); }

    void await_suspend(std::coroutine_handle<> awaiting) {
        handle_.promise().continuation_ = awaiting;
    }

    void await_resume() {}

private:
    std::coroutine_handle<promise_type> handle_;
};

} // namespace net
