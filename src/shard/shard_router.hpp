#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace shard {

class ShardRouter {
  public:
    explicit ShardRouter(size_t reactor_count) noexcept : reactor_count_(reactor_count) {}

    [[nodiscard]] auto route(const std::string& topic_name, int32_t partition) const -> size_t {
        size_t h = std::hash<std::string>{}(topic_name);
        h ^= static_cast<size_t>(partition) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h % reactor_count_;
    }

    [[nodiscard]] auto reactor_count() const -> size_t { return reactor_count_; }
  private:
    size_t reactor_count_;
};

} // namespace shard
