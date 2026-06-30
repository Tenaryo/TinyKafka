#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace util {

class Arena {
  public:
    static constexpr size_t kChunkSize = 65536;

    Arena() { new_chunk(); }

    [[nodiscard]] auto allocate(size_t size) -> std::span<uint8_t> {
        if (offset_ + size > chunks_.back().size()) {
            new_chunk();
        }
        auto ptr = chunks_.back().data() + offset_;
        offset_ += size;
        return {ptr, size};
    }

  private:
    void new_chunk() {
        chunks_.emplace_back(kChunkSize, uint8_t{0});
        offset_ = 0;
    }

    std::vector<std::vector<uint8_t>> chunks_;
    size_t offset_{0};
};

} // namespace util
