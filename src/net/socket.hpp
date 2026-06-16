#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <system_error>

auto send_all(int fd,
              std::span<const std::uint8_t> data) -> std::expected<std::size_t, std::error_code>;

auto recv_all(int fd, std::span<std::uint8_t> buf) -> std::expected<std::size_t, std::error_code>;

void set_nonblocking(int fd);
