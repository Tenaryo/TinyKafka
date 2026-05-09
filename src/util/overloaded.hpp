#pragma once

template <typename... Ts> struct overloaded : Ts... {
    using Ts::operator()...;
};
