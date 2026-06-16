#pragma once

template <typename... Ts> struct Overloaded : Ts... {
    using Ts::operator()...;
};
