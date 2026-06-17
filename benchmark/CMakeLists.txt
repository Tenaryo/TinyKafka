cmake_minimum_required(VERSION 3.21)

project(producer_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(producer_bench producer_bench.cpp)
target_compile_options(producer_bench PRIVATE -Wall -Wextra -Wpedantic)
target_compile_features(producer_bench PRIVATE cxx_std_23)
target_compile_definitions(producer_bench PRIVATE _GNU_SOURCE)
