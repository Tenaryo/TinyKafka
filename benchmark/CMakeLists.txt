cmake_minimum_required(VERSION 3.21)

project(producer_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(producer_bench producer_bench.cpp)
target_compile_options(producer_bench PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O3>)
target_compile_features(producer_bench PRIVATE cxx_std_23)
target_compile_definitions(producer_bench PRIVATE _GNU_SOURCE)

add_executable(consumer_bench consumer_bench.cpp)
target_compile_options(consumer_bench PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O3>)
target_compile_features(consumer_bench PRIVATE cxx_std_23)
target_compile_definitions(consumer_bench PRIVATE _GNU_SOURCE)
