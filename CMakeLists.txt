cmake_minimum_required(VERSION 3.21)

project(TinyKafka VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(TinyKafka_core STATIC
    src/broker/broker.cpp
    src/cluster/metadata.cpp
    src/net/server.cpp
    src/net/socket.cpp
    src/protocol/parser.cpp
    src/protocol/serializer.cpp
    src/storage/log_reader.cpp
    src/storage/log_writer.cpp
)
target_include_directories(TinyKafka_core PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_compile_features(TinyKafka_core PUBLIC cxx_std_23)
target_compile_options(TinyKafka_core PUBLIC
    -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O3>
)

add_executable(kafka src/main.cpp)
target_link_libraries(kafka PRIVATE TinyKafka_core)

option(ENABLE_SANITIZERS "Enable Address and Undefined Behavior Sanitizers" OFF)

if(ENABLE_SANITIZERS)
    target_compile_options(TinyKafka_core INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(TinyKafka_core INTERFACE -fsanitize=address,undefined)
endif()

option(ENABLE_COVERAGE "Enable code coverage instrumentation (GCC)" OFF)

if(ENABLE_COVERAGE)
    if(ENABLE_SANITIZERS)
        message(FATAL_ERROR "ENABLE_COVERAGE and ENABLE_SANITIZERS are mutually exclusive")
    endif()
    target_compile_options(TinyKafka_core PUBLIC --coverage)
    target_link_options(TinyKafka_core PUBLIC --coverage)
endif()

option(BUILD_TESTS "Build tests" ON)
if(BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    include(GoogleTest)
    enable_testing()
    add_subdirectory(tests)
endif()
