cmake_minimum_required(VERSION 3.21)

project(TinyKafka VERSION 1.0.0 LANGUAGES CXX)

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
target_compile_options(TinyKafka_core PRIVATE
    -Wall -Wextra -Wpedantic -Werror -Wshadow -Wconversion
    $<$<CONFIG:Debug>:-g -O0>
    $<$<CONFIG:Release>:-O3>
)

add_executable(kafka src/main.cpp)
target_link_libraries(kafka PRIVATE TinyKafka_core)

option(ENABLE_SANITIZERS "Enable Address and Undefined Behavior Sanitizers" OFF)
option(ENABLE_TSAN "Enable Thread Sanitizer" OFF)

if(ENABLE_SANITIZERS AND ENABLE_TSAN)
    message(FATAL_ERROR "ENABLE_SANITIZERS and ENABLE_TSAN are mutually exclusive")
endif()

if(ENABLE_SANITIZERS)
    target_compile_options(TinyKafka_core PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(TinyKafka_core PUBLIC -fsanitize=address,undefined)
endif()

if(ENABLE_TSAN)
    target_compile_options(TinyKafka_core PUBLIC -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(TinyKafka_core PUBLIC -fsanitize=thread)
endif()

option(ENABLE_COVERAGE "Enable code coverage instrumentation (GCC)" OFF)

if(ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        message(WARNING "ENABLE_COVERAGE uses GCC's --coverage flag. "
                        "For Clang, use ENABLE_COVERAGE=OFF and llvm-cov instead.")
    endif()
    if(ENABLE_SANITIZERS)
        message(FATAL_ERROR "ENABLE_COVERAGE and ENABLE_SANITIZERS are mutually exclusive")
    endif()
    if(ENABLE_TSAN)
        message(FATAL_ERROR "ENABLE_COVERAGE and ENABLE_TSAN are mutually exclusive")
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
    if(CMAKE_CXX_CLANG_TIDY)
        set_target_properties(gtest PROPERTIES CXX_CLANG_TIDY "")
        set_target_properties(gtest_main PROPERTIES CXX_CLANG_TIDY "")
        set_target_properties(gmock PROPERTIES CXX_CLANG_TIDY "")
        set_target_properties(gmock_main PROPERTIES CXX_CLANG_TIDY "")
    endif()
    include(GoogleTest)
    enable_testing()
    add_subdirectory(tests)
endif()

option(ENABLE_CLANG_TIDY "Enable clang-tidy static analysis" OFF)
if(ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE clang-tidy REQUIRED)
    set(CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_EXE}")
endif()

include(GNUInstallDirs)
install(TARGETS kafka RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

add_custom_target(setup_precommit
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${CMAKE_SOURCE_DIR}/scripts/pre-commit
        ${CMAKE_SOURCE_DIR}/.git/hooks/pre-commit
    COMMAND chmod +x ${CMAKE_SOURCE_DIR}/.git/hooks/pre-commit
    COMMENT "Installing pre-commit hook (clang-format + clang-tidy)"
)
