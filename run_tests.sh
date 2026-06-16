#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="build"
MODE="default"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

while [[ $# -gt 0 ]]; do
    case "$1" in
        --coverage) MODE="coverage" ;;
        --sanitize) MODE="sanitize" ;;
        --release) MODE="release" ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: $0 [--coverage|--sanitize|--release]"
            exit 1
            ;;
    esac
    shift
done

case "$MODE" in
sanitize) BUILD_DIR="build/sanitize" ;;
release)  BUILD_DIR="build/release" ;;
coverage) BUILD_DIR="build/coverage" ;;
*)        BUILD_DIR="build/debug" ;;
esac

case "$MODE" in
sanitize)
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Running Test Suite with Sanitizers${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    NEED_RECONFIGURE=true
    if [ -f "$BUILD_DIR/CMakeCache.txt" ] && grep -q 'ENABLE_SANITIZERS:BOOL=ON' "$BUILD_DIR/CMakeCache.txt"; then
        NEED_RECONFIGURE=false
    fi
    if $NEED_RECONFIGURE; then
        echo -e "${YELLOW}Configuring CMake with sanitizers...${NC}"
        cmake -B "$BUILD_DIR" -S . -G Ninja \
            -DCMAKE_BUILD_TYPE=Debug \
            -DENABLE_SANITIZERS=ON
    fi
    export ASAN_OPTIONS=detect_leaks=1:abort_on_error=1
    export UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1
    ;;
release)
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Running Test Suite (Release)${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    NEED_RECONFIGURE=true
    if [ -f "$BUILD_DIR/CMakeCache.txt" ] && grep -q 'CMAKE_BUILD_TYPE.*=Release' "$BUILD_DIR/CMakeCache.txt"; then
        NEED_RECONFIGURE=false
    fi
    if $NEED_RECONFIGURE; then
        echo -e "${YELLOW}Configuring CMake (Release)...${NC}"
        cmake -B "$BUILD_DIR" -S . -G Ninja -DCMAKE_BUILD_TYPE=Release
    fi
    ;;
coverage)
    if ! command -v lcov >/dev/null 2>&1 || ! command -v genhtml >/dev/null 2>&1; then
        echo -e "${RED}lcov and genhtml are required. Install with: sudo apt install lcov${NC}"
        exit 1
    fi

    GCOV_TOOL="gcov"
    GCC_VER=$(c++ -dumpversion | cut -d. -f1)
    if command -v "gcov-${GCC_VER}" >/dev/null 2>&1; then
        GCOV_TOOL="gcov-${GCC_VER}"
    fi

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Running Test Suite with Coverage${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    NEED_RECONFIGURE=true
    if [ -f "$BUILD_DIR/CMakeCache.txt" ] && grep -q 'ENABLE_COVERAGE:BOOL=ON' "$BUILD_DIR/CMakeCache.txt"; then
        NEED_RECONFIGURE=false
    fi
    if $NEED_RECONFIGURE; then
        echo -e "${YELLOW}Configuring CMake with coverage...${NC}"
        cmake -B "$BUILD_DIR" -S . -G Ninja \
            -DCMAKE_BUILD_TYPE=Debug \
            -DENABLE_COVERAGE=ON
    fi
    ;;
*)
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Running Test Suite${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    mkdir -p "$BUILD_DIR"
    if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
        echo -e "${YELLOW}Configuring CMake...${NC}"
        cmake -B "$BUILD_DIR" -S . -G Ninja -DCMAKE_BUILD_TYPE=Debug
    fi
    ;;
esac

echo -e "${YELLOW}Building tests...${NC}"
cmake --build "$BUILD_DIR" -j$(nproc)
echo

if [ "$MODE" = "coverage" ]; then
    echo -e "${YELLOW}Cleaning stale coverage data...${NC}"
    find "$BUILD_DIR" -name '*.gcda' -delete 2>/dev/null || true
    echo
fi

echo -e "${YELLOW}Running tests via ctest...${NC}"
echo

if ctest --test-dir "$BUILD_DIR" --output-on-failure -j$(nproc); then
    echo
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi

if [ "$MODE" = "coverage" ]; then
    echo
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Generating Coverage Report${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    echo -e "${YELLOW}Capturing coverage data with lcov (${GCOV_TOOL})...${NC}"
    lcov --capture \
        --directory "$BUILD_DIR" \
        --output-file "$BUILD_DIR/coverage_raw.info" \
        --gcov-tool "$GCOV_TOOL"

    echo -e "${YELLOW}Filtering project sources...${NC}"
    lcov --remove "$BUILD_DIR/coverage_raw.info" \
        '/usr/*' \
        '*/_deps/*' \
        --output-file "$BUILD_DIR/coverage.info"

    echo
    echo -e "${YELLOW}Generating HTML report...${NC}"
    genhtml "$BUILD_DIR/coverage.info" \
        --output-directory "$BUILD_DIR/report" \
        --title "TinyKafka Coverage" \
        --legend

    echo
    COVERAGE_PCT=$(lcov --summary "$BUILD_DIR/coverage.info" 2>&1 | grep lines | awk '{print $2}' | tr -d '%')
    echo -e "${GREEN}Coverage report generated:${NC} ${BUILD_DIR}/report/index.html"
    echo -e "${GREEN}Line coverage: ${COVERAGE_PCT}%${NC}"
fi
