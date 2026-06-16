#!/usr/bin/env bash
set -euo pipefail

BUILD_DIR="build"
COVERAGE_MODE=false

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

if [[ "${1:-}" == "--coverage" ]]; then
    COVERAGE_MODE=true
fi

if $COVERAGE_MODE; then
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Running Test Suite with Coverage${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    echo -e "${YELLOW}Configuring CMake with coverage...${NC}"
    cmake -B "$BUILD_DIR" -S . -G Ninja \
        -DCMAKE_BUILD_TYPE=Debug \
        -DENABLE_COVERAGE=ON
else
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Running Test Suite${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    mkdir -p "$BUILD_DIR"
    if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
        echo -e "${YELLOW}Configuring CMake...${NC}"
        cmake -B "$BUILD_DIR" -S . -G Ninja -DCMAKE_BUILD_TYPE=Debug
    fi
fi

echo -e "${YELLOW}Building tests...${NC}"
cmake --build "$BUILD_DIR" -j$(nproc)
echo

if $COVERAGE_MODE; then
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

if $COVERAGE_MODE; then
    echo
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Generating Coverage Report${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo

    echo -e "${YELLOW}Capturing coverage data with lcov...${NC}"
    lcov --capture \
        --directory "$BUILD_DIR" \
        --output-file "$BUILD_DIR/coverage_raw.info" \
        --gcov-tool gcov-14

    echo -e "${YELLOW}Filtering project sources...${NC}"
    lcov --remove "$BUILD_DIR/coverage_raw.info" \
        '/usr/*' \
        '*/_deps/*' \
        '*/gtest/*' \
        '*/gmock/*' \
        --output-file "$BUILD_DIR/coverage.info"

    echo
    echo -e "${YELLOW}Generating HTML report...${NC}"
    genhtml "$BUILD_DIR/coverage.info" \
        --output-directory "$BUILD_DIR/coverage" \
        --title "TinyKafka Coverage" \
        --legend

    echo
    COVERAGE_PCT=$(lcov --summary "$BUILD_DIR/coverage.info" 2>&1 | grep lines | awk '{print $2}' | tr -d '%')
    echo -e "${GREEN}Coverage report generated:${NC} ${BUILD_DIR}/coverage/index.html"
    echo -e "${GREEN}Line coverage: ${COVERAGE_PCT}%${NC}"
fi
