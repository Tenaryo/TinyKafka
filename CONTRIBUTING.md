# Contributing to TinyKafka

## Getting Started

```bash
git clone https://github.com/Tenaryo/TinyKafka.git
cd TinyKafka
./build.sh          # Debug build
./run_tests.sh      # Run all tests
```

### Prerequisites

- GCC 13+ or Clang 17+
- CMake 3.21+
- Ninja

```bash
sudo apt install g++-14 ninja-build cmake
```

## Development Workflow

### Build

```bash
./build.sh              # Debug (default)
./build.sh Release      # Release build
./build.sh Debug clang++-18  # Clang build
```

### Testing

```bash
./run_tests.sh              # Run all tests (Debug)
./run_tests.sh --sanitize   # Run with ASan + UBSan
./run_tests.sh --coverage   # Run with coverage report
./run_tests.sh --release    # Run Release build tests
```

### CMake Presets

IDE users can select presets directly:

```bash
cmake --preset debug
cmake --build --preset debug
ctest --preset debug
```

Available presets: `debug`, `release`, `sanitize`, `coverage`, `clang-tidy`.

## Code Style

- **C++ Standard**: C++23
- **Formatting**: `.clang-format` (LLVM-based, 4-space indent, 100-column limit)
- **Static Analysis**: `.clang-tidy` (enable with `-DENABLE_CLANG_TIDY=ON`)

Before submitting a PR, ensure:

```bash
# Format check
find src tests \( -name '*.cpp' -o -name '*.hpp' \) -print0 | xargs -0 clang-format --dry-run --Werror

# All tests pass
./run_tests.sh
./run_tests.sh --sanitize
./run_tests.sh --release
```

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

| Type | Use |
|------|-----|
| `feat:` | New feature |
| `fix:` | Bug fix |
| `refactor:` | Code restructuring |
| `test:` | Test additions/improvements |
| `build:` | Build system, dependencies |
| `docs:` | Documentation |
| `ci:` | CI configuration |
| `chore:` | Maintenance |

## Pull Requests

1. Create a feature branch from `master`
2. Make focused commits with clear messages
3. Ensure all tests pass locally
4. Open a PR using the template
5. CI must pass before review

## Project Structure

```
src/          # Core library + executable
tests/        # Unit + integration tests
docs/         # Design docs, roadmap
build.sh      # Build script
run_tests.sh  # Test runner
```

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
