cmake_minimum_required(VERSION 3.20)
project(raft_kv LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Language and toolchain settings
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)          # -std=c++20, not -std=gnu++20

# Emits build/compile_commands.json so clangd / VS Code can resolve includes.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# Warnings apply to our own targets only (via raft_kv_warnings), so noisy
# third-party headers such as gRPC do not break the build.
add_library(raft_kv_warnings INTERFACE)
target_compile_options(raft_kv_warnings INTERFACE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wno-sign-conversion)

# ---------------------------------------------------------------------------
# Sanitizers
#   cmake .. -DSANITIZE=address    -> ASan + UBSan (memory errors, UB)
#   cmake .. -DSANITIZE=thread     -> TSan (data races)
# ASan and TSan cannot be combined in one binary, so they are separate passes.
# Use a separate build dir per mode, e.g. build-asan/, build-tsan/.
# ---------------------------------------------------------------------------
set(SANITIZE "" CACHE STRING "Sanitizer: '', address, thread")
set_property(CACHE SANITIZE PROPERTY STRINGS "" address thread)

if(SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(NOT SANITIZE STREQUAL "")
  message(FATAL_ERROR "Unknown SANITIZE value '${SANITIZE}' (use address or thread)")
endif()

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
enable_testing()

# ---------------------------------------------------------------------------
# Targets are added one build step at a time:
#   Step 1-3: rafty core library (raft.cpp, storage.cpp). Depends on NO
#             networking library: raft talks only to Transport/Storage/Clock
#             interfaces, so unit and integration tests run on a simulated
#             transport with a fake clock, deterministically.
#   Step 4+:  protobuf + gRPC (find_package) for kv_server, kv_client, apps.
#   Step 7:   opentelemetry-cpp for telemetry.
# ---------------------------------------------------------------------------
