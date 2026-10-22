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
# Targets are added one build step at a time:
#   Step 1-3: rafty core library (raft.cpp, storage.cpp). Depends on NO
#             networking library: raft talks only to Transport/Storage/Clock
#             interfaces, so unit and integration tests run on a simulated
#             transport with a fake clock, deterministically.
#   Step 4+:  protobuf + gRPC (find_package) for kv_server, kv_client, apps.
#   Step 7:   opentelemetry-cpp for telemetry.
# ---------------------------------------------------------------------------

# Raft consensus core plus its storage. Links nothing but the standard
# library. storage.cpp is the only file here that touches the disk.
add_library(rafty
  src/raft.cpp
  src/storage.cpp)
target_include_directories(rafty PUBLIC ${CMAKE_SOURCE_DIR}/inc)
target_link_libraries(rafty PRIVATE raft_kv_warnings)

# KV layer: the state machine applied on top of Raft's committed log.
# Depends on rafty only for its types (LogEntry, Index); Raft itself knows
# nothing about keys and values.
add_library(kv
  src/state_machine.cpp
  src/kv_server.cpp
  src/kv_client.cpp)
target_link_libraries(kv PUBLIC rafty PRIVATE raft_kv_warnings)

# ---------------------------------------------------------------------------
# Networking (step 4d): gRPC + Protocol Buffers
#
# Only the `proto_gen` and (later) `net` libraries link gRPC. `rafty` and `kv`
# stay free of it: they speak plain C++ structs, and a converter maps those
# to/from proto at the boundary. The core can still be tested with no network.
# ---------------------------------------------------------------------------
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# C++ code generated from proto/*.proto at build time:
#   raft.pb.h/.cc, kv.pb.h/.cc         messages   (protoc)
#   raft.grpc.pb.h/.cc, kv.grpc.pb.h/.cc  services (gRPC plugin)
# Output goes to the build directory; include as "raft.pb.h" etc.
add_library(proto_gen
  proto/raft.proto
  proto/kv.proto)
target_link_libraries(proto_gen PUBLIC protobuf::libprotobuf gRPC::grpc++)
# SYSTEM: our strict warnings must not fire on generated headers we include.
target_include_directories(proto_gen SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
protobuf_generate(TARGET proto_gen LANGUAGE cpp
  IMPORT_DIRS ${CMAKE_SOURCE_DIR}/proto)
protobuf_generate(TARGET proto_gen LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
  IMPORT_DIRS ${CMAKE_SOURCE_DIR}/proto)
# Generated code is not ours: no raft_kv_warnings on this target.

# ASan + Homebrew protobuf: abseil (which protobuf uses) changes the memory
# layout of some internal types when compiled under ASan. The prebuilt
# protobuf/abseil libraries were built WITHOUT ASan, so ASan-compiled code
# that includes their headers disagrees with them about object layout and
# crashes inside libprotobuf. Fix: compile everything that includes protobuf
# headers without ASan. That is only glue (generated code, converter, gRPC
# wiring); rafty, kv, and the simulator stay fully ASan-checked.
# (TSan does not change those layouts, so it needs no exception.)
set(NO_ASAN_FOR_PROTOBUF "")
if(SANITIZE STREQUAL "address")
  set(NO_ASAN_FOR_PROTOBUF -fno-sanitize=address)
endif()
target_compile_options(proto_gen PRIVATE ${NO_ASAN_FOR_PROTOBUF})

# The network edge: struct <-> proto conversion (and, next, the gRPC
# transport). The only library besides proto_gen that sees protobuf.
add_library(net
  src/net/convert.cpp
  src/net/grpc_transport.cpp
  src/net/kv_node.cpp)
target_link_libraries(net PUBLIC kv proto_gen PRIVATE raft_kv_warnings)
target_compile_options(net PRIVATE ${NO_ASAN_FOR_PROTOBUF})

# ---------------------------------------------------------------------------
# Apps (built into build/app/)
# ---------------------------------------------------------------------------
add_executable(kv_node app/kv_node.cpp)
target_link_libraries(kv_node PRIVATE net raft_kv_warnings)
add_executable(kv_cli app/kv_cli.cpp)
target_link_libraries(kv_cli PRIVATE net raft_kv_warnings)
set_target_properties(kv_node kv_cli PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/app)
target_compile_options(kv_node PRIVATE ${NO_ASAN_FOR_PROTOBUF})
target_compile_options(kv_cli PRIVATE ${NO_ASAN_FOR_PROTOBUF})

# Test tooling (FakeClock, SimNetwork, SimCluster) plus the linearizability
# checker.
add_library(toolings
  src/lin_checker.cpp)
target_link_libraries(toolings PUBLIC rafty PRIVATE raft_kv_warnings)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

# Unit tests: one Raft node driven by hand.
add_executable(unit_tests
  unittests/raft_election_test.cpp
  unittests/raft_log_test.cpp
  unittests/raft_read_test.cpp
  unittests/storage_test.cpp
  unittests/state_machine_test.cpp
  unittests/msg_queue_test.cpp
  unittests/convert_test.cpp
  unittests/lin_checker_test.cpp)
# toolings and net (via kv) already bring in rafty; listing it too makes Apple ld warn
# about duplicate libraries.
target_link_libraries(unit_tests PRIVATE toolings net GTest::gtest_main
                                         raft_kv_warnings)
# The only unit test file that includes protobuf headers (see proto_gen note).
set_source_files_properties(unittests/convert_test.cpp PROPERTIES
  COMPILE_OPTIONS "${NO_ASAN_FOR_PROTOBUF}")
gtest_discover_tests(unit_tests)

# Integration tests: whole clusters on the simulated network.
add_executable(integration_tests
  integration_tests/election_test.cpp
  integration_tests/replication_test.cpp
  integration_tests/recovery_test.cpp
  integration_tests/kv_test.cpp)
target_link_libraries(integration_tests PRIVATE toolings kv
                                                GTest::gtest_main
                                                raft_kv_warnings)
# The chaos test runs 200 seeds; give it room under sanitizers.
gtest_discover_tests(integration_tests PROPERTIES TIMEOUT 600)
