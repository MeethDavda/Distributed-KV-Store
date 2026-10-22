#pragma once

// Converts between the core's plain C++ structs and the generated protobuf
// messages. This is the ONLY place the two meet: rafty and kv never include
// a .pb.h, and the gRPC code never touches Raft internals.
//
// Invariant N3: a round trip (struct -> proto -> bytes -> proto -> struct)
// loses nothing. Tested in unittests/convert_test.cpp.
//
// from_proto() returns nullopt for messages that cannot be valid (no payload
// set, unknown enum value). Such input can only come from a buggy or foreign
// sender, and is dropped rather than guessed at.

#include <optional>

#include "kv.pb.h"
#include "kv/kv_server.hpp"
#include "rafty/types.hpp"
#include "raft.pb.h"

namespace net {

raftpb::RaftMessage to_proto(const rafty::Message& m);
std::optional<rafty::Message> from_proto(const raftpb::RaftMessage& p);

kvpb::KvRequest to_proto(const kv::KvRequest& r);
std::optional<kv::KvRequest> from_proto(const kvpb::KvRequest& p);

kvpb::KvResponse to_proto(const kv::KvResponse& r);
std::optional<kv::KvResponse> from_proto(const kvpb::KvResponse& p);

}  // namespace net
