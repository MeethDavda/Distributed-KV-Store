// Round-trip tests for the struct <-> proto converter (invariant N3).
//
// Every test goes struct -> proto -> serialized bytes -> proto -> struct,
// the full path a message takes across the network, and then compares every
// field. Values are all distinct and non-zero, so a dropped or swapped field
// cannot hide behind a default of 0.

#include <gtest/gtest.h>

#include <string>
#include <variant>

#include "net/convert.hpp"

namespace {

using rafty::Message;

// Simulates the network: serialize to bytes and parse them back.
std::optional<Message> over_wire(const Message& m) {
  const std::string bytes = net::to_proto(m).SerializeAsString();
  raftpb::RaftMessage parsed;
  EXPECT_TRUE(parsed.ParseFromString(bytes));
  return net::from_proto(parsed);
}

template <class T>
T payload_after_wire(const T& payload) {
  auto back = over_wire(Message{11, 22, payload});
  EXPECT_TRUE(back.has_value());
  EXPECT_EQ(back->from, 11u);
  EXPECT_EQ(back->to, 22u);
  EXPECT_TRUE(std::holds_alternative<T>(back->payload));
  return std::get<T>(back->payload);
}

// ---------------------------------------------------------------------------
// Raft messages
// ---------------------------------------------------------------------------

TEST(Convert, RequestVoteRoundTrips) {
  const rafty::RequestVoteReq in{3, 4, 5, 6, true};
  const auto out = payload_after_wire(in);
  EXPECT_EQ(out.term, 3u);
  EXPECT_EQ(out.candidate_id, 4u);
  EXPECT_EQ(out.last_log_index, 5u);
  EXPECT_EQ(out.last_log_term, 6u);
  EXPECT_TRUE(out.pre_vote);
}

TEST(Convert, RequestVoteRespRoundTrips) {
  const auto out = payload_after_wire(rafty::RequestVoteResp{9, true, true});
  EXPECT_EQ(out.term, 9u);
  EXPECT_TRUE(out.vote_granted);
  EXPECT_TRUE(out.pre_vote);
}

TEST(Convert, AppendEntriesRoundTripsWithBinaryEntries) {
  rafty::AppendEntriesReq in;
  in.term = 7;
  in.leader_id = 2;
  in.prev_log_index = 40;
  in.prev_log_term = 6;
  in.leader_commit = 39;
  in.read_round = 12;
  rafty::LogEntry noop;
  noop.term = 7;
  noop.index = 41;
  noop.type = rafty::EntryType::NoOp;
  rafty::LogEntry cmd;
  cmd.term = 7;
  cmd.index = 42;
  cmd.data = std::string("bin\0\xff\x01", 6);  // zero and high bytes
  in.entries = {noop, cmd};

  const auto out = payload_after_wire(in);
  EXPECT_EQ(out.term, 7u);
  EXPECT_EQ(out.leader_id, 2u);
  EXPECT_EQ(out.prev_log_index, 40u);
  EXPECT_EQ(out.prev_log_term, 6u);
  EXPECT_EQ(out.leader_commit, 39u);
  EXPECT_EQ(out.read_round, 12u);
  EXPECT_EQ(out.entries, in.entries);  // LogEntry has operator==
}

TEST(Convert, AppendEntriesRespRoundTrips) {
  rafty::AppendEntriesResp in;
  in.term = 5;
  in.success = true;
  in.match_index = 17;
  in.rejected_index = 13;
  in.last_log_index = 19;
  in.read_round = 23;
  const auto out = payload_after_wire(in);
  EXPECT_EQ(out.term, 5u);
  EXPECT_TRUE(out.success);
  EXPECT_EQ(out.match_index, 17u);
  EXPECT_EQ(out.rejected_index, 13u);
  EXPECT_EQ(out.last_log_index, 19u);
  EXPECT_EQ(out.read_round, 23u);
}

TEST(Convert, MessageWithoutPayloadIsRejected) {
  raftpb::RaftMessage p;
  p.set_from(1);
  p.set_to(2);
  EXPECT_FALSE(net::from_proto(p).has_value());
}

TEST(Convert, UnknownEntryTypeIsRejected) {
  raftpb::RaftMessage p;
  auto* e = p.mutable_append_entries()->add_entries();
  e->set_type(static_cast<raftpb::EntryType>(7));  // from a future version
  EXPECT_FALSE(net::from_proto(p).has_value());
}

// ---------------------------------------------------------------------------
// KV requests and responses
// ---------------------------------------------------------------------------

TEST(Convert, KvRequestRoundTripsForEveryOp) {
  for (kv::Op op : {kv::Op::Get, kv::Op::Put, kv::Op::Append, kv::Op::Delete}) {
    kv::KvRequest in;
    in.op = op;
    in.client_id = 31;
    in.seq = 32;
    in.key = std::string("k\0y", 3);
    in.value = "value";
    kvpb::KvRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(net::to_proto(in).SerializeAsString()));
    auto out = net::from_proto(parsed);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->op, op);
    EXPECT_EQ(out->client_id, 31u);
    EXPECT_EQ(out->seq, 32u);
    EXPECT_EQ(out->key, in.key);
    EXPECT_EQ(out->value, "value");
  }
}

TEST(Convert, UnknownOpIsRejected) {
  kvpb::KvRequest p;
  p.set_op(static_cast<kvpb::Op>(9));
  EXPECT_FALSE(net::from_proto(p).has_value());
}

TEST(Convert, KvResponseRoundTrips) {
  kv::KvResponse in;
  in.status = kv::Status::NotLeader;
  in.seq = 41;
  in.found = true;
  in.value = "v";
  in.leader_hint = 3;
  kvpb::KvResponse parsed;
  ASSERT_TRUE(parsed.ParseFromString(net::to_proto(in).SerializeAsString()));
  auto out = net::from_proto(parsed);
  ASSERT_TRUE(out.has_value());
  EXPECT_EQ(out->status, kv::Status::NotLeader);
  EXPECT_EQ(out->seq, 41u);
  EXPECT_TRUE(out->found);
  EXPECT_EQ(out->value, "v");
  EXPECT_EQ(out->leader_hint, 3u);
}

}  // namespace
