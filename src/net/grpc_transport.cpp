#include "net/grpc_transport.hpp"

#include <chrono>

#include "net/convert.hpp"

namespace net {

namespace {
// How long one Raft.Send may take before we give up on it. Well below the
// election timeout, so a stuck RPC cannot hold back a peer's queue for long.
constexpr auto kRpcDeadline = std::chrono::milliseconds(200);
// How often an idle sender thread wakes to check for shutdown.
constexpr auto kIdlePoll = std::chrono::milliseconds(100);
}  // namespace

GrpcTransport::GrpcTransport(std::map<rafty::NodeId, std::string> peers,
                             Inbox inbox, std::size_t queue_capacity)
    : service_(std::move(inbox)) {
  for (auto& [id, addr] : peers) {
    Peer& p = peers_[id];
    // Channels connect lazily and reconnect on their own after a peer
    // restarts. Insecure: local cluster only (TLS is out of scope).
    p.stub = raftpb::Raft::NewStub(
        grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    p.queue = std::make_unique<toolings::BlockingQueue<raftpb::RaftMessage>>(
        queue_capacity);
  }
  // Start threads only after the map is complete: they hold references into
  // it, and std::map never moves its elements once inserted.
  for (auto& [id, p] : peers_) {
    p.sender = std::thread([this, &p] { sender_loop(p); });
  }
}

GrpcTransport::~GrpcTransport() { stop(); }

void GrpcTransport::stop() {
  if (stopping_.exchange(true)) return;  // already stopped
  for (auto& [id, p] : peers_) p.queue->close();
  for (auto& [id, p] : peers_) {
    if (p.sender.joinable()) p.sender.join();
  }
}

// ===========================================================================
// Outbound
// ===========================================================================

void GrpcTransport::send(const rafty::Message& msg) {
  auto it = peers_.find(msg.to);
  if (it == peers_.end()) return;  // not a peer we know
  // N2: never blocks. A full queue means the peer is slow or down; dropping
  // is safe because Raft retries (heartbeats resend, elections time out).
  it->second.queue->push(to_proto(msg));
}

void GrpcTransport::sender_loop(Peer& peer) {
  for (;;) {
    auto msg = peer.queue->pop_for(kIdlePoll);
    if (!msg) {
      if (stopping_) return;
      continue;
    }
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + kRpcDeadline);
    raftpb::Ack ack;
    // The result is ignored on purpose: Raft treats an unreachable peer the
    // same as a lost message.
    (void)peer.stub->Send(&ctx, *msg, &ack);
  }
}

// ===========================================================================
// Inbound
// ===========================================================================

grpc::Status GrpcTransport::Service::Send(grpc::ServerContext*,
                                          const raftpb::RaftMessage* req,
                                          raftpb::Ack*) {
  // N1: just convert and hand off; the loop thread does the Raft work.
  if (auto m = from_proto(*req)) inbox_(std::move(*m));
  return grpc::Status::OK;
}

}  // namespace net
