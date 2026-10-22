#pragma once

// GrpcTransport: the real-network implementation of rafty::Transport.
//
// Outbound (called by the node's loop thread):
//   send() converts the message to proto and pushes it onto that peer's
//   bounded queue, then returns immediately. One sender thread per peer pops
//   and performs the Raft.Send RPC with a short deadline. The loop thread
//   never waits on the network (invariant N2): a slow or dead peer only fills
//   its own queue, and once full, further messages to it are dropped. Raft
//   already tolerates lost messages, and other peers are unaffected (no
//   head-of-line blocking across peers).
//
// Inbound (called on gRPC server threads):
//   service() is the Raft gRPC service. Each received RaftMessage is
//   converted and handed to the `inbox` callback, which (in KvNode) pushes it
//   onto the loop thread's queue. Inbound code never touches Raft directly
//   (invariant N1).

#include <atomic>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "raft.grpc.pb.h"
#include "rafty/interfaces.hpp"
#include "toolings/msg_queue.hpp"

namespace net {

class GrpcTransport final : public rafty::Transport {
 public:
  // Receives every valid inbound message. Runs on gRPC threads.
  using Inbox = std::function<void(rafty::Message)>;

  // `peers` maps every other node's id to its "host:port".
  GrpcTransport(std::map<rafty::NodeId, std::string> peers, Inbox inbox,
                std::size_t queue_capacity = 1024);
  ~GrpcTransport() override;  // stops the sender threads

  GrpcTransport(const GrpcTransport&) = delete;
  GrpcTransport& operator=(const GrpcTransport&) = delete;

  // rafty::Transport. Never blocks; drops if the peer's queue is full.
  void send(const rafty::Message& msg) override;

  // The gRPC service to register on this node's server.
  grpc::Service* service() { return &service_; }

  // Stops sender threads (idempotent). Queued messages may be dropped.
  void stop();

 private:
  // Inbound: Raft.Send handler.
  class Service final : public raftpb::Raft::Service {
   public:
    explicit Service(Inbox inbox) : inbox_(std::move(inbox)) {}
    grpc::Status Send(grpc::ServerContext* ctx, const raftpb::RaftMessage* req,
                      raftpb::Ack* ack) override;

   private:
    Inbox inbox_;
  };

  // Outbound: one per peer.
  struct Peer {
    std::unique_ptr<raftpb::Raft::Stub> stub;
    std::unique_ptr<toolings::BlockingQueue<raftpb::RaftMessage>> queue;
    std::thread sender;
  };
  void sender_loop(Peer& peer);

  Service service_;
  std::map<rafty::NodeId, Peer> peers_;
  std::atomic<bool> stopping_{false};
};

}  // namespace net
