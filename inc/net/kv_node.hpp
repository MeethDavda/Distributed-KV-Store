#pragma once

// KvNode: one Raft + KV server process.
//
// Threads:
//   - loop thread (exactly one): owns FileStorage, Raft, and KvServer. It
//     pops events from `events_`, feeds them to Raft/KvServer, and ticks Raft
//     every `tick` interval. This is the same single-threaded code that runs
//     in the simulator; nothing else ever touches it (invariant N1).
//   - gRPC server threads: run the Raft and KV services. They only convert
//     and push events onto `events_`.
//   - one sender thread per peer, inside GrpcTransport.
//
// A client RPC waits on a ReplySlot that the loop thread fills in. If the
// wait times out first (or the queue is full), the client gets NotLeader and
// retries with the same (client_id, seq), so it is deduplicated (N5). A late
// reply into an abandoned slot is harmless: the slot is shared, nobody reads.
//
// (Lives in net/, not kv/, because it links gRPC; the kv library stays
// network-free.)

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <variant>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"
#include "kv/kv_server.hpp"
#include "net/grpc_transport.hpp"
#include "rafty/raft.hpp"
#include "rafty/storage.hpp"
#include "toolings/msg_queue.hpp"

namespace net {

struct NodeConfig {
  rafty::NodeId id = rafty::kNoNode;
  std::map<rafty::NodeId, std::string> addrs;  // every node, including self
  std::filesystem::path data_dir;
  rafty::FsyncMode fsync = rafty::FsyncMode::FullFsync;
  rafty::Millis tick{10};  // how often the loop calls Raft::tick()
};

class KvNode {
 public:
  explicit KvNode(NodeConfig cfg);  // opens storage (runs WAL recovery)
  ~KvNode();                        // stop()

  KvNode(const KvNode&) = delete;
  KvNode& operator=(const KvNode&) = delete;

  void start();  // starts the gRPC server and the loop thread
  void stop();   // clean shutdown (idempotent)
  void wait();   // blocks until the gRPC server shuts down

 private:
  // Filled by the loop thread, waited on by a gRPC handler thread.
  struct ReplySlot {
    std::mutex mu;
    std::condition_variable cv;
    std::optional<kv::KvResponse> resp;
    void set(const kv::KvResponse& r);
  };
  struct ClientCall {
    kv::KvRequest req;
    std::shared_ptr<ReplySlot> slot;
  };
  using Event = std::variant<rafty::Message, ClientCall>;

  class KvService final : public kvpb::KV::Service {
   public:
    explicit KvService(KvNode& node) : node_(node) {}
    grpc::Status Execute(grpc::ServerContext* ctx, const kvpb::KvRequest* req,
                         kvpb::KvResponse* resp) override;

   private:
    KvNode& node_;
  };

  void loop();
  void handle(Event& ev);
  void log_role_changes();

  NodeConfig cfg_;
  toolings::BlockingQueue<Event> events_{4096};  // before transport_: its
                                                 // inbox pushes here
  rafty::FileStorage storage_;
  rafty::SteadyClock clock_;
  GrpcTransport transport_;
  rafty::Raft raft_;
  kv::KvServer kv_server_;
  KvService kv_service_{*this};

  std::unique_ptr<grpc::Server> server_;
  std::thread loop_thread_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stopped_{false};

  rafty::Role last_role_ = rafty::Role::Follower;  // loop thread only
  rafty::Term last_term_ = 0;
};

}  // namespace net
