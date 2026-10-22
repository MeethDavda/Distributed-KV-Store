#include "net/kv_node.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <stdexcept>

#include "net/convert.hpp"

namespace net {

namespace {

// Longest a client RPC handler waits for the loop thread to answer. Usually
// the client's own (shorter) deadline ends the wait first.
constexpr auto kMaxClientWait = std::chrono::seconds(1);

std::map<rafty::NodeId, std::string> peers_of(const NodeConfig& cfg) {
  auto peers = cfg.addrs;
  peers.erase(cfg.id);
  return peers;
}

rafty::Config raft_config(const NodeConfig& cfg) {
  rafty::Config rc;
  rc.id = cfg.id;
  for (const auto& [id, addr] : cfg.addrs) {
    if (id != cfg.id) rc.peers.push_back(id);
  }
  // Real processes want genuinely different election timeouts; only the
  // simulator needs a fixed seed for replay.
  rc.seed = std::random_device{}();
  return rc;
}

const char* role_name(rafty::Role r) {
  switch (r) {
    case rafty::Role::Follower: return "follower";
    case rafty::Role::PreCandidate: return "pre-candidate";
    case rafty::Role::Candidate: return "candidate";
    case rafty::Role::Leader: return "leader";
  }
  return "?";
}

}  // namespace

// ===========================================================================
// Lifecycle
// ===========================================================================

KvNode::KvNode(NodeConfig cfg)
    : cfg_(std::move(cfg)),
      storage_(cfg_.data_dir, cfg_.fsync),
      transport_(peers_of(cfg_),
                 [this](rafty::Message m) { events_.push(std::move(m)); }),
      raft_(raft_config(cfg_), transport_, storage_, clock_),
      kv_server_(raft_) {
  if (!cfg_.addrs.count(cfg_.id)) {
    throw std::invalid_argument("node id missing from addrs");
  }
}

KvNode::~KvNode() { stop(); }

void KvNode::start() {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(cfg_.addrs.at(cfg_.id),
                           grpc::InsecureServerCredentials());
  builder.RegisterService(transport_.service());  // Raft.Send
  builder.RegisterService(&kv_service_);          // KV.Execute
  server_ = builder.BuildAndStart();
  if (!server_) {
    throw std::runtime_error("cannot listen on " + cfg_.addrs.at(cfg_.id));
  }

  running_ = true;
  loop_thread_ = std::thread([this] { loop(); });
  std::fprintf(stderr, "[node %llu] listening on %s (fsync=%s)\n",
               static_cast<unsigned long long>(cfg_.id),
               cfg_.addrs.at(cfg_.id).c_str(), rafty::to_string(cfg_.fsync));
}

void KvNode::stop() {
  if (stopped_.exchange(true)) return;
  // 1. Stop accepting RPCs (in-flight handlers get cancelled).
  if (server_) server_->Shutdown(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(500));
  // 2. Stop the loop thread.
  running_ = false;
  events_.close();
  if (loop_thread_.joinable()) loop_thread_.join();
  // 3. Stop the per-peer sender threads.
  transport_.stop();
}

void KvNode::wait() {
  if (server_) server_->Wait();
}

// ===========================================================================
// The loop thread: the only thread that touches Raft and KvServer (N1)
// ===========================================================================

void KvNode::loop() {
  auto next_tick = clock_.now() + cfg_.tick;
  while (running_) {
    // Sleep until the next event or the next tick, whichever comes first.
    const auto wait = std::max(next_tick - clock_.now(), rafty::Millis{0});
    if (auto ev = events_.pop_for(wait)) handle(*ev);

    if (clock_.now() >= next_tick) {
      raft_.tick();
      next_tick = clock_.now() + cfg_.tick;
    }
    kv_server_.process();  // apply commits, answer clients
    log_role_changes();
  }
}

void KvNode::handle(Event& ev) {
  if (auto* m = std::get_if<rafty::Message>(&ev)) {
    raft_.step(*m);
    return;
  }
  auto& call = std::get<ClientCall>(ev);
  kv_server_.handle(call.req, [slot = call.slot](const kv::KvResponse& r) {
    slot->set(r);
  });
}

void KvNode::log_role_changes() {
  if (raft_.role() == last_role_ && raft_.term() == last_term_) return;
  last_role_ = raft_.role();
  last_term_ = raft_.term();
  std::fprintf(stderr, "[node %llu] term %llu: %s\n",
               static_cast<unsigned long long>(cfg_.id),
               static_cast<unsigned long long>(last_term_),
               role_name(last_role_));
}

// ===========================================================================
// Client RPCs (gRPC threads)
// ===========================================================================

void KvNode::ReplySlot::set(const kv::KvResponse& r) {
  {
    std::lock_guard<std::mutex> lock(mu);
    if (resp) return;  // N5: answered at most once
    resp = r;
  }
  cv.notify_one();
}

grpc::Status KvNode::KvService::Execute(grpc::ServerContext* ctx,
                                        const kvpb::KvRequest* req,
                                        kvpb::KvResponse* out) {
  auto r = from_proto(*req);
  if (!r) return {grpc::StatusCode::INVALID_ARGUMENT, "bad request"};

  kv::KvResponse retry;  // what the client gets if we cannot answer in time
  retry.status = kv::Status::NotLeader;
  retry.seq = r->seq;

  auto slot = std::make_shared<ReplySlot>();
  if (!node_.events_.push(ClientCall{*r, slot})) {  // overloaded or stopping
    *out = to_proto(retry);
    return grpc::Status::OK;
  }

  // Wait for the loop thread, but never past the client's own deadline.
  const auto limit =
      std::min(ctx->deadline(), std::chrono::system_clock::now() + kMaxClientWait);
  std::unique_lock<std::mutex> lock(slot->mu);
  slot->cv.wait_until(lock, limit, [&] { return slot->resp.has_value(); });
  *out = to_proto(slot->resp ? *slot->resp : retry);
  return grpc::Status::OK;
}

}  // namespace net
