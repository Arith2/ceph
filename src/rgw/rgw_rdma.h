// SPDX-License-Identifier: LGPL-2.1
// RGW RDMA server: accepts one RC connection from NIXL client and
// exposes rdma_read() to pull data directly from NIXL memory.

#pragma once

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>

// Must match NixlRdmaToken in NIXL rdma_ctx.h (24 bytes packed).
struct __attribute__((packed)) NixlRdmaToken {
    uint64_t addr;    // virtual address of NIXL buffer
    uint32_t rkey;    // remote memory key
    uint32_t length;  // data length in bytes
    uint64_t offset;  // S3 object byte offset
};

// Singleton RDMA CM server.  Starts listening eagerly at module load time.
// PUT requests call rdma_read() to pull data from NIXL.
class RGWRdmaServer {
public:
    static RGWRdmaServer& instance();

    // Start RDMA CM listener on given port (non-blocking).
    // Returns true if listener thread was launched successfully.
    bool start(int port = 7471);
    void stop();
    bool is_ready() const { return ready_.load(); }

    // Perform RDMA_READ: pull tok.length bytes from NIXL memory into local_buf.
    // local_buf must have at least tok.length bytes of writable storage.
    // Returns 0 on success, -errno on failure.
    int rdma_read(const NixlRdmaToken& tok, void* local_buf, size_t len);

    // Perform RDMA_WRITE: push len bytes from src to NIXL memory at
    // tok.addr + remote_offset.  Used by the GET path to deliver object data.
    // Returns 0 on success, -errno on failure.
    int rdma_write(const NixlRdmaToken& tok, const void* src, size_t len,
                   size_t remote_offset = 0);

    // Parse 48-char hex string produced by RdmaContext::buildToken().
    // Returns false on malformed input.
    static bool parse_token(const char* hex, NixlRdmaToken& out);

private:
    RGWRdmaServer() = default;
    ~RGWRdmaServer();
    void listen_loop(int port);

    struct rdma_event_channel* ec_  = nullptr;
    struct rdma_cm_id*         lid_ = nullptr;  // listening CM ID
    struct rdma_cm_id*         cid_ = nullptr;  // connected client CM ID
    struct ibv_pd*             pd_  = nullptr;
    struct ibv_cq*             cq_  = nullptr;

    std::mutex        rdma_mutex_;   // serialise RDMA ops (v1)
    std::thread       listen_thread_;
    std::atomic<bool> ready_{false};
    std::atomic<bool> running_{false};

    // Pre-registered staging buffer: allocated once at connection time so
    // rdma_read() avoids ibv_reg_mr/ibv_dereg_mr on every call.
    static constexpr size_t kPreBufSize = 256UL << 20;  // 256 MB
    void*          pre_buf_ = nullptr;
    struct ibv_mr* pre_mr_  = nullptr;
};
