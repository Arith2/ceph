// SPDX-License-Identifier: LGPL-2.1
// RGW RDMA server: accepts one RC connection from NIXL client and
// exposes rdma_read() to pull data directly from NIXL memory.

#pragma once

#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>
#include <string>
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

    // Parallel batch RDMA_READ: posts all chunk WRs simultaneously into
    // pre_buf_[0..total_len), then polls for all completions.  Returns 0
    // on success; data is accessible via put_buf().
    int rdma_read_batch(const NixlRdmaToken& tok, size_t total_len);

    // Pipeline API: post all RDMA_READ WRs at once (no polling).
    // Returns number of chunks posted, or -errno on failure.
    // Caller must then call rdma_read_poll_one() for each chunk.
    int rdma_read_post_all(const NixlRdmaToken& tok, size_t total_len);

    // Poll for exactly one RDMA_READ completion.  Completions arrive in
    // posting order (IB spec guarantee for same-QP send WRs).
    // Returns 0 on success, -errno on failure.
    int rdma_read_poll_one();

    // Pointer to the PUT staging area within pre_buf_ (valid after rdma_read_batch).
    void* put_buf() const { return pre_buf_; }

    // Perform RDMA_WRITE (blocking): push len bytes from src to NIXL memory at
    // tok.addr + remote_offset.  Used by the GET path to deliver object data.
    // Returns 0 on success, -errno on failure.
    int rdma_write(const NixlRdmaToken& tok, const void* src, size_t len,
                   size_t remote_offset = 0);

    // Non-blocking RDMA_WRITE: post the WR and return immediately.
    // The caller MUST call rdma_write_wait() before posting another write
    // or before the src/staging buffer is reused.
    int rdma_write_post(const NixlRdmaToken& tok, const void* src, size_t len,
                        size_t remote_offset = 0);

    // Wait for the last rdma_write_post() to complete.  No-op if no write pending.
    int rdma_write_wait();

    // Parallel batch RDMA_WRITE: copies src into pre_buf_ in kRdmaChunk-sized
    // pieces, posts all WRs simultaneously, polls all completions.
    // Interleaves memcpy with WR posting so early chunks fly on the wire
    // while later chunks are still being copied.  Returns 0 on success.
    int rdma_write_batch(const NixlRdmaToken& tok, const void* src,
                         size_t total_len);

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
    static constexpr size_t kPreBufSize = (2UL << 30) + (512UL << 20);  // 512 MB
    void*          pre_buf_ = nullptr;
    struct ibv_mr* pre_mr_  = nullptr;

    // PUT batch staging: rdma_read_batch() reads into pre_buf_[0..kPutBufSz).
public:
    static constexpr size_t kPutBufSz  = 2UL << 30; // 256 MiB max object for batch read
    static constexpr size_t kRdmaChunk = 1UL << 20;     // 1 MiB per RDMA WR
    static constexpr size_t kMaxInflight = 16;          // max_qp_rd_atom (HCA limit)

    // GET staging: rdma_write_post() uses pre_buf_[kGetSlot..kGetSlot+kGetSlotSz).
    // Placed after PUT area to avoid aliasing.
    static constexpr size_t kGetSlot   = 2UL << 30; // offset into pre_buf_ for GET staging
    static constexpr size_t kGetSlotSz = 4UL << 20;   // max chunk size for non-blocking write
    bool           get_write_pending_{false};
};

// KV cache streaming: reads multiple chunk objects from DAOS, extracts layers,
// and RDMA-writes each layer to the client.  Implemented in rgw_sal_daos.cc.
// bucket_ptr is a void* to avoid including rgw_sal_daos.h here (cast to DaosBucket* internally).
class DoutPrefixProvider;
namespace rgw { namespace sal {
int kvcache_stream_daos(
    void* bucket_ptr,
    const NixlRdmaToken& rdma_tok,
    const std::vector<std::string>& chunk_keys,
    int num_layers,
    size_t kv_per_token_per_layer,
    size_t tokens_per_chunk,
    int layer_aggregate,
    const DoutPrefixProvider* dpp);
} } // namespace rgw::sal
