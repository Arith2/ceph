// SPDX-License-Identifier: LGPL-2.1
#include "rgw_rdma.h"
#include "rgw_us_trace.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

RGWRdmaServer&
RGWRdmaServer::instance() {
    static RGWRdmaServer inst;
    return inst;
}

RGWRdmaServer::~RGWRdmaServer() { stop(); }

bool
RGWRdmaServer::start(int port) {
    bool was_running = running_.exchange(true);
    FILE* sf = fopen("/tmp/rgw_rdma_start.log", "w");
    if (sf) {
        fprintf(sf, "start() called, was_running=%d, pid=%d\n", (int)was_running, (int)getpid());
        fclose(sf);
    }
    if (was_running) return true;
    listen_thread_ = std::thread(&RGWRdmaServer::listen_loop, this, port);
    return true;
}

void
RGWRdmaServer::stop() {
    running_ = false;
    if (pre_mr_)  { ibv_dereg_mr(pre_mr_);           pre_mr_  = nullptr; }
    if (pre_buf_) { munmap(pre_buf_, kPreBufSize);    pre_buf_ = nullptr; }
    if (cq_)  { ibv_destroy_cq(cq_);               cq_  = nullptr; }
    if (cid_) { rdma_destroy_id(cid_);              cid_ = nullptr; }
    if (lid_) { rdma_destroy_id(lid_);              lid_ = nullptr; }
    if (ec_)  { rdma_destroy_event_channel(ec_);    ec_  = nullptr; }
    if (listen_thread_.joinable()) listen_thread_.join();
}

void
RGWRdmaServer::listen_loop(int port) {
    FILE* dbg = fopen("/tmp/rgw_rdma_debug.log", "w");
    if (dbg) { fprintf(dbg, "listen_loop started port=%d\n", port); fflush(dbg); }

    ec_ = rdma_create_event_channel();
    if (!ec_) {
        if (dbg) { fprintf(dbg, "rdma_create_event_channel failed: %s\n", strerror(errno)); fclose(dbg); }
        return;
    }

    if (rdma_create_id(ec_, &lid_, nullptr, RDMA_PS_TCP) != 0) {
        if (dbg) { fprintf(dbg, "rdma_create_id failed: %s\n", strerror(errno)); fclose(dbg); }
        return;
    }

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    if (rdma_bind_addr(lid_, reinterpret_cast<struct sockaddr*>(&addr)) != 0) {
        if (dbg) { fprintf(dbg, "rdma_bind_addr failed: %s\n", strerror(errno)); fclose(dbg); }
        return;
    }

    if (rdma_listen(lid_, 8) != 0) {
        if (dbg) { fprintf(dbg, "rdma_listen failed: %s\n", strerror(errno)); fclose(dbg); }
        return;
    }
    if (dbg) { fprintf(dbg, "listening OK on port %d\n", port); fflush(dbg); }

    // Accept loop: handle reconnections from successive nixlbench runs.
    while (running_) {
        struct rdma_cm_event* ev = nullptr;
        if (rdma_get_cm_event(ec_, &ev) != 0) {
            if (dbg) fprintf(dbg, "rdma_get_cm_event error: %s\n", strerror(errno));
            break;
        }

        if (ev->event == RDMA_CM_EVENT_DISCONNECTED) {
            if (dbg) { fprintf(dbg, "client disconnected\n"); fflush(dbg); }
            rdma_ack_cm_event(ev);
            ready_ = false;
            continue;
        }

        if (ev->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
            if (dbg) fprintf(dbg, "unexpected event %d — ignoring\n", (int)ev->event);
            rdma_ack_cm_event(ev);
            continue;
        }

        if (dbg) { fprintf(dbg, "got CONNECT_REQUEST\n"); fflush(dbg); }

        // New client connecting — set up fresh connection resources.
        struct rdma_cm_id* new_cid = ev->id;
        rdma_ack_cm_event(ev);

        struct ibv_pd*  new_pd = ibv_alloc_pd(new_cid->verbs);
        if (!new_pd) {
            if (dbg) fprintf(dbg, "ibv_alloc_pd failed: %s\n", strerror(errno));
            rdma_destroy_id(new_cid);
            continue;
        }

        struct ibv_cq* new_cq = ibv_create_cq(new_cid->verbs, 128, nullptr, nullptr, 0);
        if (!new_cq) {
            if (dbg) fprintf(dbg, "ibv_create_cq failed\n");
            ibv_dealloc_pd(new_pd);
            rdma_destroy_id(new_cid);
            continue;
        }

        struct ibv_qp_init_attr qp_attr{};
        qp_attr.send_cq          = new_cq;
        qp_attr.recv_cq          = new_cq;
        qp_attr.qp_type          = IBV_QPT_RC;
        qp_attr.cap.max_send_wr  = 128;
        qp_attr.cap.max_recv_wr  = 1;
        qp_attr.cap.max_send_sge = 1;
        qp_attr.cap.max_recv_sge = 1;
        if (rdma_create_qp(new_cid, new_pd, &qp_attr) != 0) {
            if (dbg) fprintf(dbg, "rdma_create_qp failed: %s\n", strerror(errno));
            ibv_destroy_cq(new_cq);
            ibv_dealloc_pd(new_pd);
            rdma_destroy_id(new_cid);
            continue;
        }

        struct rdma_conn_param conn_param{};
        conn_param.responder_resources = 16;
        conn_param.initiator_depth     = 16;
        if (rdma_accept(new_cid, &conn_param) != 0) {
            if (dbg) fprintf(dbg, "rdma_accept failed: %s\n", strerror(errno));
            rdma_destroy_qp(new_cid);
            ibv_destroy_cq(new_cq);
            ibv_dealloc_pd(new_pd);
            rdma_destroy_id(new_cid);
            continue;
        }

        // Wait for ESTABLISHED on the new connection.
        if (rdma_get_cm_event(ec_, &ev) != 0 ||
            ev->event != RDMA_CM_EVENT_ESTABLISHED) {
            if (dbg) fprintf(dbg, "expected ESTABLISHED, got %d\n",
                             ev ? (int)ev->event : -1);
            if (ev) rdma_ack_cm_event(ev);
            rdma_destroy_qp(new_cid);
            ibv_destroy_cq(new_cq);
            ibv_dealloc_pd(new_pd);
            rdma_destroy_id(new_cid);
            continue;
        }
        rdma_ack_cm_event(ev);
        if (dbg) { fprintf(dbg, "NIXL client connected!\n"); fflush(dbg); }

        // Allocate and pre-register staging buffer for this connection.
        // MAP_POPULATE faults in all pages immediately so ibv_reg_mr is fast.
        void* new_buf = mmap(nullptr, kPreBufSize, PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
        struct ibv_mr* new_mr = nullptr;
        if (new_buf == MAP_FAILED) {
            new_buf = nullptr;
            if (dbg) fprintf(dbg, "mmap pre_buf failed: %s\n", strerror(errno));
        } else {
            new_mr = ibv_reg_mr(new_pd, new_buf, kPreBufSize, IBV_ACCESS_LOCAL_WRITE);
            if (!new_mr) {
                if (dbg) fprintf(dbg, "ibv_reg_mr pre_buf failed: %s\n", strerror(errno));
                munmap(new_buf, kPreBufSize);
                new_buf = nullptr;
            } else {
                if (dbg) fprintf(dbg, "pre_buf registered: size=%zu\n", kPreBufSize);
            }
        }

        // Atomically swap out old connection, install new one.
        {
            std::lock_guard<std::mutex> lock(rdma_mutex_);
            ready_ = false;

            // Tear down previous connection resources (if any).
            if (pre_mr_)  { ibv_dereg_mr(pre_mr_);           pre_mr_  = nullptr; }
            if (pre_buf_) { munmap(pre_buf_, kPreBufSize);    pre_buf_ = nullptr; }
            if (cid_)     { rdma_destroy_id(cid_);            cid_     = nullptr; }
            if (cq_)      { ibv_destroy_cq(cq_);              cq_      = nullptr; }
            if (pd_)      { ibv_dealloc_pd(pd_);              pd_      = nullptr; }

            cid_     = new_cid;
            pd_      = new_pd;
            cq_      = new_cq;
            pre_buf_ = new_buf;
            pre_mr_  = new_mr;
            ready_   = true;
        }
        if (dbg) { fprintf(dbg, "connection ready (reconnect loop)\n"); fflush(dbg); }
    }

    if (dbg) fclose(dbg);
}

int
RGWRdmaServer::rdma_read(const NixlRdmaToken& tok, void* local_buf, size_t len) {
    if (!ready_) return -ENOTCONN;

    // Use the pre-registered staging buffer if the request fits, avoiding
    // ibv_reg_mr/ibv_dereg_mr on the critical path. Fall back to per-call
    // registration for oversized requests.
    bool use_pre = (pre_buf_ && pre_mr_ && len <= kPreBufSize);

    struct ibv_mr* tmp_mr = nullptr;
    if (!use_pre) {
        tmp_mr = ibv_reg_mr(pd_, local_buf, len, IBV_ACCESS_LOCAL_WRITE);
        if (!tmp_mr) return -errno;
    }

    void*    dst  = use_pre ? pre_buf_ : local_buf;
    uint32_t lkey = use_pre ? pre_mr_->lkey : tmp_mr->lkey;

    std::lock_guard<std::mutex> lock(rdma_mutex_);

    struct ibv_sge sge{};
    sge.addr   = reinterpret_cast<uint64_t>(dst);
    sge.length = static_cast<uint32_t>(len);
    sge.lkey   = lkey;

    struct ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id               = reinterpret_cast<uint64_t>(dst);
    wr.opcode              = IBV_WR_RDMA_READ;
    wr.send_flags          = IBV_SEND_SIGNALED;
    wr.sg_list             = &sge;
    wr.num_sge             = 1;
    wr.wr.rdma.remote_addr = tok.addr;
    wr.wr.rdma.rkey        = tok.rkey;

    int ret = ibv_post_send(cid_->qp, &wr, &bad_wr);
    if (ret != 0) {
        if (tmp_mr) ibv_dereg_mr(tmp_mr);
        return -ret;
    }

    struct ibv_wc wc{};
    int nc;
    do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);

    if (tmp_mr) ibv_dereg_mr(tmp_mr);

    if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "RGWRdmaServer: RDMA_READ failed, wc.status=%s\n",
                ibv_wc_status_str(wc.status));
        return -EIO;
    }

    if (use_pre)
        memcpy(local_buf, pre_buf_, len);

    return 0;
}

int
RGWRdmaServer::rdma_read_batch(const NixlRdmaToken& tok, size_t total_len) {
    if (!ready_) return -ENOTCONN;
    if (!pre_buf_ || !pre_mr_) return -ENOMEM;
    if (total_len > kPutBufSz) return -EINVAL;

    const size_t n = (total_len + kRdmaChunk - 1) / kRdmaChunk;

    RGW_US("rdma_read_batch_before_lock");
    std::lock_guard<std::mutex> lock(rdma_mutex_);
    RGW_US("rdma_read_batch_after_lock");

    // Sliding window: keep up to kMaxInflight (= max_qp_rd_atom) WRs in
    // flight at all times.  As each completion arrives, post the next WR
    // immediately — no idle gaps between waves.
    size_t posted = 0, completed = 0;

    // Fill the initial window.
    while (posted < n && posted - completed < kMaxInflight) {
        size_t off   = posted * kRdmaChunk;
        size_t chunk = std::min(kRdmaChunk, total_len - off);

        struct ibv_sge sge{};
        sge.addr   = reinterpret_cast<uint64_t>(
                         static_cast<uint8_t*>(pre_buf_) + off);
        sge.length = static_cast<uint32_t>(chunk);
        sge.lkey   = pre_mr_->lkey;

        struct ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id               = posted;
        wr.opcode              = IBV_WR_RDMA_READ;
        wr.send_flags          = IBV_SEND_SIGNALED;
        wr.sg_list             = &sge;
        wr.num_sge             = 1;
        wr.wr.rdma.remote_addr = tok.addr + off;
        wr.wr.rdma.rkey        = tok.rkey;

        int ret = ibv_post_send(cid_->qp, &wr, &bad_wr);
        if (ret != 0) return -ret;
        ++posted;
    }

    // Slide: poll one completion, post one new WR, repeat.
    while (completed < n) {
        struct ibv_wc wc{};
        int nc;
        do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);
        if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "RGWRdmaServer: rdma_read_batch WR %lu failed, wc.status=%s\n",
                    (unsigned long)wc.wr_id, ibv_wc_status_str(wc.status));
            return -EIO;
        }
        ++completed;

        // Post next WR if any remain.
        if (posted < n) {
            size_t off   = posted * kRdmaChunk;
            size_t chunk = std::min(kRdmaChunk, total_len - off);

            struct ibv_sge sge{};
            sge.addr   = reinterpret_cast<uint64_t>(
                             static_cast<uint8_t*>(pre_buf_) + off);
            sge.length = static_cast<uint32_t>(chunk);
            sge.lkey   = pre_mr_->lkey;

            struct ibv_send_wr wr{}, *bad_wr = nullptr;
            wr.wr_id               = posted;
            wr.opcode              = IBV_WR_RDMA_READ;
            wr.send_flags          = IBV_SEND_SIGNALED;
            wr.sg_list             = &sge;
            wr.num_sge             = 1;
            wr.wr.rdma.remote_addr = tok.addr + off;
            wr.wr.rdma.rkey        = tok.rkey;

            int ret = ibv_post_send(cid_->qp, &wr, &bad_wr);
            if (ret != 0) return -ret;
            ++posted;
        }
    }

    return 0;
}

int
RGWRdmaServer::rdma_read_post_all(const NixlRdmaToken& tok, size_t total_len) {
    if (!ready_) return -ENOTCONN;
    if (!pre_buf_ || !pre_mr_) return -ENOMEM;
    if (total_len > kPutBufSz) return -EINVAL;

    const int n = static_cast<int>((total_len + kRdmaChunk - 1) / kRdmaChunk);

    std::lock_guard<std::mutex> lock(rdma_mutex_);

    for (int i = 0; i < n; ++i) {
        size_t off   = static_cast<size_t>(i) * kRdmaChunk;
        size_t chunk = std::min(kRdmaChunk, total_len - off);

        struct ibv_sge sge{};
        sge.addr   = reinterpret_cast<uint64_t>(
                         static_cast<uint8_t*>(pre_buf_) + off);
        sge.length = static_cast<uint32_t>(chunk);
        sge.lkey   = pre_mr_->lkey;

        struct ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id               = static_cast<uint64_t>(i);
        wr.opcode              = IBV_WR_RDMA_READ;
        wr.send_flags          = IBV_SEND_SIGNALED;
        wr.sg_list             = &sge;
        wr.num_sge             = 1;
        wr.wr.rdma.remote_addr = tok.addr + off;
        wr.wr.rdma.rkey        = tok.rkey;

        int ret = ibv_post_send(cid_->qp, &wr, &bad_wr);
        if (ret != 0) return -ret;
    }
    return n;
}

int
RGWRdmaServer::rdma_read_poll_one() {
    // No mutex needed: single-threaded PUT path, and the WRs were already
    // posted under the lock in rdma_read_post_all().  CQ polling is safe
    // because only one PUT is in flight at a time.
    struct ibv_wc wc{};
    int nc;
    do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);
    if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "RGWRdmaServer: rdma_read_poll_one failed, wc.status=%s\n",
                ibv_wc_status_str(wc.status));
        return -EIO;
    }
    return 0;
}

int
RGWRdmaServer::rdma_write(const NixlRdmaToken& tok, const void* src, size_t len,
                          size_t remote_offset) {
    if (!ready_) return -ENOTCONN;
    if (len == 0) return 0;

    bool use_pre = (pre_buf_ && pre_mr_ && len <= kPreBufSize);

    struct ibv_mr* tmp_mr = nullptr;

    std::lock_guard<std::mutex> lock(rdma_mutex_);

    void*    src_buf;
    uint32_t lkey;

    if (use_pre) {
        memcpy(pre_buf_, src, len);
        src_buf = pre_buf_;
        lkey    = pre_mr_->lkey;
    } else {
        // Oversized: register caller's buffer for local read access
        tmp_mr = ibv_reg_mr(pd_, const_cast<void*>(src), len, 0);
        if (!tmp_mr) return -errno;
        src_buf = const_cast<void*>(src);
        lkey    = tmp_mr->lkey;
    }

    struct ibv_sge sge{};
    sge.addr   = reinterpret_cast<uint64_t>(src_buf);
    sge.length = static_cast<uint32_t>(len);
    sge.lkey   = lkey;

    struct ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id               = reinterpret_cast<uint64_t>(src_buf);
    wr.opcode              = IBV_WR_RDMA_WRITE;
    wr.send_flags          = IBV_SEND_SIGNALED;
    wr.sg_list             = &sge;
    wr.num_sge             = 1;
    wr.wr.rdma.remote_addr = tok.addr + remote_offset;
    wr.wr.rdma.rkey        = tok.rkey;

    int ret = ibv_post_send(cid_->qp, &wr, &bad_wr);
    if (ret != 0) {
        if (tmp_mr) ibv_dereg_mr(tmp_mr);
        return -ret;
    }

    struct ibv_wc wc{};
    int nc;
    do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);

    if (tmp_mr) ibv_dereg_mr(tmp_mr);

    if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "RGWRdmaServer: RDMA_WRITE failed, wc.status=%s\n",
                ibv_wc_status_str(wc.status));
        return -EIO;
    }

    RGW_US("rdma_read_batch_done");
    return 0;
}

int
RGWRdmaServer::rdma_write_post(const NixlRdmaToken& tok, const void* src, size_t len,
                                size_t remote_offset) {
    if (!ready_) return -ENOTCONN;
    if (len == 0)  return 0;
    if (len > kGetSlotSz) return -EINVAL;  // caller must use <=4 MiB chunks

    std::lock_guard<std::mutex> lock(rdma_mutex_);

    // Wait for any previous non-blocking write before reusing the slot buffer.
    if (get_write_pending_) {
        struct ibv_wc wc{};
        int nc;
        do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);
        get_write_pending_ = false;
        if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "RGWRdmaServer: rdma_write_post drain failed wc.status=%s\n",
                    ibv_wc_status_str(wc.status));
            return -EIO;
        }
    }

    // Copy src into the dedicated GET staging slot (avoids ibv_reg_mr per call).
    void* slot = static_cast<uint8_t*>(pre_buf_) + kGetSlot;
    memcpy(slot, src, len);

    struct ibv_sge sge{};
    sge.addr   = reinterpret_cast<uint64_t>(slot);
    sge.length = static_cast<uint32_t>(len);
    sge.lkey   = pre_mr_->lkey;

    struct ibv_send_wr wr{}, *bad_wr = nullptr;
    wr.wr_id               = 0xbeef;
    wr.opcode              = IBV_WR_RDMA_WRITE;
    wr.send_flags          = IBV_SEND_SIGNALED;
    wr.sg_list             = &sge;
    wr.num_sge             = 1;
    wr.wr.rdma.remote_addr = tok.addr + remote_offset;
    wr.wr.rdma.rkey        = tok.rkey;

    int ret = ibv_post_send(cid_->qp, &wr, &bad_wr);
    if (ret != 0) return -ret;

    get_write_pending_ = true;
    return 0;
}

int
RGWRdmaServer::rdma_write_wait() {
    if (!get_write_pending_) return 0;

    std::lock_guard<std::mutex> lock(rdma_mutex_);
    struct ibv_wc wc{};
    int nc;
    do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);
    get_write_pending_ = false;

    if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "RGWRdmaServer: rdma_write_wait failed wc.status=%s\n",
                ibv_wc_status_str(wc.status));
        return -EIO;
    }
    return 0;
}

int
RGWRdmaServer::rdma_write_batch(const NixlRdmaToken& tok, const void* src,
                                size_t total_len) {
    if (!ready_) return -ENOTCONN;
    if (!pre_buf_ || !pre_mr_) return -ENOMEM;
    if (total_len > kPutBufSz) return -EINVAL;
    if (total_len == 0) return 0;

    const size_t n = (total_len + kRdmaChunk - 1) / kRdmaChunk;

    std::lock_guard<std::mutex> lock(rdma_mutex_);

    // Sliding window: keep up to kMaxInflight WRs in flight.
    size_t posted = 0, completed = 0;

    auto post_one = [&](size_t i) -> int {
        size_t off   = i * kRdmaChunk;
        size_t chunk = std::min(kRdmaChunk, total_len - off);

        void* dst = static_cast<uint8_t*>(pre_buf_) + off;
        const void* chunk_src = static_cast<const uint8_t*>(src) + off;
        if (chunk_src != dst)
            memcpy(dst, chunk_src, chunk);

        struct ibv_sge sge{};
        sge.addr   = reinterpret_cast<uint64_t>(dst);
        sge.length = static_cast<uint32_t>(chunk);
        sge.lkey   = pre_mr_->lkey;

        struct ibv_send_wr wr{}, *bad_wr = nullptr;
        wr.wr_id               = i;
        wr.opcode              = IBV_WR_RDMA_WRITE;
        wr.send_flags          = IBV_SEND_SIGNALED;
        wr.sg_list             = &sge;
        wr.num_sge             = 1;
        wr.wr.rdma.remote_addr = tok.addr + off;
        wr.wr.rdma.rkey        = tok.rkey;

        return ibv_post_send(cid_->qp, &wr, &bad_wr);
    };

    // Fill the initial window.
    while (posted < n && posted - completed < kMaxInflight) {
        int ret = post_one(posted);
        if (ret != 0) return -ret;
        ++posted;
    }

    // Slide: poll one completion, post one new WR, repeat.
    while (completed < n) {
        struct ibv_wc wc{};
        int nc;
        do { nc = ibv_poll_cq(cq_, 1, &wc); } while (nc == 0);
        if (nc < 0 || wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "RGWRdmaServer: rdma_write_batch WR %lu failed, wc.status=%s\n",
                    (unsigned long)wc.wr_id, ibv_wc_status_str(wc.status));
            return -EIO;
        }
        ++completed;

        if (posted < n) {
            int ret = post_one(posted);
            if (ret != 0) return -ret;
            ++posted;
        }
    }
    return 0;
}

bool
RGWRdmaServer::parse_token(const char* hex, NixlRdmaToken& out) {
    constexpr size_t expected = sizeof(NixlRdmaToken) * 2;
    if (!hex || strlen(hex) != expected) return false;
    auto* dst = reinterpret_cast<uint8_t*>(&out);
    for (size_t i = 0; i < sizeof(NixlRdmaToken); ++i) {
        char byte_str[3] = {hex[i * 2], hex[i * 2 + 1], '\0'};
        char* end        = nullptr;
        dst[i] = static_cast<uint8_t>(strtoul(byte_str, &end, 16));
        if (end == nullptr || *end != '\0') return false;
    }
    return true;
}
