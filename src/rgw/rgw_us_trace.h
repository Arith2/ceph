// BENCH-DIAGNOSTIC: microsecond-resolution wallclock event log.
// Writes one line per probe to /tmp/rgw_trace_us.log so events can be
// correlated against client-side NIXL_TRACE timestamps.
#ifndef RGW_US_TRACE_H
#define RGW_US_TRACE_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <pthread.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace rgw_us_trace_ns {

inline FILE* fp() {
    static FILE* f = nullptr;
    static std::once_flag once;
    std::call_once(once, []{
        const char* path = std::getenv("RGW_US_TRACE_FILE");
        if (!path) path = "/tmp/rgw_trace_us.log";
        f = std::fopen(path, "a");
        if (f) std::setvbuf(f, nullptr, _IOLBF, 0);
    });
    return f;
}

inline bool enabled() {
    static const bool en = (std::getenv("RGW_US_TRACE") != nullptr);
    return en;
}

// Thread-local current request id.  Set by rgw_rest_s3.cc after parsing
// the x-nixl-req-id header; read by every emit() on the same thread.
// Probes that fire on other threads (e.g. daos_eq progress thread) will
// see req_id=0 and must be matched by thread+time instead.
inline uint64_t& tls_req_id() {
    static thread_local uint64_t rid = 0;
    return rid;
}

inline void set_req_id(uint64_t rid) { tls_req_id() = rid; }
inline void clear_req_id()           { tls_req_id() = 0; }

inline void emit(const char* event) {
    if (!enabled()) return;
    FILE* f = fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld %lu %s\n", ts, tid,
                 (unsigned long)tls_req_id(), event);
}

inline void emit_r(const char* event, uint64_t rid) {
    if (!enabled()) return;
    FILE* f = fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld %lu %s\n", ts, tid, (unsigned long)rid, event);
}

}  // namespace rgw_us_trace_ns

#define RGW_US(name)         ::rgw_us_trace_ns::emit(name)
#define RGW_US_R(name, rid)  ::rgw_us_trace_ns::emit_r(name, rid)
#define RGW_US_SET(rid)      ::rgw_us_trace_ns::set_req_id(rid)
#define RGW_US_CLR()         ::rgw_us_trace_ns::clear_req_id()

#endif
