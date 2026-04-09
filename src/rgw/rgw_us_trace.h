// BENCH-DIAGNOSTIC: microsecond-resolution wallclock event log.
// Writes one line per probe to /tmp/rgw_trace_us.log so events can be
// correlated against client-side NIXL_TRACE timestamps.
#ifndef RGW_US_TRACE_H
#define RGW_US_TRACE_H

#include <atomic>
#include <chrono>
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

inline void emit(const char* event) {
    if (!enabled()) return;
    FILE* f = fp();
    if (!f) return;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    double ts = std::chrono::duration_cast<std::chrono::microseconds>(now).count() / 1e6;
    long tid = (long)syscall(SYS_gettid);
    std::fprintf(f, "%.6f %ld %s\n", ts, tid, event);
}

}  // namespace rgw_us_trace_ns

#define RGW_US(name) ::rgw_us_trace_ns::emit(name)

#endif
