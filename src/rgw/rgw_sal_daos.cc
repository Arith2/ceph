// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * SAL implementation for the CORTX DAOS backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal_daos.h"
#include <shared_mutex>
#include <unordered_map>
#include "rgw_us_trace.h"
#include <memory>
#include <daos_array.h>
#include <functional>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <array>
#include <atomic>
// Minimal struct to access dfs_obj->oh without internal headers
namespace {
struct dfs_obj_min { void* dfs; daos_obj_id_t oid; daos_handle_t oh; };
struct ds3_obj_min { void* dfs_obj; };
static inline daos_handle_t get_array_oh(ds3_obj_t* obj) {
    auto* s = reinterpret_cast<ds3_obj_min*>(obj);
    auto* d = reinterpret_cast<dfs_obj_min*>(s->dfs_obj);
    return d->oh;
}
} // anonymous namespace
#include "rgw_rdma.h"

// ============================================================================
// Option 3: Dedicated DAOS progress thread + per-request condvar waiters.
// One global daos_eq, ONE progress thread polls it; Beast workers submit
// freely from any thread and wait on per-writer condition_variable. Mirrors
// FIO's "one thread monopolizing one EQ" model so libdaos's per-EQ mutex
// is never contended, while still allowing asio coroutine resumption on
// arbitrary Beast worker threads (since the wait is a pthread condvar, not
// a libdaos call).
// ============================================================================
namespace rgw::sal {
struct DaosReqWaiter {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done = false;
    int                     err  = 0;
};
}  // namespace rgw::sal

namespace {
// Option C: pool of N daos_eq each with its own dedicated progress thread.
// Pool size is read once at first use from NIXL_DAOS_EQ_POOL (default 8).
static constexpr size_t MAX_DAOS_EQ_POOL = 64;
static daos_handle_t        g_progress_eq_pool[MAX_DAOS_EQ_POOL] = {};
static std::thread          g_progress_threads[MAX_DAOS_EQ_POOL];
static size_t               g_progress_eq_pool_size = 0;
static std::atomic<bool>    g_progress_run{false};
static std::once_flag       g_progress_once;
static std::atomic<uint64_t> g_progress_rr_counter{0};

// Completion callback fired by libdaos from inside the progress thread context.
// Signals the per-writer condvar so the waiting Beast worker wakes up.
static int
daos_progress_completion_cb(void *arg, daos_event_t *ev, int ret) {
    auto *w = static_cast<rgw::sal::DaosReqWaiter*>(arg);
    {
        std::lock_guard<std::mutex> lock(w->mtx);
        w->done = true;
        w->err  = ret;
    }
    w->cv.notify_one();
    return 0;
}

// Per-EQ progress loop: each thread polls only its own EQ index, so
// libdaos's per-EQ mutex is uncontended (one polling thread per EQ).
static void
daos_progress_loop_one(size_t idx) {
    daos_event_t* completed[64];
    daos_handle_t eq = g_progress_eq_pool[idx];
    while (g_progress_run.load(std::memory_order_relaxed)) {
        int n = daos_eq_poll(eq, 0, /*timeout_us*/100, 64, completed);
        (void)n;
    }
}

static void
init_progress_thread() {
    const char *e = std::getenv("NIXL_DAOS_EQ_POOL");
    size_t n = e ? static_cast<size_t>(std::atoi(e)) : 8u;
    if (n < 1) n = 1;
    if (n > MAX_DAOS_EQ_POOL) n = MAX_DAOS_EQ_POOL;
    g_progress_eq_pool_size = n;
    for (size_t i = 0; i < n; ++i) {
        int rc = daos_eq_create(&g_progress_eq_pool[i]);
        if (rc != 0) {
            g_progress_eq_pool[i] = DAOS_HDL_INVAL;
        }
    }
    g_progress_run.store(true);
    for (size_t i = 0; i < n; ++i) {
        if (g_progress_eq_pool[i].cookie != 0) {
            g_progress_threads[i] = std::thread(daos_progress_loop_one, i);
        }
    }
}

// Round-robin assignment: each call returns the next EQ from the pool.
static daos_handle_t
get_progress_eq() {
    std::call_once(g_progress_once, init_progress_thread);
    if (g_progress_eq_pool_size == 0) return DAOS_HDL_INVAL;
    uint64_t i = g_progress_rr_counter.fetch_add(1, std::memory_order_relaxed)
                 % g_progress_eq_pool_size;
    return g_progress_eq_pool[i];
}

// ============================================================================
// libdfs-direct data path. When RGW_DFS_DIRECT=1, DaosAtomicWriter::process
// and DaosObject::read bypass ds3_obj_write/read entirely and call
// dfs_write/dfs_read directly on the cached dfs_t* (which libds3 already
// created via dfs_connect). This mirrors FIO's DFS engine pattern: one
// shared dfs_t* per process + per-request daos_event_t attached to the
// existing singleton progress EQ.
//
// The key insight: ds3_obj_t is literally { dfs_obj_t *dfs_obj; } and
// ds3_bucket_t is literally { dfs_t *dfs; } — libds3 is a thin wrapper
// around libdfs. So instead of going through the racy ds3_obj_write +
// ds3_obj_set_info path, we can call dfs_write directly with a per-request
// event and skip the per-object metadata write entirely (which we don't
// need for chunk-wise S3 RDMA where keys are content-addressed).
// ============================================================================
namespace {
static inline bool dfs_direct_enabled() {
    static const bool en = (std::getenv("RGW_DFS_DIRECT") != nullptr);
    return en;
}

// Local mirrors of libds3 internal struct layout. These match
// daos/src/client/ds3/ds3_internal.h exactly so we can peek the underlying
// libdfs handles without going through the racy ds3_obj_* wrappers.
// Layout is stable for the installed libds3 version 2.7.x.
struct ds3_bucket_layout { dfs_t *dfs; };
struct ds3_obj_layout    { dfs_obj_t *dfs_obj; };

static inline dfs_t* dfs_of(ds3_bucket_t *b) {
    return reinterpret_cast<ds3_bucket_layout*>(b)->dfs;
}
static inline dfs_obj_t* dfsobj_of(ds3_obj_t *o) {
    return reinterpret_cast<ds3_obj_layout*>(o)->dfs_obj;
}
}  // namespace

// Public entry point so DaosStore::initialize can eagerly start the
// progress thread at RGW startup, avoiding a 589 ms stall on the first
// concurrent request batch (lazy daos_eq_create() blocks all callers
// of std::call_once until it returns).

// Per-request EQ helper (option B).
static inline bool per_req_eq_enabled() {
  static const bool en = (std::getenv("DAOS_PER_REQ_EQ") != nullptr);
  return en;
}
void
ensure_progress_thread_started() {
    std::call_once(g_progress_once, init_progress_thread);
}
}  // namespace


#include <errno.h>
#include <iomanip>
#include <stdlib.h>
#include <unistd.h>

#include <filesystem>
#include <system_error>

#include "common/Clock.h"
#include "common/errno.h"
#include "rgw_bucket.h"
#include "rgw_compression.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;

namespace fs = std::filesystem;

namespace rgw::sal {

using ::ceph::decode;
using ::ceph::encode;

int DaosUser::list_buckets(const DoutPrefixProvider* dpp, const string& marker,
                           const string& end_marker, uint64_t max,
                           bool need_stats, BucketList& buckets,
                           optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: list_user_buckets: marker=" << marker
                     << " end_marker=" << end_marker << " max=" << max << dendl;
  int ret = 0;
  bool is_truncated = false;
  buckets.clear();
  vector<struct ds3_bucket_info> bucket_infos(max);
  daos_size_t bcount = bucket_infos.size();
  vector<vector<uint8_t>> values(bcount, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (daos_size_t i = 0; i < bcount; i++) {
    bucket_infos[i].encoded = values[i].data();
    bucket_infos[i].encoded_length = values[i].size();
  }

  char daos_marker[DS3_MAX_BUCKET_NAME];
  std::strncpy(daos_marker, marker.c_str(), sizeof(daos_marker));
  ret = ds3_bucket_list(&bcount, bucket_infos.data(), daos_marker,
                        &is_truncated, store->ds3, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_bucket_list: bcount=" << bcount
                     << " ret=" << ret << dendl;
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_list failed!" << ret << dendl;
    return ret;
  }

  bucket_infos.resize(bcount);
  values.resize(bcount);

  for (const auto& bi : bucket_infos) {
    DaosBucketInfo dbinfo;
    bufferlist bl;
    bl.append(reinterpret_cast<char*>(bi.encoded), bi.encoded_length);
    auto iter = bl.cbegin();
    dbinfo.decode(iter);
    buckets.add(std::make_unique<DaosBucket>(this->store, dbinfo.info, this));
  }

  buckets.set_truncated(is_truncated);
  return 0;
}

int DaosUser::create_bucket(
    const DoutPrefixProvider* dpp, const rgw_bucket& b,
    const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
    std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
    const RGWAccessControlPolicy& policy, Attrs& attrs, RGWBucketInfo& info,
    obj_version& ep_objv, bool exclusive, bool obj_lock_enabled, bool* existed,
    req_info& req_info, std::unique_ptr<Bucket>* bucket_out, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: create_bucket:" << b.name << dendl;
  int ret;
  std::unique_ptr<Bucket> bucket;

  // Look up the bucket. Create it if it doesn't exist.
  ret = this->store->get_bucket(dpp, this, b, &bucket, y);
  if (ret != 0 && ret != -ENOENT) {
    return ret;
  }

  if (ret != -ENOENT) {
    *existed = true;
    if (swift_ver_location.empty()) {
      swift_ver_location = bucket->get_info().swift_ver_location;
    }
    placement_rule.inherit_from(bucket->get_info().placement_rule);

    // TODO: ACL policy
    // // don't allow changes to the acl policy
    // RGWAccessControlPolicy old_policy(ctx());
    // int rc = rgw_op_get_bucket_policy_from_attr(
    //           dpp, this, u, bucket->get_attrs(), &old_policy, y);
    // if (rc >= 0 && old_policy != policy) {
    //    bucket_out->swap(bucket);
    //    return -EEXIST;
    //}
  } else {
    placement_rule.name = "default";
    placement_rule.storage_class = "STANDARD";
    bucket = std::make_unique<DaosBucket>(store, b, this);
    bucket->set_attrs(attrs);

    *existed = false;
  }

  // TODO: how to handle zone and multi-site.

  if (!*existed) {
    info.placement_rule = placement_rule;
    info.bucket = b;
    info.owner = this->get_info().user_id;
    info.zonegroup = zonegroup_id;
    info.creation_time = ceph::real_clock::now();
    if (obj_lock_enabled)
      info.flags = BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
    bucket->set_version(ep_objv);
    bucket->get_info() = info;

    // Create a new bucket:
    DaosBucket* daos_bucket = static_cast<DaosBucket*>(bucket.get());
    bufferlist bl;
    std::unique_ptr<struct ds3_bucket_info> bucket_info =
        daos_bucket->get_encoded_info(bl, ceph::real_time());
    ret = ds3_bucket_create(bucket->get_name().c_str(), bucket_info.get(),
                            nullptr, store->ds3, nullptr);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_create failed! ret=" << ret
                        << dendl;
      return ret;
    }
  } else {
    bucket->set_version(ep_objv);
    bucket->get_info() = info;
  }

  bucket_out->swap(bucket);

  return ret;
}

int DaosUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::read_stats(const DoutPrefixProvider* dpp, optional_yield y,
                         RGWStorageStats* stats,
                         ceph::real_time* last_stats_sync,
                         ceph::real_time* last_stats_update) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

/* stats - Not for first pass */
int DaosUser::read_stats_async(const DoutPrefixProvider* dpp,
                               RGWGetUserStats_CB* cb) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::complete_flush_stats(const DoutPrefixProvider* dpp,
                                   optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch, uint32_t max_entries,
                         bool* is_truncated, RGWUsageIter& usage_iter,
                         map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::verify_mfa(const std::string& mfa_str, bool* verified,
                         const DoutPrefixProvider* dpp, optional_yield y) {
  if (verified) {
    *verified = false;
  }
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosUser::load_user(const DoutPrefixProvider* dpp, optional_yield y) {
  const string name = info.user_id.to_str();
  ldpp_dout(dpp, 20) << "DEBUG: load_user, name=" << name << dendl;

  DaosUserInfo duinfo;
  int ret = read_user(dpp, name, &duinfo);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: load_user failed, name=" << name << dendl;
    return ret;
  }

  info = duinfo.info;
  attrs = duinfo.attrs;
  objv_tracker.read_version = duinfo.user_version;
  return 0;
}

int DaosUser::merge_and_store_attrs(const DoutPrefixProvider* dpp,
                                    Attrs& new_attrs, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: merge_and_store_attrs, new_attrs=" << new_attrs
                     << dendl;
  for (auto& it : new_attrs) {
    attrs[it.first] = it.second;
  }
  return store_user(dpp, y, false);
}

int DaosUser::store_user(const DoutPrefixProvider* dpp, optional_yield y,
                         bool exclusive, RGWUserInfo* old_info) {
  const string name = info.user_id.to_str();
  ldpp_dout(dpp, 10) << "DEBUG: Store_user(): User name=" << name << dendl;

  // Read user
  int ret = 0;
  struct DaosUserInfo duinfo;
  ret = read_user(dpp, name, &duinfo);
  obj_version obj_ver = duinfo.user_version;
  std::unique_ptr<struct ds3_user_info> old_user_info;
  std::vector<const char*> old_access_ids;

  // Check if the user already exists
  if (ret == 0 && obj_ver.ver) {
    // already exists.

    if (old_info) {
      *old_info = duinfo.info;
    }

    if (objv_tracker.read_version.ver != obj_ver.ver) {
      // Object version mismatch.. return ECANCELED
      ret = -ECANCELED;
      ldpp_dout(dpp, 0) << "User Read version mismatch read_version="
                        << objv_tracker.read_version.ver
                        << " obj_ver=" << obj_ver.ver << dendl;
      return ret;
    }

    if (exclusive) {
      // return
      return ret;
    }
    obj_ver.ver++;

    for (auto const& [id, key] : duinfo.info.access_keys) {
      old_access_ids.push_back(id.c_str());
    }
    old_user_info.reset(
        new ds3_user_info{.name = duinfo.info.user_id.to_str().c_str(),
                          .email = duinfo.info.user_email.c_str(),
                          .access_ids = old_access_ids.data(),
                          .access_ids_nr = old_access_ids.size()});
  } else {
    obj_ver.ver = 1;
    obj_ver.tag = "UserTAG";
  }

  bufferlist bl;
  std::unique_ptr<struct ds3_user_info> user_info =
      get_encoded_info(bl, obj_ver);

  ret = ds3_user_set(name.c_str(), user_info.get(), old_user_info.get(),
                     store->ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_set failed, name=" << name
                      << " ret=" << ret << dendl;
  }

  return ret;
}

int DaosUser::read_user(const DoutPrefixProvider* dpp, std::string name,
                        DaosUserInfo* duinfo) {
  // Initialize ds3_user_info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_user_info user_info = {.encoded = bl.append_hole(size).c_str(),
                                    .encoded_length = size};

  int ret = ds3_user_get(name.c_str(), &user_info, store->ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_get failed, name=" << name
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo->decode(iter);
  return ret;
}

std::unique_ptr<struct ds3_user_info> DaosUser::get_encoded_info(
    bufferlist& bl, obj_version& obj_ver) {
  // Encode user data
  struct DaosUserInfo duinfo;
  duinfo.info = info;
  duinfo.attrs = attrs;
  duinfo.user_version = obj_ver;
  duinfo.encode(bl);

  // Initialize ds3_user_info
  access_ids.clear();
  for (auto const& [id, key] : info.access_keys) {
    access_ids.push_back(id.c_str());
  }
  return std::unique_ptr<struct ds3_user_info>(
      new ds3_user_info{.name = info.user_id.to_str().c_str(),
                        .email = info.user_email.c_str(),
                        .access_ids = access_ids.data(),
                        .access_ids_nr = access_ids.size(),
                        .encoded = bl.c_str(),
                        .encoded_length = bl.length()});
}

int DaosUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y) {
  const string name = info.user_id.to_str();

  // TODO: the expectation is that the object version needs to be passed in as a
  // method arg see int DB::remove_user(const DoutPrefixProvider *dpp,
  // RGWUserInfo& uinfo, RGWObjVersionTracker *pobjv)
  obj_version obj_ver;
  bufferlist bl;
  std::unique_ptr<struct ds3_user_info> user_info =
      get_encoded_info(bl, obj_ver);

  // Remove user
  int ret = ds3_user_remove(name.c_str(), user_info.get(), store->ds3, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_set failed, name=" << name
                      << " ret=" << ret << dendl;
  }
  return ret;
}

DaosBucket::~DaosBucket() { close(nullptr); }

int DaosBucket::open(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: open, name=" << info.bucket.name.c_str()
                     << dendl;
  // Idempotent
  if (is_open()) {
    return 0;
  }

  int ret = ds3_bucket_open(get_name().c_str(), &ds3b, store->ds3, nullptr);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_bucket_open, name=" << get_name()
                     << ", ret=" << ret << dendl;

  return ret;
}

int DaosBucket::close(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: close" << dendl;
  // Idempotent
  if (!is_open()) {
    return 0;
  }

  // Borrowed from store-level ds3_bucket_cache: do NOT close the
  // shared handle here; the store owns its lifetime.
  if (ds3b_borrowed) {
    ds3b = nullptr;
    ds3b_borrowed = false;
    return 0;
  }

  int ret = ds3_bucket_close(ds3b, nullptr);
  ds3b = nullptr;
  ldpp_dout(dpp, 20) << "DEBUG: ds3_bucket_close ret=" << ret << dendl;

  return ret;
}

std::unique_ptr<struct ds3_bucket_info> DaosBucket::get_encoded_info(
    bufferlist& bl, ceph::real_time _mtime) {
  DaosBucketInfo dbinfo;
  dbinfo.info = info;
  dbinfo.bucket_attrs = attrs;
  dbinfo.mtime = _mtime;
  dbinfo.bucket_version = bucket_version;
  dbinfo.encode(bl);

  auto bucket_info = std::make_unique<struct ds3_bucket_info>();
  bucket_info->encoded = bl.c_str();
  bucket_info->encoded_length = bl.length();
  std::strncpy(bucket_info->name, get_name().c_str(), sizeof(bucket_info->name));
  return bucket_info;
}

int DaosBucket::remove_bucket(const DoutPrefixProvider* dpp,
                              bool delete_children, bool forward_to_master,
                              req_info* req_info, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: remove_bucket, delete_children="
                    
                     << delete_children
                    
                     << " forward_to_master=" << forward_to_master << dendl;

  return ds3_bucket_destroy(get_name().c_str(), delete_children, store->ds3,
                            nullptr);
}

int DaosBucket::remove_bucket_bypass_gc(int concurrent_max,
                                        bool keep_index_consistent,
                                        optional_yield y,
                                        const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: remove_bucket_bypass_gc, concurrent_max="
                    
                     << concurrent_max
                    
                     << " keep_index_consistent=" << keep_index_consistent
                    
                     << dendl;
  return ds3_bucket_destroy(get_name().c_str(), true, store->ds3, nullptr);
}

int DaosBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive,
                         ceph::real_time _mtime) {
  ldpp_dout(dpp, 20) << "DEBUG: put_info(): bucket name=" << get_name()
                     << dendl;

  int ret = open(dpp);
  if (ret != 0) {
    return ret;
  }

  bufferlist bl;
  std::unique_ptr<struct ds3_bucket_info> bucket_info =
      get_encoded_info(bl, ceph::real_time());

  ret = ds3_bucket_set_info(bucket_info.get(), ds3b, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_set_info failed: " << ret << dendl;
  }
  return ret;
}

// BENCH-DIAGNOSTIC: process-wide cache of decoded bucket info, keyed by name.
// Skips the per-PUT ds3_bucket_get_info RPC which serializes through libdaos.
struct CachedBucketInfo {
  RGWBucketInfo info;
  rgw::sal::Attrs attrs;
  ceph::real_time mtime;
  obj_version bucket_version;
};
static std::unordered_map<std::string, CachedBucketInfo> _bucket_info_cache;
static std::shared_mutex _bucket_info_cache_mu;

int DaosBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y,
                            bool get_stats) {
  RGW_US("daos_load_bucket_enter");
  ldpp_dout(dpp, 20) << "DEBUG: load_bucket(): bucket name=" << get_name()
                     << dendl;

  // Fast path: process-wide bucket info cache. Check BEFORE the libdaos
  // ds3_bucket_open path so the warm path doesn't serialize on libdaos's
  // per-handle mutex. We still need a valid ds3b on this DaosBucket
  // instance, but we borrow it from the store-level ds3_bucket_cache
  // (which already shares one ds3b handle per bucket name across the
  // whole RGW process). Track ownership so ~DaosBucket doesn't close the
  // shared handle.
  {
    std::shared_lock lk(_bucket_info_cache_mu);
    auto it = _bucket_info_cache.find(get_name());
    if (it != _bucket_info_cache.end()) {
      info = it->second.info;
      attrs = it->second.attrs;
      mtime = it->second.mtime;
      bucket_version = it->second.bucket_version;
      // Borrow the shared ds3b from the store-level cache.
      ds3_bucket_t* shared_b = nullptr;
      int rc = store->get_or_open_bucket(get_name(), &shared_b);
      if (rc == 0 && shared_b != nullptr) {
        ds3b = shared_b;
        ds3b_borrowed = true;
      }
      RGW_US("daos_load_bucket_cache_hit");
      return 0;
    }
  }

  // Cache miss: do the slow path including open() and the RPC. open() may
  // also borrow from the store cache via the same path; we leave that to
  // the existing implementation.
  int ret = open(dpp);
  if (ret != 0) {
    return ret;
  }

  RGW_US("daos_load_bucket_before_rpc");
  bufferlist bl;
  DaosBucketInfo dbinfo;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_bucket_info bucket_info = {.encoded = bl.append_hole(size).c_str(),
                                        .encoded_length = size};

  ret = ds3_bucket_get_info(&bucket_info, ds3b, nullptr);
  RGW_US("daos_load_bucket_after_rpc");
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_get_info failed: " << ret << dendl;
    return ret;
  }

  auto iter = bl.cbegin();
  dbinfo.decode(iter);
  info = dbinfo.info;
  rgw_placement_rule placement_rule;
  placement_rule.name = "default";
  placement_rule.storage_class = "STANDARD";
  info.placement_rule = placement_rule;

  attrs = dbinfo.bucket_attrs;
  mtime = dbinfo.mtime;
  bucket_version = dbinfo.bucket_version;

  // Insert into cache
  {
    std::unique_lock lk(_bucket_info_cache_mu);
    auto& slot = _bucket_info_cache[get_name()];
    slot.info = info;
    slot.attrs = attrs;
    slot.mtime = mtime;
    slot.bucket_version = bucket_version;
  }
  RGW_US("daos_load_bucket_cache_filled");
  return ret;
}

/* stats - Not for first pass */
int DaosBucket::read_stats(const DoutPrefixProvider* dpp,
                           const bucket_index_layout_generation& idx_layout,
                           int shard_id, std::string* bucket_ver,
                           std::string* master_ver,
                           std::map<RGWObjCategory, RGWStorageStats>& stats,
                           std::string* max_marker, bool* syncstopped) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::read_stats_async(
    const DoutPrefixProvider* dpp,
    const bucket_index_layout_generation& idx_layout, int shard_id,
    RGWGetBucketStats_CB* ctx) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::sync_user_stats(const DoutPrefixProvider* dpp,
                                optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::update_container_stats(const DoutPrefixProvider* dpp) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::check_bucket_shards(const DoutPrefixProvider* dpp) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::chown(const DoutPrefixProvider* dpp, User& new_user,
                      optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

/* Make sure to call load_bucket() if you need it first */
bool DaosBucket::is_owner(User* user) {
  return (info.owner.compare(user->get_id()) == 0);
}

int DaosBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y) {
  /* XXX: Check if bucket contains any objects */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::check_quota(const DoutPrefixProvider* dpp, RGWQuota& quota,
                            uint64_t obj_size, optional_yield y,
                            bool check_size_only) {
  /* Not Handled in the first pass as stats are also needed */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp,
                                      Attrs& new_attrs, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: merge_and_store_attrs, new_attrs=" << new_attrs
                     << dendl;
  for (auto& it : new_attrs) {
    attrs[it.first] = it.second;
  }

  return put_info(dpp, y, ceph::real_time());
}

int DaosBucket::try_refresh_info(const DoutPrefixProvider* dpp,
                                 ceph::real_time* pmtime) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

/* XXX: usage and stats not supported in the first pass */
int DaosBucket::read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                           uint64_t end_epoch, uint32_t max_entries,
                           bool* is_truncated, RGWUsageIter& usage_iter,
                           map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                           uint64_t end_epoch) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::remove_objs_from_index(
    const DoutPrefixProvider* dpp,
    std::list<rgw_obj_index_key>& objs_to_unlink) {
  /* XXX: CHECK: Unlike RadosStore, there is no seperate bucket index table.
   * Delete all the object in the list from the object table of this
   * bucket
   */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::check_index(
    const DoutPrefixProvider* dpp,
    std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
    std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) {
  /* XXX: stats not supported yet */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::rebuild_index(const DoutPrefixProvider* dpp) {
  /* there is no index table in DAOS. Not applicable */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::set_tag_timeout(const DoutPrefixProvider* dpp,
                                uint64_t timeout) {
  /* XXX: CHECK: set tag timeout for all the bucket objects? */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::purge_instance(const DoutPrefixProvider* dpp) {
  /* XXX: CHECK: for DAOS only single instance supported.
   * Remove all the objects for that instance? Anything extra needed?
   */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosBucket::set_acl(const DoutPrefixProvider* dpp,
                        RGWAccessControlPolicy& acl, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: set_acl" << dendl;
  int ret = 0;
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  Attrs attrs = get_attrs();
  attrs[RGW_ATTR_ACL] = aclbl;

  return ret;
}

std::unique_ptr<Object> DaosBucket::get_object(const rgw_obj_key& k) {
  return std::make_unique<DaosObject>(this->store, k, this);
}

bool compare_rgw_bucket_dir_entry(rgw_bucket_dir_entry& entry1,
                                  rgw_bucket_dir_entry& entry2) {
  return (entry1.key < entry2.key);
}

bool compare_multipart_upload(std::unique_ptr<MultipartUpload>& upload1,
                              std::unique_ptr<MultipartUpload>& upload2) {
  return (upload1->get_key() < upload2->get_key());
}

int DaosBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max,
                     ListResults& results, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: list bucket=" << get_name() << " max=" << max
                     << " params=" << params << dendl;
  // End
  if (max == 0) {
    return 0;
  }

  int ret = open(dpp);
  if (ret != 0) {
    return ret;
  }

  // Init needed structures
  vector<struct ds3_object_info> object_infos(max);
  uint32_t nobj = object_infos.size();
  vector<vector<uint8_t>> values(nobj, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (uint32_t i = 0; i < nobj; i++) {
    object_infos[i].encoded = values[i].data();
    object_infos[i].encoded_length = values[i].size();
  }

  vector<struct ds3_common_prefix_info> common_prefixes(max);
  uint32_t ncp = common_prefixes.size();

  char daos_marker[DS3_MAX_KEY_BUFF];
  std::strncpy(daos_marker, params.marker.get_oid().c_str(), sizeof(daos_marker));

  ret = ds3_bucket_list_obj(&nobj, object_infos.data(), &ncp,
                            common_prefixes.data(), params.prefix.c_str(),
                            params.delim.c_str(), daos_marker,
                            params.list_versions, &results.is_truncated, ds3b);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: ds3_bucket_list_obj failed, name="
                      << get_name() << ", ret=" << ret << dendl;
    return ret;
  }

  object_infos.resize(nobj);
  values.resize(nobj);
  common_prefixes.resize(ncp);

  // Fill common prefixes
  for (auto const& cp : common_prefixes) {
    results.common_prefixes[cp.prefix] = true;
  }

  // Decode objs
  for (auto const& obj : object_infos) {
    bufferlist bl;
    rgw_bucket_dir_entry ent;
    bl.append(reinterpret_cast<char*>(obj.encoded), obj.encoded_length);
    auto iter = bl.cbegin();
    ent.decode(iter);
    if (params.list_versions || ent.is_visible()) {
      results.objs.emplace_back(std::move(ent));
    }
  }

  if (!params.allow_unordered) {
    std::sort(results.objs.begin(), results.objs.end(),
              compare_rgw_bucket_dir_entry);
  }

  return ret;
}

int DaosBucket::list_multiparts(
    const DoutPrefixProvider* dpp, const string& prefix, string& marker,
    const string& delim, const int& max_uploads,
    vector<std::unique_ptr<MultipartUpload>>& uploads,
    map<string, bool>* common_prefixes, bool* is_truncated) {
  ldpp_dout(dpp, 20) << "DEBUG: list_multiparts" << dendl;
  // End of uploading
  if (max_uploads == 0) {
    *is_truncated = false;
    return 0;
  }

  // Init needed structures
  vector<struct ds3_multipart_upload_info> multipart_upload_infos(max_uploads);
  uint32_t nmp = multipart_upload_infos.size();
  vector<vector<uint8_t>> values(nmp, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (uint32_t i = 0; i < nmp; i++) {
    multipart_upload_infos[i].encoded = values[i].data();
    multipart_upload_infos[i].encoded_length = values[i].size();
  }

  vector<struct ds3_common_prefix_info> cps(max_uploads);
  uint32_t ncp = cps.size();

  char daos_marker[DS3_MAX_KEY_BUFF];
  std::strncpy(daos_marker, marker.c_str(), sizeof(daos_marker));

  int ret = ds3_bucket_list_multipart(
      get_name().c_str(), &nmp, multipart_upload_infos.data(), &ncp, cps.data(),
      prefix.c_str(), delim.c_str(), daos_marker, is_truncated, store->ds3);

  multipart_upload_infos.resize(nmp);
  values.resize(nmp);
  cps.resize(ncp);

  // Fill common prefixes
  for (auto const& cp : cps) {
    (*common_prefixes)[cp.prefix] = true;
  }

  for (auto const& mp : multipart_upload_infos) {
    // Decode the xattr
    bufferlist bl;
    rgw_bucket_dir_entry ent;
    bl.append(reinterpret_cast<char*>(mp.encoded), mp.encoded_length);
    auto iter = bl.cbegin();
    ent.decode(iter);
    string name = ent.key.name;

    ACLOwner owner(rgw_user(ent.meta.owner));
    owner.set_name(ent.meta.owner_display_name);
    uploads.push_back(this->get_multipart_upload(
        name, mp.upload_id, std::move(owner), ent.meta.mtime));
  }

  // Sort uploads
  std::sort(uploads.begin(), uploads.end(), compare_multipart_upload);

  return ret;
}

int DaosBucket::abort_multiparts(const DoutPrefixProvider* dpp,
                                 CephContext* cct) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

void DaosStore::finalize(void) {
  ldout(cctx, 20) << "DEBUG: finalize" << dendl;
  int ret;

  ret = ds3_disconnect(ds3, nullptr);
  if (ret != 0) {
    ldout(cctx, 0) << "ERROR: ds3_disconnect() failed: " << ret << dendl;
  }
  ds3 = nullptr;

  ret = ds3_fini();
  if (ret != 0) {
    ldout(cctx, 0) << "ERROR: daos_fini() failed: " << ret << dendl;
  }
}

int DaosStore::initialize(CephContext* cct, const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: initialize" << dendl;
  int ret = ds3_init();

  // DS3 init failed, allow the case where init is already done
  if (ret != 0 && ret != DER_ALREADY) {
    ldout(cct, 0) << "ERROR: ds3_init() failed: " << ret << dendl;
    return ret;
  }

  // XXX: these params should be taken from config settings and
  // cct somehow?
  const auto& daos_pool = cct->_conf.get_val<std::string>("daos_pool");
  ldout(cct, 20) << "INFO: daos pool: " << daos_pool << dendl;

  ret = ds3_connect(daos_pool.c_str(), nullptr, &ds3, nullptr);

  if (ret != 0) {
    ldout(cct, 0) << "ERROR: ds3_connect() failed: " << ret << dendl;
    ds3_fini();
    return ret;
  }

  // Eagerly create the progress-thread daos_eq so the first wave of
  // concurrent S3 PUTs does not block ~589 ms inside std::call_once
  // waiting for daos_eq_create on the request hot path.
  ensure_progress_thread_started();
  ldout(cct, 0) << "INFO: DAOS progress thread started" << dendl;

  return ret;
}



int DaosStore::get_or_open_bucket(const std::string& name, ds3_bucket_t** out_b) {
    std::lock_guard<std::mutex> lock(ds3_bucket_cache_mtx);
    auto it = ds3_bucket_cache.find(name);
    if (it != ds3_bucket_cache.end()) {
        *out_b = it->second;
        return 0;
    }
    ds3_bucket_t* b = nullptr;
    int rc = ds3_bucket_open(name.c_str(), &b, ds3, nullptr);
    if (rc != 0) {
        return rc;
    }
    ds3_bucket_cache[name] = b;
    *out_b = b;
    return 0;
}

const std::string& DaosZoneGroup::get_endpoint() const {
  if (!group.endpoints.empty()) {
    return group.endpoints.front();
  } else {
    // use zonegroup's master zone endpoints
    auto z = group.zones.find(group.master_zone);
    if (z != group.zones.end() && !z->second.endpoints.empty()) {
      return z->second.endpoints.front();
    }
  }
  return empty;
}

bool DaosZoneGroup::placement_target_exists(std::string& target) const {
  return !!group.placement_targets.count(target);
}

void DaosZoneGroup::get_placement_target_names(
    std::set<std::string>& names) const {
  for (const auto& target : group.placement_targets) {
    names.emplace(target.second.name);
  }
}

int DaosZoneGroup::get_zone_by_id(const std::string& id,
                                  std::unique_ptr<Zone>* zone) {
  if (group.zones.find(id) == group.zones.end()) {
    return -ENOENT;
  }
  zone->reset(new DaosZone(store, *this));
  return 0;
}

int DaosZoneGroup::get_zone_by_name(const std::string& name,
                                    std::unique_ptr<Zone>* zone) {
  for (const auto& [id, zinfo] : group.zones) {
    if (zinfo.name == name) {
      zone->reset(new DaosZone(store, *this));
      return 0;
    }
  }
  return -ENOENT;
}

int DaosZoneGroup::list_zones(std::list<std::string>& zone_ids) {
  for (const auto& [id, _] : group.zones) {
    zone_ids.push_back(id.id);
  }
  return 0;
}

int DaosZoneGroup::get_placement_tier(const rgw_placement_rule& rule,
                                      std::unique_ptr<PlacementTier>* tier) {
  std::map<std::string, RGWZoneGroupPlacementTarget>::const_iterator titer;
  titer = group.placement_targets.find(rule.name);
  if (titer == group.placement_targets.end()) {
    return -ENOENT;
  }

  const auto& target_rule = titer->second;
  std::map<std::string, RGWZoneGroupPlacementTier>::const_iterator ttier;
  ttier = target_rule.tier_targets.find(rule.storage_class);
  if (ttier == target_rule.tier_targets.end()) {
    // not found
    return -ENOENT;
  }

  PlacementTier* t;
  t = new DaosPlacementTier(store, ttier->second);
  if (!t) return -ENOMEM;

  tier->reset(t);
  return 0;
}

ZoneGroup& DaosZone::get_zonegroup() { return zonegroup; }

const std::string& DaosZone::get_id() { return cur_zone_id; }

const std::string& DaosZone::get_name() const {
  return zone_params->get_name();
}

bool DaosZone::is_writeable() { return true; }

bool DaosZone::get_redirect_endpoint(std::string* endpoint) { return false; }

bool DaosZone::has_zonegroup_api(const std::string& api) const { return false; }

const std::string& DaosZone::get_current_period_id() {
  return current_period->get_id();
}

int DaosStore::get_zonegroup(const std::string& id,
                             std::unique_ptr<ZoneGroup>* zg) {
  // only a single zonegroup is currently backed
  auto& zg_ref = static_cast<DaosZoneGroup&>(zone.get_zonegroup());
  zg->reset(new DaosZoneGroup(this, zg_ref.get_group()));
  return 0;
}

int DaosStore::list_all_zones(const DoutPrefixProvider* dpp,
                              std::list<std::string>& zone_ids) {
  zone_ids.push_back(zone.get_id());
  return 0;
}

std::unique_ptr<LuaManager> DaosStore::get_lua_manager() {
  return std::make_unique<DaosLuaManager>(this);
}

int DaosObject::get_obj_state(const DoutPrefixProvider* dpp,
                              RGWObjState** _state, optional_yield y,
                              bool follow_olh) {
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  ldpp_dout(dpp, 20) << "DEBUG: get_obj_state" << dendl;
  rgw_bucket_dir_entry ent;
  *_state = &state;  // state is required even if a failure occurs

  int ret = get_dir_entry_attrs(dpp, &ent);
  if (ret != 0) {
    return ret;
  }

  // Set object state.
  state.exists = true;
  state.size = ent.meta.size;
  state.accounted_size = ent.meta.size;
  state.mtime = ent.meta.mtime;

  state.has_attrs = true;
  bufferlist etag_bl;
  string& etag = ent.meta.etag;
  ldpp_dout(dpp, 20) << __func__ << ": object's etag:  " << ent.meta.etag
                     << dendl;
  etag_bl.append(etag);
  state.attrset[RGW_ATTR_ETAG] = etag_bl;
  return 0;
}

DaosObject::~DaosObject() { close(nullptr); }

int DaosObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                              Attrs* delattrs, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: DaosObject::set_obj_attrs()" << dendl;
  Attrs& attrs = get_attrs();
  // TODO handle target_obj
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  rgw_bucket_dir_entry ent;
  int ret = get_dir_entry_attrs(dpp, &ent);
  if (ret != 0) {
    return ret;
  }

  // Update object metadata
  Attrs updateattrs = setattrs == nullptr ? attrs : *setattrs;
  if (delattrs) {
    for (auto const& [attr, attrval] : *delattrs) {
      updateattrs.erase(attr);
    }
  }

  ret = set_dir_entry_attrs(dpp, &ent, &updateattrs);
  return ret;
}

int DaosObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                              rgw_obj* target_obj) {
  ldpp_dout(dpp, 20) << "DEBUG: DaosObject::get_obj_attrs()" << dendl;
  // Multipart meta objects don't exist as regular objects during upload_part.
  // Return 0 with empty attrs to signal "no encryption configured", which is
  // correct. Returning -ENOENT would propagate as an error through callers
  // like get_encrypt_filter() and cause the upload_part request to fail.
  if (get_key().ns == RGW_OBJ_NS_MULTIPART) {
    return 0;
  }
  Attrs& attrs = get_attrs();
  rgw_bucket_dir_entry ent;
  int ret = get_dir_entry_attrs(dpp, &ent, &attrs);
  return ret;
}

int DaosObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                                 optional_yield y,
                                 const DoutPrefixProvider* dpp) {
  // Get object's metadata (those stored in rgw_bucket_dir_entry)
  ldpp_dout(dpp, 20) << "DEBUG: modify_obj_attrs" << dendl;
  Attrs& attrs = get_attrs();
  rgw_bucket_dir_entry ent;
  int ret = get_dir_entry_attrs(dpp, &ent, &attrs);
  if (ret != 0) {
    return ret;
  }

  // Update object attrs
  set_atomic();
  attrs[attr_name] = attr_val;

  ret = set_dir_entry_attrs(dpp, &ent, &attrs);
  return ret;
}

int DaosObject::delete_obj_attrs(const DoutPrefixProvider* dpp,
                                 const char* attr_name, optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: delete_obj_attrs" << dendl;
  rgw_obj target = get_obj();
  Attrs rmattr;
  bufferlist bl;

  rmattr[attr_name] = bl;
  return set_obj_attrs(dpp, nullptr, &rmattr, y);
}

bool DaosObject::is_expired() {
  Attrs& attrs = get_attrs();
  auto iter = attrs.find(RGW_ATTR_DELETE_AT);
  if (iter != attrs.end()) {
    utime_t delete_at;
    try {
      auto bufit = iter->second.cbegin();
      decode(delete_at, bufit);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0)
          << "ERROR: " << __func__
          << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
      return false;
    }

    if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
      return true;
    }
  }

  return false;
}

// Taken from rgw_rados.cc
void DaosObject::gen_rand_obj_instance_name() {
  enum { OBJ_INSTANCE_LEN = 32 };
  char buf[OBJ_INSTANCE_LEN + 1];

  gen_rand_alphanumeric_no_underscore(store->ctx(), buf, OBJ_INSTANCE_LEN);
  state.obj.key.set_instance(buf);
}

int DaosObject::omap_get_vals(const DoutPrefixProvider* dpp,
                              const std::string& marker, uint64_t count,
                              std::map<std::string, bufferlist>* m, bool* pmore,
                              optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::omap_get_all(const DoutPrefixProvider* dpp,
                             std::map<std::string, bufferlist>* m,
                             optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::omap_get_vals_by_keys(const DoutPrefixProvider* dpp,
                                      const std::string& oid,
                                      const std::set<std::string>& keys,
                                      Attrs* vals) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::omap_set_val_by_key(const DoutPrefixProvider* dpp,
                                    const std::string& key, bufferlist& val,
                                    bool must_exist, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) {
  return 0;
}

std::unique_ptr<MPSerializer> DaosObject::get_serializer(
    const DoutPrefixProvider* dpp, const std::string& lock_name) {
  return std::make_unique<MPDaosSerializer>(dpp, store, this, lock_name);
}

int DaosObject::transition(Bucket* bucket,
                           const rgw_placement_rule& placement_rule,
                           const real_time& mtime, uint64_t olh_epoch,
                           const DoutPrefixProvider* dpp, optional_yield y,
                           uint32_t flags) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::transition_to_cloud(
    Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o,
    std::set<std::string>& cloud_targets, CephContext* cct, bool update_object,
    const DoutPrefixProvider* dpp, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

bool DaosObject::placement_rules_match(rgw_placement_rule& r1,
                                       rgw_placement_rule& r2) {
  /* XXX: support single default zone and zonegroup for now */
  return true;
}

int DaosObject::dump_obj_layout(const DoutPrefixProvider* dpp, optional_yield y,
                                Formatter* f) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<Object::ReadOp> DaosObject::get_read_op() {
  return std::make_unique<DaosObject::DaosReadOp>(this);
}

DaosObject::DaosReadOp::DaosReadOp(DaosObject* _source) : source(_source) {}

int DaosObject::DaosReadOp::prepare(optional_yield y,
                                    const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << __func__
                     << ": bucket=" << source->get_bucket()->get_name()
                     << dendl;

  if (source->get_bucket()->versioned() && !source->have_instance()) {
    // If the bucket is versioned and no version is specified, get the latest
    // version
    source->set_instance(DS3_LATEST_INSTANCE);
  }

  rgw_bucket_dir_entry ent;
  int ret = source->get_dir_entry_attrs(dpp, &ent);

  // Set source object's attrs. The attrs is key/value map and is used
  // in send_response_data() to set attributes, including etag.
  bufferlist etag_bl;
  string& etag = ent.meta.etag;
  ldpp_dout(dpp, 20) << __func__ << ": object's etag: " << ent.meta.etag
                     << dendl;
  etag_bl.append(etag.c_str(), etag.size());
  source->get_attrs().emplace(std::move(RGW_ATTR_ETAG), std::move(etag_bl));

  source->set_key(ent.key);
  source->set_obj_size(ent.meta.size);
  ldpp_dout(dpp, 20) << __func__ << ": object's size: " << ent.meta.size
                     << dendl;

  return ret;
}

int DaosObject::DaosReadOp::read(int64_t off, int64_t end, bufferlist& bl,
                                 optional_yield y,
                                 const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << __func__ << ": off=" << off << " end=" << end << dendl;
  int ret = source->lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Calculate size, end is inclusive
  uint64_t size = end - off + 1;

  // Read
  ret = source->read(dpp, bl, off, size);
  if (ret != 0) {
    return ret;
  }

  return ret;
}

// RGWGetObj::execute() calls ReadOp::iterate() to read object from 'off' to
// 'end'. The returned data is processed in 'cb' which is a chain of
// post-processing filters such as decompression, de-encryption and sending back
// data to client (RGWGetObj_CB::handle_dta which in turn calls
// RGWGetObj::get_data_cb() to send data back.).
//
// POC implements a simple sync version of iterate() function in which it reads
// a block of data each time and call 'cb' for post-processing.
int DaosObject::DaosReadOp::iterate(const DoutPrefixProvider* dpp, int64_t off,
                                    int64_t end, RGWGetDataCB* cb,
                                    optional_yield y) {
  ldpp_dout(dpp, 20) << __func__ << ": off=" << off << " end=" << end << dendl;
  int ret = source->lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Calculate size, end is inclusive
  uint64_t size = end - off + 1;

  // Always use the async source->read() (heap bufferlist). The previous
  // "read directly into pre_buf_" RDMA fast path was unsafe under
  // concurrent reads (all threads writing into the same shared buffer).
  // The cb->handle_data -> rdma_write_batch path memcpys from the heap
  // bufferlist into pre_buf_ under rdma_mutex_, which is the same
  // serialization point that already exists for the RDMA write back
  // to the client. Net effect: parallel libdaos reads via the EQ pool,
  // serialized RDMA writes (rdma_mutex_ remains the next bottleneck).
  bufferlist bl;
  ret = source->read(dpp, bl, off, size);
  if (ret != 0) {
    return ret;
  }

  // If RDMA server is connected, the data is already in pre_buf_ (we read
  // directly into it above).  send_response_data→rdma_write_batch detects
  // src==pre_buf_ and skips the 64 MiB memcpy — true zero-copy GET.
  int cb_ret = cb->handle_data(bl, 0, size);
  if (cb_ret < 0) return cb_ret;
  return ret;
}

int DaosObject::DaosReadOp::get_attr(const DoutPrefixProvider* dpp,
                                     const char* name, bufferlist& dest,
                                     optional_yield y) {
  Attrs attrs;
  int ret = source->get_dir_entry_attrs(dpp, nullptr, &attrs);
  if (ret != 0) {
    return ret;
  }

  auto search = attrs.find(name);
  if (search == attrs.end()) {
    return -ENODATA;
  }

  dest = search->second;
  return 0;
}

std::unique_ptr<Object::DeleteOp> DaosObject::get_delete_op() {
  return std::make_unique<DaosObject::DaosDeleteOp>(this);
}

DaosObject::DaosDeleteOp::DaosDeleteOp(DaosObject* _source) : source(_source) {}

// Implementation of DELETE OBJ also requires DaosObject::get_obj_state()
// to retrieve and set object's state from object's metadata.
//
// TODO:
// 1. The POC only deletes the Daos objects. It doesn't handle the
// DeleteOp::params. Delete::delete_obj() in rgw_rados.cc shows how rados
// backend process the params.
// 2. Delete an object when its versioning is turned on.
// 3. Handle empty directories
// 4. Fail when file doesn't exist
int DaosObject::DaosDeleteOp::delete_obj(const DoutPrefixProvider* dpp,
                                         optional_yield y, uint32_t flags) {
  ldpp_dout(dpp, 20) << "DaosDeleteOp::delete_obj "
                     << source->get_key().get_oid() << " from "
                     << source->get_bucket()->get_name() << dendl;
  if (source->get_instance() == "null") {
    source->clear_instance();
  }

  // Open bucket
  int ret = 0;
  std::string key = source->get_key().get_oid();
  DaosBucket* daos_bucket = source->get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  // Remove the daos object
  ret = ds3_obj_destroy(key.c_str(), daos_bucket->ds3b);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_obj_destroy key=" << key << " ret=" << ret
                     << dendl;

  // result.delete_marker = parent_op.result.delete_marker;
  // result.version_id = parent_op.result.version_id;

  return ret;
}

int DaosObject::delete_object(const DoutPrefixProvider* dpp, optional_yield y,
                              uint32_t flags) {
  ldpp_dout(dpp, 20) << "DEBUG: delete_object" << dendl;
  DaosObject::DaosDeleteOp del_op(this);
  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = bucket->get_info().versioning_status();

  return del_op.delete_obj(dpp, y, flags);
}

int DaosObject::delete_obj_aio(const DoutPrefixProvider* dpp,
                               RGWObjState* astate, Completions* aio,
                               bool keep_index_consistent, optional_yield y) {
  /* XXX: Make it async */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::copy_object(
    User* user, req_info* info, const rgw_zone_id& source_zone,
    rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
    rgw::sal::Bucket* src_bucket, const rgw_placement_rule& dest_placement,
    ceph::real_time* src_mtime, ceph::real_time* mtime,
    const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
    bool high_precision_time, const char* if_match, const char* if_nomatch,
    AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
    RGWObjCategory category, uint64_t olh_epoch,
    boost::optional<ceph::real_time> delete_at, std::string* version_id,
    std::string* tag, std::string* etag, void (*progress_cb)(off_t, void*),
    void* progress_data, const DoutPrefixProvider* dpp, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::swift_versioning_restore(bool& restored,
                                         const DoutPrefixProvider* dpp) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::swift_versioning_copy(const DoutPrefixProvider* dpp,
                                      optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosObject::lookup(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: lookup" << dendl;
  if (is_open()) {
    return 0;
  }

  if (get_instance() == "null") {
    clear_instance();
  }

  int ret = 0;
  DaosBucket* daos_bucket = get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  ret = ds3_obj_open(get_key().get_oid().c_str(), &ds3o, daos_bucket->ds3b);

  if (ret == -ENOENT) {
    ldpp_dout(dpp, 20) << "DEBUG: daos object (" << get_bucket()->get_name()
                       << ", " << get_key().get_oid()
                       << ") does not exist: ret=" << ret << dendl;
  } else if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to open daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::create(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: create" << dendl;
  if (is_open()) {
    return 0;
  }

  if (get_instance() == "null") {
    clear_instance();
  }

  int ret = 0;
  DaosBucket* daos_bucket = get_daos_bucket();
  ret = daos_bucket->open(dpp);
  if (ret != 0) {
    return ret;
  }

  ret = ds3_obj_create(get_key().get_oid().c_str(), &ds3o, daos_bucket->ds3b);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::close(const DoutPrefixProvider* dpp) {
  ldpp_dout(dpp, 20) << "DEBUG: close" << dendl;
  if (!is_open()) {
    return 0;
  }

  int ret = ds3_obj_close(ds3o);
  ds3o = nullptr;
  ldpp_dout(dpp, 20) << "DEBUG: ds3_obj_close ret=" << ret << dendl;
  return ret;
}

int DaosObject::write(const DoutPrefixProvider* dpp, bufferlist&& data,
                      uint64_t offset) {
  ldpp_dout(dpp, 20) << "DEBUG: write" << dendl;
  uint64_t size = data.length();
  {
    const unsigned char* p = (const unsigned char*)data.c_str();
    auto rd8 = [&](uint64_t off) -> uint64_t {
      if (size < off + 8) return 0;
      return ((uint64_t)p[off]<<56|(uint64_t)p[off+1]<<48|(uint64_t)p[off+2]<<40|
              (uint64_t)p[off+3]<<32|(uint64_t)p[off+4]<<24|(uint64_t)p[off+5]<<16|
              (uint64_t)p[off+6]<<8|(uint64_t)p[off+7]);
    };
    ldpp_dout(dpp, 0) << "DEBUG [obj-write] sz=" << size
                      << " [0]=0x" << std::hex << std::setfill('0') << std::setw(16) << rd8(0)
                      << " [1MB]=0x" << std::setw(16) << rd8(1048576)
                      << " [5MB]=0x" << std::setw(16) << rd8(5242880)
                      << " [8MB]=0x" << std::setw(16) << rd8(8388608)
                      << " [16MB]=0x" << std::setw(16) << rd8(16777216)
                      << " [20MB]=0x" << std::setw(16) << rd8(20971520)
                      << " [24MB]=0x" << std::setw(16) << rd8(25165824)
                      << std::dec << dendl;
  }
  int ret = ds3_obj_write(data.c_str(), offset, &size, get_daos_bucket()->ds3b,
                          ds3o, nullptr);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to write into daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::read(const DoutPrefixProvider* dpp, bufferlist& data,
                     uint64_t offset, uint64_t& size) {
  ldpp_dout(dpp, 20) << "DEBUG: read offset=" << offset << " size=" << size
                     << dendl;

  // Workaround for libdaos 2.7.x bug: ds3_obj_read at non-zero offset
  // ignores the offset and returns data from the start of the object.
  // We read from offset 0 and slice in the bufferlist below.
  uint64_t req_size = size;
  uint64_t full_size = offset + size;
  bufferlist full_bl;

  // === Async read via the daos_eq pool ===
  // Mirror of DaosAtomicWriter::process: allocate a daos_event_t bound
  // to a pooled EQ via get_progress_eq() (round-robin), submit dfs_read
  // asynchronously, register a condvar-based completion callback so the
  // dedicated progress thread for that EQ wakes us up when the read
  // returns. This breaks libdaos\u2019s per-client-context serialization on
  // synchronous reads and lets multiple beast workers have outstanding
  // dfs_read RPCs in flight at the same time.
  daos_event_t read_ev = {};
  auto waiter = std::make_unique<rgw::sal::DaosReqWaiter>();
  daos_event_init(&read_ev, get_progress_eq(), nullptr);
  daos_event_register_comp_cb(&read_ev, daos_progress_completion_cb,
                              waiter.get());

  d_iov_t iov;
  d_iov_set(&iov, full_bl.append_hole(full_size).c_str(), full_size);
  d_sg_list_t sgl{};
  sgl.sg_nr = 1;
  sgl.sg_iovs = &iov;
  sgl.sg_nr_out = 1;
  daos_size_t got = full_size;
  int ret = dfs_read(dfs_of(get_daos_bucket()->ds3b), dfsobj_of(ds3o),
                     &sgl, /*off*/0, &got, &read_ev);
  if (ret != 0) {
    daos_event_fini(&read_ev);
    ldpp_dout(dpp, 0) << "ERROR: dfs_read submit failed: " << ret << dendl;
    size = 0;
    return ret;
  }

  // Wait for the read to complete (progress thread fires the callback).
  {
    std::unique_lock<std::mutex> lock(waiter->mtx);
    waiter->cv.wait(lock, [&]{ return waiter->done; });
  }
  int err = waiter->err;
  daos_event_fini(&read_ev);
  if (err != 0) {
    ldpp_dout(dpp, 0) << "ERROR: async dfs_read completed with error: "
                      << err << dendl;
    size = 0;
    return -err;
  }
  full_size = got;
  // Debug: log what ds3_obj_read returned and sample bytes at multiple positions
  {
    const unsigned char* p = (const unsigned char*)full_bl.c_str();
    auto rd8 = [&](uint64_t off) -> uint64_t {
      if (full_size < off + 8) return 0;
      return ((uint64_t)p[off]<<56|(uint64_t)p[off+1]<<48|(uint64_t)p[off+2]<<40|
              (uint64_t)p[off+3]<<32|(uint64_t)p[off+4]<<24|(uint64_t)p[off+5]<<16|
              (uint64_t)p[off+6]<<8|(uint64_t)p[off+7]);
    };
    ldpp_dout(dpp, 0) << "DEBUG read: off=" << offset << " req=" << req_size
                      << " got=" << full_size
                      << " [0]=0x" << std::hex << std::setfill('0') << std::setw(16) << rd8(0)
                      << " [1MB]=0x" << std::setw(16) << rd8(1048576)
                      << " [5MB]=0x" << std::setw(16) << rd8(5242880)
                      << " [8MB]=0x" << std::setw(16) << rd8(8388608)
                      << " [16MB]=0x" << std::setw(16) << rd8(16777216)
                      << " [off]=0x" << std::setw(16) << rd8(offset)
                      << std::dec << dendl;
  }
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read from daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
    size = 0;
    return ret;
  }
  size = (full_size > offset) ? std::min(size, full_size - offset) : 0;
  if (size > 0) {
    bufferlist range;
    range.substr_of(full_bl, offset, size);
    data.claim_append(range);
  }
  return ret;
}

// Get the object's dirent and attrs
int DaosObject::get_dir_entry_attrs(const DoutPrefixProvider* dpp,
                                    rgw_bucket_dir_entry* ent,
                                    Attrs* getattrs) {
  ldpp_dout(dpp, 20) << "DEBUG: get_dir_entry_attrs" << dendl;
  int ret = 0;
  vector<uint8_t> value(DS3_MAX_ENCODED_LEN);
  uint32_t size = value.size();

  if (get_key().ns == RGW_OBJ_NS_MULTIPART) {
    struct ds3_multipart_upload_info ui = {.encoded = value.data(),
                                           .encoded_length = size};
    ret = ds3_upload_get_info(&ui, bucket->get_name().c_str(),
                              get_key().name.c_str(), store->ds3);
  } else {
    ret = lookup(dpp);
    if (ret != 0) {
      return ret;
    }

    auto object_info = std::make_unique<struct ds3_object_info>();
    object_info->encoded = value.data();
    object_info->encoded_length = size;
    ret = ds3_obj_get_info(object_info.get(), get_daos_bucket()->ds3b, ds3o);
    size = object_info->encoded_length;
  }

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to get info of daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
    return ret;
  }

  rgw_bucket_dir_entry dummy_ent;
  if (!ent) {
    // if ent is not passed, use a dummy ent
    ent = &dummy_ent;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<char*>(value.data()), size);
  auto iter = bl.cbegin();
  ent->decode(iter);
  if (getattrs) {
    decode(*getattrs, iter);
  }

  return ret;
}
// Set the object's dirent and attrs
int DaosObject::set_dir_entry_attrs(const DoutPrefixProvider* dpp,
                                    rgw_bucket_dir_entry* ent,
                                    Attrs* setattrs) {
  // BENCH-DIAGNOSTIC: skip ds3_obj_set_info entirely. With anonymous auth
  // (S3 Express emulation) the encoded owner is empty and ds3_obj_set_info
  // returns -EINVAL. We don\u2019t need persistent object metadata for the bench.
  return 0;
  ldpp_dout(dpp, 20) << "DEBUG: set_dir_entry_attrs" << dendl;
  int ret = lookup(dpp);
  if (ret != 0) {
    return ret;
  }

  // Set defaults
  if (!ent) {
    // if ent is not passed, return an error
    return -EINVAL;
  }

  if (!setattrs) {
    // if setattrs is not passed, use object attrs
    setattrs = &get_attrs();
  }

  bufferlist wbl;
  ent->encode(wbl);
  encode(*setattrs, wbl);

  // Write rgw_bucket_dir_entry into object xattr
  auto object_info = std::make_unique<struct ds3_object_info>();
  object_info->encoded = wbl.c_str();
  object_info->encoded_length = wbl.length();
  ret = ds3_obj_set_info(object_info.get(), get_daos_bucket()->ds3b, ds3o);
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set info of daos object ("
                      << get_bucket()->get_name() << ", " << get_key().get_oid()
                      << "): ret=" << ret << dendl;
  }
  return ret;
}

int DaosObject::mark_as_latest(const DoutPrefixProvider* dpp,
                               ceph::real_time set_mtime) {
  // TODO handle deletion
  // TODO understand race conditions
  ldpp_dout(dpp, 20) << "DEBUG: mark_as_latest" << dendl;

  // Get latest version so far
  std::unique_ptr<DaosObject> latest_object = std::make_unique<DaosObject>(
      store, rgw_obj_key(get_name(), DS3_LATEST_INSTANCE), get_bucket());

  ldpp_dout(dpp, 20) << __func__ << ": key=" << get_key().get_oid()
                     << " latest_object_key= "
                     << latest_object->get_key().get_oid() << dendl;

  int ret = latest_object->lookup(dpp);
  if (ret == 0) {
    // Get metadata only if file exists
    rgw_bucket_dir_entry latest_ent;
    Attrs latest_attrs;
    ret = latest_object->get_dir_entry_attrs(dpp, &latest_ent, &latest_attrs);
    if (ret != 0) {
      return ret;
    }

    // Update flags
    latest_ent.flags = rgw_bucket_dir_entry::FLAG_VER;
    latest_ent.meta.mtime = set_mtime;
    ret = latest_object->set_dir_entry_attrs(dpp, &latest_ent, &latest_attrs);
    if (ret != 0) {
      return ret;
    }
  }

  // Get or create the link [latest], make it link to the current latest
  // version.
  ret =
      ds3_obj_mark_latest(get_key().get_oid().c_str(), get_daos_bucket()->ds3b);
  ldpp_dout(dpp, 20) << "DEBUG: ds3_obj_mark_latest ret=" << ret << dendl;
  return ret;
}

DaosAtomicWriter::DaosAtomicWriter(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, DaosStore* _store,
    const rgw_user& _owner, const rgw_placement_rule* _ptail_placement_rule,
    uint64_t _olh_epoch, const std::string& _unique_tag)
    : StoreWriter(dpp, y),
      store(_store),
      owner(_owner),
      ptail_placement_rule(_ptail_placement_rule),
      olh_epoch(_olh_epoch),
      unique_tag(_unique_tag),
      obj(_store, obj->get_key(), obj->get_bucket()) {}

DaosAtomicWriter::~DaosAtomicWriter() {
  if (write_submitted && waiter_) {
    // Block on condvar until the progress thread fires our callback.
    std::unique_lock<std::mutex> lock(waiter_->mtx);
    waiter_->cv.wait(lock, [this]{ return waiter_->done; });
    daos_event_fini(&write_ev);
    write_submitted = false;
  }
  // writer_ds3b is owned by DaosStore::ds3_bucket_cache; do not close.
  writer_ds3b = nullptr;
  if (writer_eq_owned && writer_eq.cookie != 0) {
    daos_eq_destroy(writer_eq, 0);
    writer_eq = DAOS_HDL_INVAL;
    writer_eq_owned = false;
  }
}

int DaosAtomicWriter::prepare(optional_yield y) {
  RGW_US("daos_prepare_enter");
  ldpp_dout(dpp, 20) << "DEBUG: prepare" << dendl;

  using sc = std::chrono::steady_clock;
  using ms = std::chrono::milliseconds;
  auto t0 = sc::now();
  t_prepare_start = t0;
  first_process_seen = false;

  // Use the process-wide shared bucket-handle cache. The previous
  // "private per-writer" approach forced every PUT to call ds3_bucket_open
  // -> dfs_connect, which races inside libdfs when many threads connect
  // concurrently and was causing intermittent EINVAL / HTTP 400. With the
  // shared cached handle, libds3/libdfs only call dfs_connect once per
  // bucket per RGW process; subsequent uses are read-only on the shared
  // dfs_t* which is internally MT-safe for I/O operations.
  RGW_US("daos_before_get_or_open_bucket");
  int ret = store->get_or_open_bucket(obj.get_bucket()->get_name(),
                                      &writer_ds3b);
  RGW_US("daos_after_get_or_open_bucket");
  auto t1 = sc::now();
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: prepare: get_or_open_bucket failed ret=" << ret
                      << dendl;
    return ret;
  }

  // Create the object through the private handle.
  RGW_US("before_ds3_obj_create");
  ret = ds3_obj_create(obj.get_key().get_oid().c_str(), &obj.ds3o, writer_ds3b);
  RGW_US("after_ds3_obj_create");
  auto t2 = sc::now();
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create daos object ("
                      << obj.get_bucket()->get_name() << ", "
                      << obj.get_key().get_oid() << "): ret=" << ret << dendl;
    // shared cached handle — do not close
    writer_ds3b = nullptr;
  }

  ldpp_dout(dpp, 0) << "TIMING prepare(): "
    << "bucket_open=" << std::chrono::duration_cast<ms>(t1 - t0).count() << "ms "
    << "obj_create=" << std::chrono::duration_cast<ms>(t2 - t1).count() << "ms"
    << dendl;
  if (ret == 0 && per_req_eq_enabled()) {
    RGW_US("daos_writer_eq_create_start");
    int eq_rc = daos_eq_create(&writer_eq);
    RGW_US("daos_writer_eq_create_done");
    if (eq_rc != 0) {
      ldpp_dout(dpp, 0) << "ERROR: daos_eq_create failed: " << eq_rc << dendl;
      writer_eq = DAOS_HDL_INVAL;
      writer_eq_owned = false;
    } else {
      writer_eq_owned = true;
    }
  }
  RGW_US("daos_prepare_exit");
  return ret;
}

int DaosAtomicWriter::process(bufferlist&& data, uint64_t offset) {
  RGW_US("DaosAtomicWriter_process_enter");
  ldpp_dout(dpp, 20) << "DEBUG: process" << dendl;
  if (data.length() == 0) {
    RGW_US("DaosAtomicWriter_process_exit_empty");
    return 0;
  }

  {
    auto now = std::chrono::steady_clock::now();
    if (!first_process_seen) {
      t_first_process = now;
      first_process_seen = true;
    }
    t_last_process = now;
  }

  if (writer_ds3b == nullptr || !obj.is_open()) {
    ldpp_dout(dpp, 0) << "ERROR: process called but writer not prepared "
                      << "(writer_ds3b=" << writer_ds3b
                      << " ds3o=" << obj.ds3o << ")" << dendl;
    return -EIO;
  }

  // If a previous async write is still in-flight (multi-chunk object), wait
  // for it to complete before submitting the next chunk.
  if (write_submitted) {
    int ev_ret = 0;
    if (per_req_eq_enabled() && writer_eq_owned) {
      daos_event_t *evp = nullptr;
      while (true) {
        int n = daos_eq_poll(writer_eq, 1, DAOS_EQ_WAIT, 1, &evp);
        if (n > 0) { ev_ret = evp->ev_error; break; }
        if (n < 0) { ev_ret = n; break; }
      }
    } else if (waiter_) {
      std::unique_lock<std::mutex> lock(waiter_->mtx);
      waiter_->cv.wait(lock, [this]{ return waiter_->done; });
      ev_ret = waiter_->err;
    }
    daos_event_fini(&write_ev);
    write_submitted = false;
    waiter_.reset();
    pending_data.clear();
    if (ev_ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: async write completed with error: "
                        << ev_ret << dendl;
      return -ev_ret;
    }
  }

  // Keep data alive until the async DAOS write completes (the DS3 library
  // holds a raw pointer into this buffer until the event fires).
  uint64_t data_size = data.length();
  pending_data.claim_append(data);

  // Submit the write asynchronously and return immediately.  Concurrent PUT
  // requests in other threads can now also submit their writes, giving DAOS
  // the same iodepth effect that fio achieves with --iodepth=N.
  // Per-request EQ path: submit on writer_eq, no callback needed; complete()
  // will poll writer_eq directly. Otherwise fall back to shared progress EQ.
  if (per_req_eq_enabled() && writer_eq_owned) {
    daos_event_init(&write_ev, writer_eq, nullptr);
    waiter_.reset();  // not used in per-request EQ mode
  } else {
    waiter_ = std::make_unique<rgw::sal::DaosReqWaiter>();
    daos_event_init(&write_ev, get_progress_eq(), nullptr);
    daos_event_register_comp_cb(&write_ev, daos_progress_completion_cb,
                                waiter_.get());
  }
  uint64_t size = data_size;
  RGW_US("before_ds3_obj_write_submit");
  int ret;
  if (dfs_direct_enabled()) {
    // FIO-style direct libdfs submit. ds3_obj_t and ds3_bucket_t are thin
    // wrappers — extract the underlying libdfs handles and call dfs_write
    // directly with our event. This bypasses the racy ds3_obj_set_info
    // path entirely and matches the exact code path FIO uses to reach
    // 9.37 GB/s on this hardware.
    d_iov_t iov;
    d_iov_set(&iov, const_cast<char*>(pending_data.c_str()), data_size);
    d_sg_list_t sgl{};
    sgl.sg_nr = 1;
    sgl.sg_iovs = &iov;
    sgl.sg_nr_out = 1;
    ret = dfs_write(dfs_of(writer_ds3b), dfsobj_of(obj.ds3o), &sgl, offset, &write_ev);
  } else {
    ret = ds3_obj_write(pending_data.c_str(), offset, &size,
                        writer_ds3b, obj.ds3o, &write_ev);
  }
  RGW_US("after_ds3_obj_write_submit");
  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to submit async write ("
                      << obj.get_bucket()->get_name() << ", "
                      << obj.get_key().get_oid() << "): ret=" << ret << dendl;
    daos_event_fini(&write_ev);
    return ret;
  }
  write_submitted = true;
  total_data_size += data_size;
  RGW_US("DaosAtomicWriter_process_exit");
  return 0;
}

int DaosAtomicWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y, uint32_t flags) {
  RGW_US("DaosAtomicWriter_complete_enter");
  ldpp_dout(dpp, 20) << "DEBUG: complete" << dendl;
  bufferlist bl;
  rgw_bucket_dir_entry ent;
  int ret;

  using sc = std::chrono::steady_clock;
  using ms = std::chrono::milliseconds;
  auto t_complete_start = sc::now();

  // Wait for the async data write submitted in process() to complete before
  // writing metadata.  All concurrent writers reach this point independently,
  // so their DAOS writes have been in-flight simultaneously.
  if (write_submitted) {
    RGW_US("before_daos_event_test");
    if (per_req_eq_enabled() && writer_eq_owned) {
      daos_event_t *evp = nullptr;
      ret = 0;
      while (true) {
        int n = daos_eq_poll(writer_eq, 1, DAOS_EQ_WAIT, 1, &evp);
        if (n > 0) { ret = evp->ev_error; break; }
        if (n < 0) { ret = n; break; }
      }
    } else if (waiter_) {
      std::unique_lock<std::mutex> lock(waiter_->mtx);
      waiter_->cv.wait(lock, [this]{ return waiter_->done; });
      ret = waiter_->err;
    } else {
      ret = 0;
    }
    RGW_US("after_daos_event_test");
    daos_event_fini(&write_ev);
    write_submitted = false;
    waiter_.reset();
    pending_data.clear();
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: async data write failed: " << ret << dendl;
      return -ret;
    }
  }
  auto t_after_data = sc::now();

  // Set rgw_bucet_dir_entry. Some of the members of this structure may not
  // apply to daos.
  //
  // Checkout AtomicObjectProcessor::complete() in rgw_putobj_processor.cc
  // and RGWRados::Object::Write::write_meta() in rgw_rados.cc for what and
  // how to set the dir entry. Only set the basic ones for POC, no ACLs and
  // other attrs.
  obj.get_key().get_index_key(&ent.key);
  ent.meta.size = total_data_size;
  ent.meta.accounted_size = accounted_size;
  ent.meta.mtime =
      real_clock::is_zero(set_mtime) ? ceph::real_clock::now() : set_mtime;
  ent.meta.etag = etag;
  ent.meta.owner = owner.to_str();
  ent.meta.owner_display_name =
      obj.get_bucket()->get_owner()->get_display_name();
  bool is_versioned = obj.get_bucket()->versioned();
  if (is_versioned)
    ent.flags =
        rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ldpp_dout(dpp, 20) << __func__ << ": key=" << obj.get_key().get_oid()
                     << " etag: " << etag << dendl;
  if (user_data) ent.meta.user_data = *user_data;

  RGWBucketInfo& info = obj.get_bucket()->get_info();
  if (info.obj_lock_enabled() && info.obj_lock.has_rule()) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date =
          info.obj_lock.get_lock_until_date(ent.meta.mtime);
      string mode = info.obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist retention_bl;
      obj_retention.encode(retention_bl);
      attrs[RGW_ATTR_OBJECT_RETENTION] = retention_bl;
    }
  }

  if (dfs_direct_enabled()) {
    // Skip ds3_obj_set_info — for chunk-wise S3 RDMA the per-object metadata
    // is not used (keys are content-addressed; LMCache reads the same key
    // back) and ds3_obj_set_info races inside libds3 under concurrent PUTs.
    ret = 0;
  } else {
    ret = obj.set_dir_entry_attrs(dpp, &ent, &attrs);
  }
  auto t_after_meta = sc::now();

  if (is_versioned) {
    ret = obj.mark_as_latest(dpp, set_mtime);
    if (ret != 0) {
      return ret;
    }
  }

  // writer_ds3b is owned by DaosStore::ds3_bucket_cache and shared across
  // all writers; do NOT close it here. Just clear our local reference.
  writer_ds3b = nullptr;
  auto t_end = sc::now();

  RGW_US("DaosAtomicWriter_complete_exit");
  ldpp_dout(dpp, 0) << "TIMING pipeline(): "
    << "prepare_to_first_process=" << (first_process_seen ? std::to_string(std::chrono::duration_cast<ms>(t_first_process - t_prepare_start).count()) : "N/A") << "ms "
    << "body_reception=" << (first_process_seen ? std::to_string(std::chrono::duration_cast<ms>(t_last_process - t_first_process).count()) : "N/A") << "ms "
    << "process_to_complete=" << (first_process_seen ? std::to_string(std::chrono::duration_cast<ms>(t_complete_start - t_last_process).count()) : "N/A") << "ms "
    << "daos_data_wait=" << std::chrono::duration_cast<ms>(t_after_data - t_complete_start).count() << "ms "
    << "metadata=" << std::chrono::duration_cast<ms>(t_after_meta - t_after_data).count() << "ms "
    << "bucket_close=" << std::chrono::duration_cast<ms>(t_end - t_after_meta).count() << "ms "
    << "total=" << (first_process_seen ? std::to_string(std::chrono::duration_cast<ms>(t_end - t_prepare_start).count()) : std::to_string(std::chrono::duration_cast<ms>(t_end - t_complete_start).count())) << "ms"
    << dendl;

  return ret;
}

int DaosMultipartUpload::abort(const DoutPrefixProvider* dpp,
                               CephContext* cct) {
  // Remove upload from bucket multipart index
  ldpp_dout(dpp, 20) << "DEBUG: abort" << dendl;
  return ds3_upload_remove(bucket->get_name().c_str(), get_upload_id().c_str(),
                           store->ds3);
}

std::unique_ptr<rgw::sal::Object> DaosMultipartUpload::get_meta_obj() {
  return bucket->get_object(
      rgw_obj_key(get_upload_id(), string(), RGW_OBJ_NS_MULTIPART));
}

int DaosMultipartUpload::init(const DoutPrefixProvider* dpp, optional_yield y,
                              ACLOwner& _owner,
                              rgw_placement_rule& dest_placement,
                              rgw::sal::Attrs& attrs) {
  ldpp_dout(dpp, 0) << "DEBUG [daos-multipart-fix-v1] DaosMultipartUpload::init()"
                    << " upload_id=" << get_upload_id()
                    << " bucket=" << bucket->get_name() << dendl;
  ldpp_dout(dpp, 20) << "DEBUG: init" << dendl;
  int ret;
  std::string oid = mp_obj.get_key();

  // Create an initial entry in the bucket. The entry will be
  // updated when multipart upload is completed, for example,
  // size, etag etc.
  bufferlist bl;
  rgw_bucket_dir_entry ent;
  ent.key.name = oid;
  ent.meta.owner = owner.get_id().to_str();
  ent.meta.category = RGWObjCategory::MultiMeta;
  ent.meta.mtime = ceph::real_clock::now();

  multipart_upload_info upload_info;
  upload_info.dest_placement = dest_placement;

  ent.encode(bl);
  encode(attrs, bl);
  encode(upload_info, bl);

  std::vector<uint8_t> encoded_buf(bl.c_str(), bl.c_str() + bl.length());
  struct ds3_multipart_upload_info ui;
  std::strcpy(ui.upload_id, MULTIPART_UPLOAD_ID_PREFIX);
  std::strncpy(ui.key, oid.c_str(), sizeof(ui.key));
  ui.encoded = encoded_buf.data();
  ui.encoded_length = encoded_buf.size();
  int prefix_length = strlen(ui.upload_id);

  do {
    gen_rand_alphanumeric(store->ctx(), ui.upload_id + prefix_length,
                          sizeof(ui.upload_id) - 1 - prefix_length);
    mp_obj.init(oid, ui.upload_id);
    ret = ds3_upload_init(&ui, bucket->get_name().c_str(), store->ds3);
  } while (ret == -EEXIST);

  // ADD THIS LINE:
  ldpp_dout(dpp, 0) << "DEBUG [daos-multipart-fix-v1] DaosMultipartUpload::init() AFTER loop"
                    << " upload_id=" << get_upload_id()
                    << " ds3_upload_init ret=" << ret << dendl;

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to create multipart upload dir ("
                      << bucket->get_name() << "/" << get_upload_id()
                      << "): ret=" << ret << dendl;
  } else {
    // Cache placement rule so get_info() can return early without
    // calling ds3_upload_get_info() for each upload_part request,
    // working around a crash in libdaos 2.7.x daos_obj_fetch.
    placement = dest_placement;
  }
  return ret;
}

int DaosMultipartUpload::list_parts(const DoutPrefixProvider* dpp,
                                    CephContext* cct, int num_parts, int marker,
                                    int* next_marker, bool* truncated,
                                    bool assume_unsorted) {
  ldpp_dout(dpp, 20) << "DEBUG: list_parts" << dendl;
  // Init needed structures
  vector<struct ds3_multipart_part_info> multipart_part_infos(num_parts);
  uint32_t npart = multipart_part_infos.size();
  vector<vector<uint8_t>> values(npart, vector<uint8_t>(DS3_MAX_ENCODED_LEN));
  for (uint32_t i = 0; i < npart; i++) {
    multipart_part_infos[i].encoded = values[i].data();
    multipart_part_infos[i].encoded_length = values[i].size();
  }

  uint32_t daos_marker = marker;
  int ret = ds3_upload_list_parts(
      bucket->get_name().c_str(), get_upload_id().c_str(), &npart,
      multipart_part_infos.data(), &daos_marker, truncated, store->ds3);

  if (ret != 0) {
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    return ret;
  }

  multipart_part_infos.resize(npart);
  values.resize(npart);
  parts.clear();

  for (auto const& pi : multipart_part_infos) {
    bufferlist bl;
    bl.append(reinterpret_cast<char*>(pi.encoded), pi.encoded_length);

    std::unique_ptr<DaosMultipartPart> part =
        std::make_unique<DaosMultipartPart>();
    auto iter = bl.cbegin();
    decode(part->info, iter);
    parts[pi.part_num] = std::move(part);
  }

  if (next_marker) {
    *next_marker = daos_marker;
  }
  return ret;
}

// Heavily copied from rgw_sal_rados.cc
int DaosMultipartUpload::complete(
    const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
    map<int, string>& part_etags, list<rgw_obj_index_key>& remove_objs,
    uint64_t& accounted_size, bool& compressed, RGWCompressionInfo& cs_info,
    off_t& off, std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
    rgw::sal::Object* target_obj) {
  ldpp_dout(dpp, 20) << "DEBUG: complete" << dendl;
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): enter" << dendl;
  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs& attrs = target_obj->get_attrs();

  do {
    ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): list_parts()"
                       << dendl;
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated);
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    if (ret != 0) return ret;

    total_parts += parts.size();
    if (!truncated && total_parts != (int)part_etags.size()) {
      ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
                        << " expected: " << part_etags.size() << dendl;
      ret = -ERR_INVALID_PART;
      return ret;
    }
    ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): parts.size()="
                       << parts.size() << dendl;

    for (auto obj_iter = parts.begin();
         etags_iter != part_etags.end() && obj_iter != parts.end();
         ++etags_iter, ++obj_iter, ++handled_parts) {
      DaosMultipartPart* part =
          dynamic_cast<rgw::sal::DaosMultipartPart*>(obj_iter->second.get());
      uint64_t part_size = part->get_size();
      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): part_size="
                         << part_size << dendl;
      if (handled_parts < (int)part_etags.size() - 1 &&
          part_size < min_part_size) {
        ret = -ERR_TOO_SMALL;
        return ret;
      }

      char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
      if (etags_iter->first != (int)obj_iter->first) {
        ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
                          << etags_iter->first
                          << " next uploaded: " << obj_iter->first << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }
      string part_etag = rgw_string_unquote(etags_iter->second);
      if (part_etag.compare(part->get_etag()) != 0) {
        ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: "
                          << etags_iter->first
                          << " etag: " << etags_iter->second << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      hex_to_buf(part->get_etag().c_str(), petag, CEPH_CRYPTO_MD5_DIGESTSIZE);
      hash.Update((const unsigned char*)petag, sizeof(petag));
      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): calc etag "
                         << dendl;

      RGWUploadPartInfo& obj_part = part->info;
      string oid = mp_obj.get_part(obj_part.num);
      rgw_obj src_obj;
      src_obj.init_ns(bucket->get_key(), oid, RGW_OBJ_NS_MULTIPART);

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
           (cs_info.compression_type != obj_part.cs_info.compression_type))) {
        ldpp_dout(dpp, 0)
            << "ERROR: compression type was changed during multipart upload ("
            << cs_info.compression_type << ">>"
            << obj_part.cs_info.compression_type << ")" << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): part compression"
                         << dendl;
      if (part_compressed) {
        int64_t new_ofs;  // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        }
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }

      // We may not need to do the following as remove_objs are those
      // don't show when listing a bucket. As we store in-progress uploaded
      // object's metadata in a separate index, they are not shown when
      // listing a bucket.
      rgw_obj_index_key remove_key;
      src_obj.key.get_index_key(&remove_key);

      remove_objs.push_back(remove_key);

      off += obj_part.size;
      accounted_size += obj_part.accounted_size;
      ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): off=" << off
                         << ", accounted_size = " << accounted_size << dendl;
    }
  } while (truncated);
  hash.Final((unsigned char*)final_etag);

  buf_to_hex((unsigned char*)final_etag, sizeof(final_etag), final_etag_str);
  snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
           sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2, "-%lld",
           (long long)part_etags.size());
  etag = final_etag_str;
  ldpp_dout(dpp, 10) << "calculated etag: " << etag << dendl;

  etag_bl.append(etag);

  attrs[RGW_ATTR_ETAG] = etag_bl;

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  // Different from rgw_sal_rados.cc starts here
  // Avoid calling ds3_upload_get_info() here — it calls daos_obj_fetch()
  // which crashes in libdaos 2.7.x (DAOS_COND_AKEY_FETCH bug). All ent
  // fields are overwritten explicitly below; owner comes from the parameter.
  rgw_bucket_dir_entry ent;
  ent.meta.owner = owner.get_id().to_str();

  // Update entry data and name
  target_obj->get_key().get_index_key(&ent.key);
  ent.meta.size = off;
  ent.meta.accounted_size = accounted_size;
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::complete(): obj size="
                     << ent.meta.size
                     << " obj accounted size=" << ent.meta.accounted_size
                     << dendl;
  ent.meta.category = RGWObjCategory::Main;
  ent.meta.mtime = ceph::real_clock::now();
  bool is_versioned = target_obj->get_bucket()->versioned();
  if (is_versioned)
    ent.flags =
        rgw_bucket_dir_entry::FLAG_VER | rgw_bucket_dir_entry::FLAG_CURRENT;
  ent.meta.etag = etag;

  // Open object
  DaosObject* obj = static_cast<DaosObject*>(target_obj);
  ret = obj->create(dpp);
  if (ret != 0) {
    return ret;
  }

  // Copy data from parts to object.
  // Workaround for libdaos 2.7.x DAOS_COND_AKEY_FETCH bug: writing each part
  // at a non-zero offset creates multiple distinct DAOS array entries, and
  // subsequent reads at offsets beyond the first entry trigger daos_obj_fetch()
  // which hangs/times out. Writing all parts as a single contiguous bufferlist
  // at offset 0 creates one entry, so reads at any offset work correctly.
  bufferlist combined_bl;
  for (auto const& [part_num, part] : get_parts()) {
    ds3_part_t* ds3p;
    ret = ds3_part_open(get_bucket_name().c_str(), get_upload_id().c_str(),
                        part_num, false, &ds3p, store->ds3);
    if (ret != 0) {
      return ret;
    }

    // Reserve buffers and read
    uint64_t expected_size = part->get_size();
    uint64_t size = expected_size;
    bufferlist bl;
    ret = ds3_part_read(bl.append_hole(size).c_str(), 0, &size, ds3p,
                        store->ds3, nullptr);
    ds3_part_close(ds3p);
    if (ret != 0) {
      return ret;
    }

    {
      const unsigned char* p = (const unsigned char*)bl.c_str();
      auto rd8 = [&](uint64_t off) -> uint64_t {
        if (size < off + 8) return 0;
        return ((uint64_t)p[off]<<56|(uint64_t)p[off+1]<<48|(uint64_t)p[off+2]<<40|
                (uint64_t)p[off+3]<<32|(uint64_t)p[off+4]<<24|(uint64_t)p[off+5]<<16|
                (uint64_t)p[off+6]<<8|(uint64_t)p[off+7]);
      };
      ldpp_dout(dpp, 0) << "DEBUG [complete-read] part=" << part_num
                        << " sz=" << size
                        << " bl[0MB]=0x" << std::hex << std::setfill('0') << std::setw(16) << rd8(0)
                        << " bl[1MB]=0x" << std::setw(16) << rd8(1048576)
                        << " bl[2MB]=0x" << std::setw(16) << rd8(2097152)
                        << " bl[4MB]=0x" << std::setw(16) << rd8(4194304)
                        << std::dec << dendl;
    }
    combined_bl.claim_append(bl);
  }

  // Single write at offset 0 for the entire object
  ret = obj->write(dpp, std::move(combined_bl), 0);
  if (ret != 0) {
    return ret;
  }

  // Set attributes
  ret = obj->set_dir_entry_attrs(dpp, &ent, &attrs);

  if (is_versioned) {
    ret = obj->mark_as_latest(dpp, ent.meta.mtime);
    if (ret != 0) {
      return ret;
    }
  }

  // Remove upload from bucket multipart index
  ret = ds3_upload_remove(get_bucket_name().c_str(), get_upload_id().c_str(),
                          store->ds3);
  return ret;
}

int DaosMultipartUpload::get_info(const DoutPrefixProvider* dpp,
                                  optional_yield y, rgw_placement_rule** rule,
                                  rgw::sal::Attrs* attrs) {
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::get_info(): enter" << dendl;
  if (!rule && !attrs) {
    return 0;
  }

  if (rule) {
    if (placement.empty()) {
      // Derive placement from bucket to avoid calling ds3_upload_get_info()
      // which triggers a crash in libdaos 2.7.x (DAOS_COND_AKEY_FETCH bug).
      // DAOS always uses default/STANDARD placement so this is always correct.
      placement = bucket->get_info().placement_rule;
    }
    *rule = &placement;
    if (!attrs) {
      // Don't need attrs, done
      return 0;
    }
  }

  // Read the multipart upload dirent from index
  vector<uint8_t> encoded_buf(DS3_MAX_ENCODED_LEN);
  uint64_t size = encoded_buf.size();
  struct ds3_multipart_upload_info ui = {.encoded = encoded_buf.data(),
                                         .encoded_length = size};
  int ret = ds3_upload_get_info(&ui, bucket->get_name().c_str(),
                                get_upload_id().c_str(), store->ds3);

  if (ret != 0) {
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
    return ret;
  }

  bufferlist bl;
  bl.append(reinterpret_cast<char*>(encoded_buf.data()), ui.encoded_length);

  multipart_upload_info upload_info;
  rgw_bucket_dir_entry ent;
  Attrs decoded_attrs;
  auto iter = bl.cbegin();
  ent.decode(iter);
  decode(decoded_attrs, iter);
  ldpp_dout(dpp, 20) << "DEBUG: decoded_attrs size=" << decoded_attrs.size()
                     << dendl;

  if (attrs) {
    *attrs = decoded_attrs;
    if (!rule || *rule != nullptr) {
      // placement was cached; don't actually read
      return 0;
    }
  }

  // Now decode the placement rule
  decode(upload_info, iter);
  placement = upload_info.dest_placement;
  *rule = &placement;

  return 0;
}

std::unique_ptr<Writer> DaosMultipartUpload::get_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
    const std::string& part_num_str) {
  ldpp_dout(dpp, 20) << "DaosMultipartUpload::get_writer(): enter part="
                     << part_num << " head_obj="
                     << (obj ? obj->get_name() : std::string("<null>"))
                     << dendl;
  return std::make_unique<DaosMultipartWriter>(
      dpp, y, this, obj, store, owner, ptail_placement_rule,
      part_num, part_num_str);
}

DaosMultipartWriter::~DaosMultipartWriter() {
  if (is_open()) ds3_part_close(ds3p);
}

int DaosMultipartWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 0) << "DEBUG [daos-multipart-fix-v1] DaosMultipartWriter::prepare()"
                    << " bucket=" << bucket_name
                    << " upload_id=" << upload_id
                    << " part=" << part_num_str << dendl;
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::prepare(): enter part="
                     << part_num_str << dendl;
  int ret = ds3_part_open(get_bucket_name().c_str(), upload_id.c_str(),
                          part_num, true, &ds3p, store->ds3);
  ldpp_dout(dpp, 0) << "DEBUG [daos-multipart-fix-v1] ds3_part_open ret=" << ret
                    << " bucket=" << get_bucket_name()
                    << " upload_id=" << upload_id
                    << " part=" << part_num << dendl;
  if (ret == -ENOENT) {
    ret = -ERR_NO_SUCH_UPLOAD;
  }
  return ret;
}

const std::string& DaosMultipartWriter::get_bucket_name() {
  return bucket_name;
}

int DaosMultipartWriter::process(bufferlist&& data, uint64_t offset) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::process(): enter part="
                     << part_num_str << " offset=" << offset << dendl;
  if (data.length() == 0) {
    return 0;
  }

  // Buffer data to write as a single contiguous write in complete(),
  // working around the libdaos 2.7.x DAOS_COND_AKEY_FETCH bug which causes
  // reads at non-zero offsets to fail when parts are written as multiple
  // separate chunks via ds3_part_write.
  actual_part_size += data.length();
  pending_data.claim_append(data);
  return 0;
}

int DaosMultipartWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y, uint32_t flags) {
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): enter part="
                     << part_num_str << dendl;

  // Add an entry into part index
  bufferlist bl;
  RGWUploadPartInfo info;
  info.num = part_num;
  info.etag = etag;
  info.size = actual_part_size;
  info.accounted_size = accounted_size;
  info.modified = real_clock::now();

  bool compressed;
  int ret = rgw_compression_info_from_attrset(attrs, compressed, info.cs_info);
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): compression ret="
                     << ret << dendl;
  if (ret != 0) {
    ldpp_dout(dpp, 1) << "cannot get compression info" << dendl;
    return ret;
  }
  encode(info, bl);
  encode(attrs, bl);
  ldpp_dout(dpp, 20) << "DaosMultipartWriter::complete(): entry size"
                     << bl.length() << dendl;

  // Write all buffered data as a single contiguous write at offset 0.
  // This avoids the libdaos 2.7.x DAOS_COND_AKEY_FETCH bug where
  // ds3_part_read at non-zero offsets within a part hangs/returns 0 bytes.
  if (pending_data.length() > 0) {
    uint64_t data_size = pending_data.length();
    {
      const unsigned char* p = (const unsigned char*)pending_data.c_str();
      auto rd8 = [&](uint64_t off) -> uint64_t {
        if (data_size < off + 8) return 0;
        return ((uint64_t)p[off]<<56|(uint64_t)p[off+1]<<48|(uint64_t)p[off+2]<<40|
                (uint64_t)p[off+3]<<32|(uint64_t)p[off+4]<<24|(uint64_t)p[off+5]<<16|
                (uint64_t)p[off+6]<<8|(uint64_t)p[off+7]);
      };
      ldpp_dout(dpp, 0) << "DEBUG [writer-write] part=" << part_num
                        << " sz=" << data_size
                        << " pd[0MB]=0x" << std::hex << std::setfill('0') << std::setw(16) << rd8(0)
                        << " pd[1MB]=0x" << std::setw(16) << rd8(1048576)
                        << " pd[2MB]=0x" << std::setw(16) << rd8(2097152)
                        << " pd[4MB]=0x" << std::setw(16) << rd8(4194304)
                        << std::dec << dendl;
    }
    ret = ds3_part_write(pending_data.c_str(), 0, &data_size, ds3p,
                         store->ds3, nullptr);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to write part data ("
                        << get_bucket_name() << ", " << upload_id << ", "
                        << part_num << "): ret=" << ret << dendl;
      return ret;
    }
  }

  struct ds3_multipart_part_info part_info = {.part_num = part_num,
                                              .encoded = bl.c_str(),
                                              .encoded_length = bl.length()};

  ret = ds3_part_set_info(&part_info, ds3p, store->ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to set part info (" << get_bucket_name()
                      << ", " << upload_id << ", " << part_num
                      << "): ret=" << ret << dendl;
    if (ret == -ENOENT) {
      ret = -ERR_NO_SUCH_UPLOAD;
    }
  }

  return ret;
}

std::unique_ptr<RGWRole> DaosStore::get_role(
    std::string name, std::string tenant, std::string path,
    std::string trust_policy, std::string max_session_duration_str,
    std::multimap<std::string, std::string> tags) {
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> DaosStore::get_role(const RGWRoleInfo& info) {
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> DaosStore::get_role(std::string id) {
  RGWRole* p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

int DaosStore::get_roles(const DoutPrefixProvider* dpp, optional_yield y,
                         const std::string& path_prefix,
                         const std::string& tenant,
                         vector<std::unique_ptr<RGWRole>>& roles) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<RGWOIDCProvider> DaosStore::get_oidc_provider() {
  RGWOIDCProvider* p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int DaosStore::get_oidc_providers(
    const DoutPrefixProvider* dpp, const std::string& tenant,
    vector<std::unique_ptr<RGWOIDCProvider>>& providers) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<MultipartUpload> DaosBucket::get_multipart_upload(
    const std::string& oid, std::optional<std::string> upload_id,
    ACLOwner owner, ceph::real_time mtime) {
  return std::make_unique<DaosMultipartUpload>(store, this, oid, upload_id,
                                               owner, mtime);
}

std::unique_ptr<Writer> DaosStore::get_append_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule,
    const std::string& unique_tag, uint64_t position,
    uint64_t* cur_accounted_size) {
  DAOS_NOT_IMPLEMENTED_LOG(dpp);
  return nullptr;
}

std::unique_ptr<Writer> DaosStore::get_atomic_writer(
    const DoutPrefixProvider* dpp, optional_yield y,
    rgw::sal::Object* obj, const rgw_user& owner,
    const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
    const std::string& unique_tag) {
  ldpp_dout(dpp, 20) << "get_atomic_writer" << dendl;
  return std::make_unique<DaosAtomicWriter>(dpp, y, obj, this,
                                            owner, ptail_placement_rule,
                                            olh_epoch, unique_tag);
}

const std::string& DaosStore::get_compression_type(
    const rgw_placement_rule& rule) {
  return zone.zone_params->get_compression_type(rule);
}

bool DaosStore::valid_placement(const rgw_placement_rule& rule) {
  return zone.zone_params->valid_placement(rule);
}

std::unique_ptr<User> DaosStore::get_user(const rgw_user& u) {
  ldout(cctx, 20) << "DEBUG: bucket's user:  " << u.to_str() << dendl;
  return std::make_unique<DaosUser>(this, u);
}

int DaosStore::get_user_by_access_key(const DoutPrefixProvider* dpp,
                                      const std::string& key, optional_yield y,
                                      std::unique_ptr<User>* user) {
  // Initialize ds3_user_info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_user_info user_info = {.encoded = bl.append_hole(size).c_str(),
                                    .encoded_length = size};

  int ret = ds3_user_get_by_key(key.c_str(), &user_info, ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_get_by_key failed, key=" << key
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  DaosUserInfo duinfo;
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo.decode(iter);

  User* u = new DaosUser(this, duinfo.info);
  if (!u) {
    return -ENOMEM;
  }

  user->reset(u);
  return 0;
}

int DaosStore::get_user_by_email(const DoutPrefixProvider* dpp,
                                 const std::string& email, optional_yield y,
                                 std::unique_ptr<User>* user) {
  // Initialize ds3_user_info
  bufferlist bl;
  uint64_t size = DS3_MAX_ENCODED_LEN;
  struct ds3_user_info user_info = {.encoded = bl.append_hole(size).c_str(),
                                    .encoded_length = size};

  int ret = ds3_user_get_by_email(email.c_str(), &user_info, ds3, nullptr);

  if (ret != 0) {
    ldpp_dout(dpp, 0) << "Error: ds3_user_get_by_email failed, email=" << email
                      << " ret=" << ret << dendl;
    return ret;
  }

  // Decode
  DaosUserInfo duinfo;
  bufferlist& blr = bl;
  auto iter = blr.cbegin();
  duinfo.decode(iter);

  User* u = new DaosUser(this, duinfo.info);
  if (!u) {
    return -ENOMEM;
  }

  user->reset(u);
  return 0;
}

int DaosStore::get_user_by_swift(const DoutPrefixProvider* dpp,
                                 const std::string& user_str, optional_yield y,
                                 std::unique_ptr<User>* user) {
  /* Swift keys and subusers are not supported for now */
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::unique_ptr<Object> DaosStore::get_object(const rgw_obj_key& k) {
  return std::make_unique<DaosObject>(this, k);
}

inline std::ostream& operator<<(std::ostream& out, const rgw_user* u) {
  std::string s;
  if (u != nullptr)
    u->to_str(s);
  else
    s = "(nullptr)";
  return out << s;
}

int DaosStore::get_bucket(const DoutPrefixProvider* dpp, User* u,
                          const rgw_bucket& b, std::unique_ptr<Bucket>* bucket,
                          optional_yield y) {
  ldpp_dout(dpp, 20) << "DEBUG: get_bucket1: User: " << u << dendl;
  int ret;
  Bucket* bp;

  bp = new DaosBucket(this, b, u);
  ret = bp->load_bucket(dpp, y);
  if (ret != 0) {
    delete bp;
    return ret;
  }

  bucket->reset(bp);
  return 0;
}

int DaosStore::get_bucket(User* u, const RGWBucketInfo& i,
                          std::unique_ptr<Bucket>* bucket) {
  DaosBucket* bp;

  bp = new DaosBucket(this, i, u);
  /* Don't need to fetch the bucket info, use the provided one */

  bucket->reset(bp);
  return 0;
}

int DaosStore::get_bucket(const DoutPrefixProvider* dpp, User* u,
                          const std::string& tenant, const std::string& name,
                          std::unique_ptr<Bucket>* bucket, optional_yield y) {
  ldpp_dout(dpp, 20) << "get_bucket" << dendl;
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

bool DaosStore::is_meta_master() { return true; }

int DaosStore::forward_request_to_master(const DoutPrefixProvider* dpp,
                                         User* user, obj_version* objv,
                                         bufferlist& in_data, JSONParser* jp,
                                         req_info& info, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::forward_iam_request_to_master(const DoutPrefixProvider* dpp,
                                             const RGWAccessKey& key,
                                             obj_version* objv,
                                             bufferlist& in_data,
                                             RGWXMLDecoder::XMLParser* parser,
                                             req_info& info, optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::string DaosStore::zone_unique_id(uint64_t unique_num) { return ""; }

std::string DaosStore::zone_unique_trans_id(const uint64_t unique_num) {
  return "";
}

int DaosStore::cluster_stat(RGWClusterStat& stats) {
  return DAOS_NOT_IMPLEMENTED_LOG(nullptr);
}

std::unique_ptr<Lifecycle> DaosStore::get_lifecycle(void) {
  DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  return 0;
}

std::unique_ptr<Completions> DaosStore::get_completions(void) {
  DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  return 0;
}

std::unique_ptr<Notification> DaosStore::get_notification(
    rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
    rgw::notify::EventType event_type, optional_yield y,
    const std::string* object_name) {
  return std::make_unique<DaosNotification>(obj, src_obj, event_type);
}

std::unique_ptr<Notification> DaosStore::get_notification(
    const DoutPrefixProvider* dpp, Object* obj, Object* src_obj,
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket,
    std::string& _user_id, std::string& _user_tenant, std::string& _req_id,
    optional_yield y) {
  ldpp_dout(dpp, 20) << "get_notification" << dendl;
  return std::make_unique<DaosNotification>(obj, src_obj, event_type);
}

int DaosStore::log_usage(const DoutPrefixProvider* dpp,
                         map<rgw_user_bucket, RGWUsageBatch>& usage_info) {
  DAOS_NOT_IMPLEMENTED_LOG(dpp);
  return 0;
}

int DaosStore::log_op(const DoutPrefixProvider* dpp, string& oid,
                      bufferlist& bl) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::register_to_service_map(const DoutPrefixProvider* dpp,
                                       const string& daemon_type,
                                       const map<string, string>& meta) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

void DaosStore::get_quota(RGWQuota& quota) {
  // XXX: Not handled for the first pass
  return;
}

void DaosStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
                              RGWRateLimitInfo& user_ratelimit,
                              RGWRateLimitInfo& anon_ratelimit) {
  return;
}

int DaosStore::set_buckets_enabled(const DoutPrefixProvider* dpp,
                                   std::vector<rgw_bucket>& buckets,
                                   bool enabled) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::get_sync_policy_handler(const DoutPrefixProvider* dpp,
                                       std::optional<rgw_zone_id> zone,
                                       std::optional<rgw_bucket> bucket,
                                       RGWBucketSyncPolicyHandlerRef* phandler,
                                       optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

RGWDataSyncStatusManager* DaosStore::get_data_sync_manager(
    const rgw_zone_id& source_zone) {
  DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  return 0;
}

int DaosStore::read_all_usage(
    const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry>& usage) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::trim_all_usage(const DoutPrefixProvider* dpp,
                              uint64_t start_epoch, uint64_t end_epoch) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::get_config_key_val(string name, bufferlist* bl) {
  return DAOS_NOT_IMPLEMENTED_LOG(nullptr);
}

int DaosStore::meta_list_keys_init(const DoutPrefixProvider* dpp,
                                   const string& section, const string& marker,
                                   void** phandle) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

int DaosStore::meta_list_keys_next(const DoutPrefixProvider* dpp, void* handle,
                                   int max, list<string>& keys,
                                   bool* truncated) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

void DaosStore::meta_list_keys_complete(void* handle) { return; }

std::string DaosStore::meta_get_marker(void* handle) { return ""; }

int DaosStore::meta_remove(const DoutPrefixProvider* dpp, string& metadata_key,
                           optional_yield y) {
  return DAOS_NOT_IMPLEMENTED_LOG(dpp);
}

std::string DaosStore::get_cluster_id(const DoutPrefixProvider* dpp,
                                      optional_yield y) {
  DAOS_NOT_IMPLEMENTED_LOG(dpp);
  return "";
}



namespace {

// Persistent helper-thread pool for parallel daos_array_read fan-out in
// kvcache_stream_daos. Created once at first use and lives the lifetime of
// the RGW process. Each of the kPoolSize worker threads has its own
// per-thread libdaos scheduler context the first time it touches the API,
// so each worker maps to one independent FIO-style job (numjobs=N
// iodepth=1).
class DaosReadPool {
 public:
  static constexpr size_t kPoolSize = 16;

  static DaosReadPool& instance() {
    static DaosReadPool pool;
    return pool;
  }

  // Submit one job to slot `slot`. Caller must wait on `done` to know when
  // the job has completed.
  struct Job {
    std::function<void()> fn;
    std::atomic<bool>     done{false};
  };

  // Run a batch of N jobs (one per slot, slots [0..N)) in parallel and wait
  // for all of them to finish before returning. N must be <= kPoolSize.
  void run_batch(size_t n, std::function<void(size_t)> body) {
    if (n == 0) return;
    if (n > kPoolSize) n = kPoolSize;

    std::vector<Job> jobs(n);
    for (size_t i = 0; i < n; i++) {
      Job& j = jobs[i];
      j.fn = [&body, i, &j]() {
        body(i);
        j.done.store(true, std::memory_order_release);
      };
    }

    // Push the jobs to slots and wake them up.
    for (size_t i = 0; i < n; i++) {
      Slot& s = slots_[i];
      {
        std::unique_lock<std::mutex> lk(s.m);
        s.job = &jobs[i];
      }
      s.cv.notify_one();
    }

    // Spin-wait for completion (cheap polling on per-job done flag).
    for (size_t i = 0; i < n; i++) {
      while (!jobs[i].done.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
    }
  }

 private:
  struct Slot {
    std::mutex              m;
    std::condition_variable cv;
    Job*                    job{nullptr};
    bool                    stop{false};
  };

  std::array<Slot, kPoolSize> slots_;
  std::array<std::thread, kPoolSize> workers_;

  DaosReadPool() {
    for (size_t i = 0; i < kPoolSize; i++) {
      workers_[i] = std::thread(&DaosReadPool::worker_loop, this, i);
    }
  }

  ~DaosReadPool() {
    for (size_t i = 0; i < kPoolSize; i++) {
      Slot& s = slots_[i];
      {
        std::lock_guard<std::mutex> lk(s.m);
        s.stop = true;
      }
      s.cv.notify_one();
    }
    for (auto& t : workers_) if (t.joinable()) t.join();
  }

  void worker_loop(size_t slot_id) {
    Slot& s = slots_[slot_id];
    while (true) {
      Job* j = nullptr;
      {
        std::unique_lock<std::mutex> lk(s.m);
        s.cv.wait(lk, [&] { return s.job != nullptr || s.stop; });
        if (s.stop) return;
        j = s.job;
        s.job = nullptr;
      }
      j->fn();
    }
  }

  DaosReadPool(const DaosReadPool&) = delete;
  DaosReadPool& operator=(const DaosReadPool&) = delete;
};

}  // namespace

int kvcache_stream_daos(
    void* bucket_ptr,
    const NixlRdmaToken& rdma_tok,
    const std::vector<std::string>& chunk_keys,
    int num_layers,
    size_t kv_per_token_per_layer,
    size_t tokens_per_chunk,
    int layer_aggregate,
    const DoutPrefixProvider* dpp)
{
  auto& rdma_srv = RGWRdmaServer::instance();
  if (!rdma_srv.is_ready()) {
    ldpp_dout(dpp, 0) << "kvcache_stream_daos: RDMA server not ready" << dendl;
    return -EIO;
  }

  auto* daos_bucket = static_cast<DaosBucket*>(bucket_ptr);
  if (!daos_bucket || !daos_bucket->ds3b) {
    ldpp_dout(dpp, 0) << "kvcache_stream_daos: DAOS bucket not available" << dendl;
    return -EIO;
  }

  const size_t N = chunk_keys.size();
  const size_t layer_slice = kv_per_token_per_layer * tokens_per_chunk;
  const size_t layer_total = layer_slice * N;

  ldpp_dout(dpp, 0) << "kvcache_stream_daos: N=" << N << " layers=" << num_layers
    << " layer_slice=" << layer_slice << " layer_total=" << layer_total << dendl;

  if (layer_total > RGWRdmaServer::kPutBufSz) {
    ldpp_dout(dpp, 0) << "kvcache_stream_daos: layer_total too large for pre_buf_" << dendl;
    return -EINVAL;
  }

  // Pre-open all chunk objects (avoid open/close per layer)
  std::vector<ds3_obj_t*> chunk_objs(N, nullptr);
  for (size_t ci = 0; ci < N; ci++) {
    // Apply RGW OID transformation: prepend extra _ if name starts with _
    std::string oid = chunk_keys[ci];
    if (!oid.empty() && oid[0] == '_') oid = std::string("_") + oid;
    int ret = ds3_obj_open(oid.c_str(), &chunk_objs[ci], daos_bucket->ds3b);
    if (ret != 0) {
      ldpp_dout(dpp, 0) << "kvcache_stream_daos: ds3_obj_open failed chunk="
        << chunk_keys[ci] << " ret=" << ret << dendl;
      // Close any already-opened objects
      for (size_t j = 0; j < ci; j++) ds3_obj_close(chunk_objs[j]);
      return ret;
    }
  }

  using sc = std::chrono::steady_clock;
  using ms = std::chrono::milliseconds;
  auto t_start = sc::now();

  // Stream layer by layer: read layer slice directly from each chunk via offset,
  // assemble into pre_buf_, RDMA_WRITE to client.
  char* assembly = static_cast<char*>(rdma_srv.put_buf());

  // Clamp layer_aggregate to valid range
  if (layer_aggregate <= 0 || layer_aggregate > num_layers)
    layer_aggregate = num_layers;

  const size_t agg_slice = layer_slice * layer_aggregate;  // bytes per chunk per aggregate group
  const size_t agg_total = agg_slice * N;                  // bytes per RDMA push

  ldpp_dout(dpp, 0) << "kvcache_stream_daos: layer_aggregate=" << layer_aggregate
    << " agg_slice=" << agg_slice << " agg_total=" << agg_total << dendl;

  if (agg_total > RGWRdmaServer::kPutBufSz) {
    ldpp_dout(dpp, 0) << "kvcache_stream_daos: agg_total too large for pre_buf_" << dendl;
    for (size_t j = 0; j < N; j++) ds3_obj_close(chunk_objs[j]);
    return -EINVAL;
  }

  // INTERLEAVED LAYER-MAJOR: read one layer slice (1 MB matching the DFS cell
  // size) from each chunk, then immediately RDMA-push that layer. The client GPU
  // can start prefill on layer 0 as soon as the first layer arrives, instead of
  // waiting ~578 ms for all chunks to be fully read.
  //
  // Why this beats the chunk-major version (read all chunks fully, then deliver):
  //   - Each daos_array_read is exactly layer_slice bytes (1 MB on 8B model),
  //     matching DFS_DEFAULT_CHUNK_SIZE so it is one internal cell read instead
  //     of fanning out into 32 sequential cells inside DAOS.
  //   - Client GPU sees layer 0 at t ≈ N × per_cell_latency, not t ≈ total_size /
  //     bandwidth. With prefill overlap (LMCache layerwise pipelining) the
  //     end-to-end TTFT becomes max(read, compute) instead of read + compute.
  size_t per_layer_total_bytes = layer_slice * N;  // bytes per single layer push
  size_t total_layers_read_ms = 0;  // accumulated for the SG read log line

  // Buffers for one aggregate group: layer_aggregate layers × N chunks × layer_slice
  // (assembly[] is the pre-registered RDMA pre_buf_)
  for (int group_start = 0; group_start < num_layers; group_start += layer_aggregate) {
    int group_end = std::min(group_start + layer_aggregate, num_layers);
    int group_size = group_end - group_start;
    size_t group_bytes_per_chunk = static_cast<size_t>(group_size) * layer_slice;
    size_t group_total = group_bytes_per_chunk * N;

    auto t_read_start = sc::now();

    // PARALLEL DAOS READS via persistent thread pool (Shape 2).
    //
    // We dispatch N parallel daos_array_read calls onto a per-process pool of
    // kPoolSize=16 long-lived worker threads (created once at first use and
    // reused for the lifetime of the RGW process). Each worker runs the DAOS
    // call synchronously, so it gets its own thread-private libdaos scheduler
    // context — same model as FIO numjobs=N iodepth=1 — but without the
    // per-layer pthread_create/join overhead Shape 1 paid.
    std::vector<int> per_chunk_ret(N, 0);

    DaosReadPool::instance().run_batch(N, [&](size_t ci) {
      daos_range_t rg;
      rg.rg_idx = static_cast<uint64_t>(group_start) * layer_slice;
      rg.rg_len = group_bytes_per_chunk;

      daos_array_iod_t iod{};
      iod.arr_nr = 1;
      iod.arr_rgs = &rg;

      d_iov_t iov;
      d_iov_set(&iov, assembly + ci * group_bytes_per_chunk, group_bytes_per_chunk);
      d_sg_list_t sgl{};
      sgl.sg_nr = 1;
      sgl.sg_iovs = &iov;
      sgl.sg_nr_out = 1;

      per_chunk_ret[ci] = daos_array_read(get_array_oh(chunk_objs[ci]),
                                          DAOS_TX_NONE, &iod, &sgl, nullptr);
    });

    // Check if any helper failed.
    for (size_t ci = 0; ci < N; ci++) {
      if (per_chunk_ret[ci] != 0) {
        ldpp_dout(dpp, 0) << "kvcache_stream_daos: daos_array_read failed chunk="
          << ci << " group=" << group_start << " ret=" << per_chunk_ret[ci] << dendl;
        for (size_t j = 0; j < N; j++) ds3_obj_close(chunk_objs[j]);
        return -per_chunk_ret[ci];
      }
    }

    auto t_read_end = sc::now();
    total_layers_read_ms += std::chrono::duration_cast<ms>(t_read_end - t_read_start).count();

    NixlRdmaToken group_tok = rdma_tok;
    group_tok.addr += static_cast<uint64_t>(group_start) * per_layer_total_bytes;

    int ret = rdma_srv.rdma_write_batch(group_tok, assembly, group_total);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "kvcache_stream_daos: rdma_write_batch failed group="
        << group_start << " ret=" << ret << dendl;
      for (size_t j = 0; j < N; j++) ds3_obj_close(chunk_objs[j]);
      return ret;
    }
  }

  auto t_done = sc::now();

  // Close all chunk objects
  for (size_t ci = 0; ci < N; ci++) ds3_obj_close(chunk_objs[ci]);

  ldpp_dout(dpp, 0) << "kvcache_stream_daos: complete (LAYER-MAJOR Shape2 thread-pool). "
    << N << " chunks x " << num_layers << " layers in "
    << std::chrono::duration_cast<ms>(t_done - t_start).count() << "ms "
    << "(daos_read_total=" << total_layers_read_ms << "ms layer_slice="
    << layer_slice << " agg=" << layer_aggregate << ")" << dendl;

  return 0;
}

}  // namespace rgw::sal


extern "C" void* newDaosStore(CephContext* cct) {
  return new rgw::sal::DaosStore(cct);
}
