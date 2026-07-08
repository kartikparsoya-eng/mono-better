// goivm_napi: N-API addon bridging the Go IVM engine (libgoivm, a Go
// c-shared library) into the Node process — replacing the Unix-socket
// transport with direct in-process calls.
//
// Raw N-API (no node-addon-api dependency). Responsibilities:
//   1. dlopen(libgoivm) + resolve the 4 ABI symbols; verify ABI version.
//   2. goivm_start with a C deliver callback that copies each (kind, bytes)
//      delivery and enqueues it onto ONE napi_threadsafe_function — the
//      single ordered queue that preserves the Go side's emit order across
//      frames (kind 1) and row records (kinds 2/3).
//   3. send(buffer): forwards a request frame to goivm_send (which copies
//      and returns immediately — never blocks the JS thread).
//   4. Backpressure: the TSFN queue is bounded; the deliver callback
//      enqueues with napi_tsfn_nonblocking and returns a status (ABI v4:
//      0=queued, 1=queue full, 2=closing). The GO side owns the retry —
//      which is what makes a delivery stalled on a starved JS event loop
//      CANCELLABLE (pull-gate cancel / group teardown / deliver timeout).
//      Pre-v4 this used napi_tsfn_blocking ("backpressure like a slow
//      socket"): a JS loop starved for minutes parked the producing Go
//      goroutine in an uninterruptible call while it held the row plane's
//      mutex — the G13 permanent CG wedge.
//   5. Drain signal (ABI v5): the addon tracks queue occupancy and, after
//      any FULL rejection, calls goivm_queue_drained() (a direct dlsym'd
//      Go export, streamCredit-class: O(1), never blocks) once the queue
//      drains below the low-water mark — the EVENT-DRIVEN wakeup for Go
//      producers parked on a full queue. Replaces the v4 Go-side
//      sleep-poll, whose up-to-5ms dead air per park was the measured
//      latency tax (19,242 parks in one 20-min soak).
//
// Memory: the deliver callback malloc+memcpy's the payload (Go's buffer is
// only valid during the call — cgo pointer rules); the JS-side callback
// wraps it in an external Buffer whose finalizer free()s it. One copy
// Go→JS, zero syscalls, no framing, no reassembly.
//
// External-memory accounting is LOAD-BEARING: the malloc'd payloads live
// outside the V8 heap, and the Buffer handles V8 sees are tiny — without
// napi_adjust_external_memory, GC feels no pressure from gigabytes of
// pending payloads, finalizers run arbitrarily late, and RSS floats up
// under sustained row traffic (ART memory-growth finding: RSS +500MB in
// 147s while both the Go and JS heaps stayed flat). Adjusting ±len on
// create/finalize makes V8 collect promptly under load.

#include <dlfcn.h>
#include <node_api.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// NDEBUG-proof fatal check (REVIEW-napi-transport LOW-1). These guard
// napi calls that fail only under extreme conditions (V8 heap exhaustion,
// torn-down env) — but they are LOAD-BEARING: silently continuing after a
// failed arg construction would invoke the JS callback with garbage, and
// silently DROPPING a delivery corrupts the stream (a missing row/frame
// surfaces later as a hung RPC or drift — far harder to diagnose than a
// crash). assert() is the wrong tool: node-gyp Release configs on some
// toolchains define NDEBUG, compiling the check out entirely. This macro
// survives NDEBUG and dies loudly where the failure happened.
#define GOIVM_FATAL_IF(cond, what)                                        \
  do {                                                                    \
    if (cond) {                                                           \
      fprintf(stderr, "[goivm_napi] FATAL: %s (%s:%d)\n", what, __FILE__, \
              __LINE__);                                                  \
      abort();                                                            \
    }                                                                     \
  } while (0)

// ABI v2: delivery kind 4 (host death) added — the client fatals the worker
// on receipt, so a v1 library that never emits it must not pair with this
// addon (the A3 guarantee would silently vanish).
// ABI v3: goivm_stream_credit / goivm_stream_cancel added (pull-hydration
// demand gate — DESIGN-duplex-streaming). Both are DIRECT JS-thread calls
// into the Go library (leaf-mutex registry lookup; O(1), never blocks,
// never touches N-API on the Go side) — deliberately NOT routed through
// the TSFN: credit grants are upstream control flow, not deliveries.
// ABI v4: the deliver callback returns int32_t status and enqueues with
// napi_tsfn_nonblocking (see deliver_from_go). The version gates the
// SIGNATURE: a v4 library reading a return value from a v3 addon's void
// callback would consume garbage (a phantom "queue full" retries an
// enqueue that SUCCEEDED — duplicate delivery → stream corruption), and a
// v3 library's blocking callback on this addon would reintroduce the wedge.
// ABI v5: adds the goivm_queue_drained export (dlsym'd + REQUIRED below —
// the event-driven wakeup for Go producers parked on a full queue; called
// from call_js_deliver's drain accounting) and delivery kind 5 (record
// batch — framed sub-records staged Go-side under congestion; decoded by
// napi-records.ts iterateBatch). The version gates BOTH: a v5 library on a
// v4 addon would never receive drain signals (producers degrade to
// tick-polling) and — worse — its kind-5 batches would hit a JS side with
// no batch decoder: dropped deliveries → stream corruption.
#define EXPECTED_GOIVM_ABI 5
// Deliver statuses returned to the Go library (must mirror rowplane.go).
#define GOIVM_DELIVER_OK 0
#define GOIVM_DELIVER_FULL 1
#define GOIVM_DELIVER_CLOSED 2
// Default TSFN queue bound. Tunable via GOIVM_NAPI_QUEUE_MAX (env, read at
// start): in row mode a single large advance can fill 8192 by itself (one
// entry per row), putting the producing Go goroutine into its retry loop —
// which is the designed backpressure, but operators may want more headroom
// before it engages. Larger queue = more C-heap payloads pending on the
// queue (accounted to V8 via napi_adjust_external_memory below).
#define TSFN_MAX_QUEUE_DEFAULT 8192

typedef int32_t (*goivm_deliver_cb)(void* ctx, int32_t kind, const void* data,
                                    int32_t len);
typedef int32_t (*goivm_start_fn)(goivm_deliver_cb cb, void* ctx);
typedef int32_t (*goivm_send_fn)(const void* data, int32_t len);
typedef void (*goivm_shutdown_fn)(void);
typedef int32_t (*goivm_abi_version_fn)(void);
typedef void (*goivm_stream_credit_fn)(double req_id, int32_t n);
typedef void (*goivm_stream_cancel_fn)(double req_id);
typedef void (*goivm_queue_drained_fn)(void);

typedef struct {
  void* dl;
  goivm_start_fn start;
  goivm_send_fn send;
  goivm_shutdown_fn shutdown;
  goivm_abi_version_fn abi_version;
  goivm_stream_credit_fn stream_credit;
  goivm_stream_cancel_fn stream_cancel;
  goivm_queue_drained_fn queue_drained;
  napi_threadsafe_function tsfn;
  // low_water = queue_max/2: the drain-signal threshold (ABI v5). After
  // any FULL rejection, the first dequeue that brings occupancy to or
  // below this fires ONE goivm_queue_drained() (latched by
  // g_queue_was_full — one signal per full episode).
  size_t low_water;
  int started;
} bridge_state;

static bridge_state g_bridge = {0};

// Queue-occupancy accounting for the ABI v5 drain signal. g_queue_len is
// an UPPER bound on queued items (incremented BEFORE the enqueue attempt,
// decremented on failure or dequeue — never transiently under-counts, so a
// drain signal is never fired while the queue might still be full).
// Touched from Go threads (deliver_from_go) and the JS thread
// (call_js_deliver); C11 atomics.
static atomic_long g_queue_len;
static atomic_bool g_queue_was_full;

typedef struct {
  int32_t kind;
  void* data;
  size_t len;
} delivery_item;

// Finalizer for the external Buffer handed to JS — frees the malloc'd copy
// once V8 GCs the Buffer, and reverses the external-memory accounting the
// create side registered (hint carries the byte length).
static void free_delivery_buffer(napi_env env, void* data, void* hint) {
  if (env != NULL && hint != NULL) {
    int64_t adjusted;
    napi_adjust_external_memory(env, -(int64_t)(uintptr_t)hint, &adjusted);
  }
  free(data);
}

// Runs on the JS thread for each queued delivery: build (kind, Buffer) and
// invoke the registered JS callback.
static void call_js_deliver(napi_env env, napi_value js_cb, void* ctx,
                            void* data) {
  delivery_item* item = (delivery_item*)data;
  // Drain accounting (ABI v5): every dequeued item decrements — including
  // the teardown path below. After a FULL episode (latch set), the first
  // dequeue at or below the low-water mark fires ONE drain signal to wake
  // the Go producers parked on the full queue. atomic_exchange makes
  // exactly one dequeue win the latch; env==NULL (TSFN teardown) skips the
  // call into a dying bridge — parked producers unwind via deliverClosed
  // on their next attempt.
  long remaining = atomic_fetch_sub(&g_queue_len, 1) - 1;
  if (remaining <= (long)g_bridge.low_water &&
      atomic_load(&g_queue_was_full)) {
    if (atomic_exchange(&g_queue_was_full, false) && env != NULL &&
        g_bridge.queue_drained != NULL) {
      g_bridge.queue_drained();
    }
  }
  if (env == NULL || js_cb == NULL) {
    // TSFN torn down with items still queued — just free.
    if (item != NULL) {
      free(item->data);
      free(item);
    }
    return;
  }
  napi_value kind_val, buf_val, undefined;
  napi_status status = napi_create_int32(env, item->kind, &kind_val);
  GOIVM_FATAL_IF(status != napi_ok, "napi_create_int32(kind) failed");
  if (item->len > 0) {
    // External buffer: zero-copy handoff of the malloc'd bytes; finalizer
    // frees + un-accounts (hint = len). (If the platform/Node build refuses
    // external buffers, fall back to a copy.)
    status = napi_create_external_buffer(env, item->len, item->data,
                                         free_delivery_buffer,
                                         (void*)(uintptr_t)item->len, &buf_val);
    if (status == napi_ok) {
      // Tell V8 how much C heap this Buffer pins so GC pressure scales
      // with the real footprint (see file header).
      int64_t adjusted;
      napi_adjust_external_memory(env, (int64_t)item->len, &adjusted);
    } else {
      status = napi_create_buffer_copy(env, item->len, item->data, NULL, &buf_val);
      GOIVM_FATAL_IF(status != napi_ok, "napi_create_buffer_copy failed");
      free(item->data);
    }
  } else {
    status = napi_create_buffer_copy(env, 0, "", NULL, &buf_val);
    GOIVM_FATAL_IF(status != napi_ok, "napi_create_buffer_copy(empty) failed");
    free(item->data);
  }
  free(item);

  status = napi_get_undefined(env, &undefined);
  GOIVM_FATAL_IF(status != napi_ok, "napi_get_undefined failed");
  napi_value argv[2] = {kind_val, buf_val};
  // Exceptions from the JS callback propagate as uncaught — the JS wrapper
  // (napi-transport.ts) try/catches internally, mirroring #onData's
  // load-bearing try/catch around onPartial.
  napi_call_function(env, undefined, js_cb, 2, argv, NULL);
}

// The C deliver callback Go invokes from its goroutines. Copies the payload
// and enqueues NONBLOCKING (ABI v4); the returned status tells the Go side
// whether the entry was queued (payload copied — caller may reuse its
// buffer), the queue was full (nothing enqueued — Go owns the retry, which
// keeps a stalled delivery cancellable), or the TSFN is closing (transport
// dead).
static int32_t deliver_from_go(void* ctx, int32_t kind, const void* data,
                               int32_t len) {
  (void)ctx;
  if (g_bridge.tsfn == NULL) return GOIVM_DELIVER_CLOSED;  // shutdown race
  // Containment (scale review): no legitimate delivery exceeds 64MB — the
  // Go side caps frames (maxFrameSize, both directions) and row records
  // (rowrecord.go R1) at 64MB. A larger (or negative) len here can only be
  // ABI mismatch or memory corruption; the old path would malloc up to 2GB
  // and abort() the whole worker on OOM. Convert it into a synthetic
  // host-death record (kind 4, same as go-ivm abi.go's death watcher): the
  // JS client sweeps pending RPCs and fatals the worker CLEANLY.
  static const int32_t kMaxDelivery = 64 * 1024 * 1024;
  char death_msg[160];
  if (len > kMaxDelivery || len < 0) {
    snprintf(death_msg, sizeof(death_msg),
             "goivm_napi: oversized delivery (kind=%d len=%d) — ABI mismatch "
             "or memory corruption",
             (int)kind, (int)len);
    kind = 4; /* DELIVERY_KIND_HOST_DEATH */
    data = death_msg;
    len = (int32_t)strlen(death_msg);
  }
  delivery_item* item = (delivery_item*)malloc(sizeof(delivery_item));
  // OOM here must NOT silently drop the delivery: a missing row/frame
  // corrupts the stream (hung RPC / drift), strictly worse than dying
  // under memory exhaustion the process wouldn't survive anyway.
  GOIVM_FATAL_IF(item == NULL, "malloc(delivery_item) failed (OOM)");
  item->kind = kind;
  item->len = (size_t)(len > 0 ? len : 0);
  item->data = malloc(item->len > 0 ? item->len : 1);
  GOIVM_FATAL_IF(item->data == NULL, "malloc(payload) failed (OOM)");
  if (item->len > 0) memcpy(item->data, data, item->len);
  // Count BEFORE the enqueue attempt (upper bound — see g_queue_len) and
  // undo on failure: the drain signal must never fire while the queue
  // might still be full.
  atomic_fetch_add(&g_queue_len, 1);
  napi_status status = napi_call_threadsafe_function(g_bridge.tsfn, item,
                                                     napi_tsfn_nonblocking);
  if (status == napi_ok) return GOIVM_DELIVER_OK;
  // Nothing was enqueued — undo the count and free the copy either way.
  atomic_fetch_sub(&g_queue_len, 1);
  free(item->data);
  free(item);
  if (status == napi_queue_full) {
    // Latch the full episode: the next drain below low-water fires ONE
    // goivm_queue_drained() (ABI v5) to wake the parked producers.
    atomic_store(&g_queue_was_full, true);
    return GOIVM_DELIVER_FULL;
  }
  // napi_closing (shutdown teardown) or any other terminal condition:
  // the transport is dead; the Go side unwinds the stream.
  return GOIVM_DELIVER_CLOSED;
}

static napi_value throw_error(napi_env env, const char* msg) {
  napi_throw_error(env, NULL, msg);
  return NULL;
}

// start(libPath: string, onDelivery: (kind: number, payload: Buffer) => void)
static napi_value Start(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_status status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (status != napi_ok || argc < 2) {
    return throw_error(env, "start(libPath, onDelivery) requires 2 arguments");
  }
  if (g_bridge.started) {
    return throw_error(env, "goivm bridge already started (one per process)");
  }

  char lib_path[4096];
  size_t path_len = 0;
  status = napi_get_value_string_utf8(env, argv[0], lib_path, sizeof(lib_path),
                                      &path_len);
  if (status != napi_ok) return throw_error(env, "libPath must be a string");

  g_bridge.dl = dlopen(lib_path, RTLD_NOW | RTLD_LOCAL);
  if (g_bridge.dl == NULL) {
    char msg[4400];
    snprintf(msg, sizeof(msg), "dlopen(%s) failed: %s", lib_path, dlerror());
    return throw_error(env, msg);
  }
  g_bridge.start = (goivm_start_fn)dlsym(g_bridge.dl, "goivm_start");
  g_bridge.send = (goivm_send_fn)dlsym(g_bridge.dl, "goivm_send");
  g_bridge.shutdown = (goivm_shutdown_fn)dlsym(g_bridge.dl, "goivm_shutdown");
  g_bridge.abi_version =
      (goivm_abi_version_fn)dlsym(g_bridge.dl, "goivm_abi_version");
  g_bridge.stream_credit =
      (goivm_stream_credit_fn)dlsym(g_bridge.dl, "goivm_stream_credit");
  g_bridge.stream_cancel =
      (goivm_stream_cancel_fn)dlsym(g_bridge.dl, "goivm_stream_cancel");
  g_bridge.queue_drained =
      (goivm_queue_drained_fn)dlsym(g_bridge.dl, "goivm_queue_drained");
  if (!g_bridge.start || !g_bridge.send || !g_bridge.shutdown ||
      !g_bridge.abi_version || !g_bridge.stream_credit ||
      !g_bridge.stream_cancel || !g_bridge.queue_drained) {
    dlclose(g_bridge.dl);
    g_bridge.dl = NULL;
    return throw_error(env, "libgoivm missing goivm_* symbols (wrong library?)");
  }
  int32_t abi = g_bridge.abi_version();
  if (abi != EXPECTED_GOIVM_ABI) {
    char msg[128];
    snprintf(msg, sizeof(msg), "goivm ABI mismatch: lib=%d addon=%d", abi,
             EXPECTED_GOIVM_ABI);
    dlclose(g_bridge.dl);
    g_bridge.dl = NULL;
    return throw_error(env, msg);
  }

  napi_value resource_name;
  status = napi_create_string_utf8(env, "goivm_deliver", NAPI_AUTO_LENGTH,
                                   &resource_name);
  if (status != napi_ok) {
    dlclose(g_bridge.dl);
    g_bridge.dl = NULL;
    return throw_error(env, "napi_create_string_utf8 failed");
  }
  size_t queue_max = TSFN_MAX_QUEUE_DEFAULT;
  const char* queue_env = getenv("GOIVM_NAPI_QUEUE_MAX");
  if (queue_env != NULL) {
    long v = atol(queue_env);
    if (v > 0) queue_max = (size_t)v;
  }
  g_bridge.low_water = queue_max / 2;
  status = napi_create_threadsafe_function(
      env, argv[1], NULL, resource_name, queue_max, 1, NULL, NULL, NULL,
      call_js_deliver, &g_bridge.tsfn);
  if (status != napi_ok) {
    dlclose(g_bridge.dl);
    g_bridge.dl = NULL;
    return throw_error(env, "failed to create threadsafe function");
  }
  // The engine must not keep an otherwise-idle worker alive; the owner
  // holds the process open. unref like a socket handle.
  napi_unref_threadsafe_function(env, g_bridge.tsfn);

  int32_t rc = g_bridge.start(deliver_from_go, NULL);
  if (rc != 0) {
    char msg[96];
    snprintf(msg, sizeof(msg), "goivm_start failed: rc=%d (see stderr)", rc);
    napi_release_threadsafe_function(g_bridge.tsfn, napi_tsfn_abort);
    g_bridge.tsfn = NULL;
    return throw_error(env, msg);
  }
  g_bridge.started = 1;
  return NULL;
}

// send(payload: Buffer): number  — 0 on success (mirrors goivm_send rc).
static napi_value Send(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_status status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (status != napi_ok || argc < 1) {
    return throw_error(env, "send(payload) requires a Buffer");
  }
  if (!g_bridge.started) return throw_error(env, "bridge not started");

  void* data = NULL;
  size_t len = 0;
  status = napi_get_buffer_info(env, argv[0], &data, &len);
  if (status != napi_ok) return throw_error(env, "payload must be a Buffer");

  int32_t rc = g_bridge.send(data, (int32_t)len);
  napi_value out;
  status = napi_create_int32(env, rc, &out);
  if (status != napi_ok) return throw_error(env, "napi_create_int32 failed");
  return out;
}

// abiVersion(): number — of the LOADED library (-1 before start).
//
// NOTE: there is deliberately NO shutdown export (scale review). The old
// Shutdown called goivm_shutdown ON THE JS THREAD: it joins the Go pumps and
// drains every client group, while a handler blocked pushing into the TSFN
// queue needs the JS thread to drain it — a deadlock; and releasing the TSFN
// while Go goroutines may still call deliver_from_go was a TOCTOU
// use-after-free. Production never called it (SidecarManager.stop()
// deliberately skips teardown; process exit reclaims everything), so the
// export's only reachable behaviors were the pathological ones. The Go
// library still exports goivm_shutdown (resolved above for ABI-surface
// validation); nothing in-process may safely call it.
static napi_value AbiVersion(napi_env env, napi_callback_info info) {
  (void)info;
  napi_value out;
  int32_t v = g_bridge.abi_version != NULL ? g_bridge.abi_version() : -1;
  napi_status status = napi_create_int32(env, v, &out);
  if (status != napi_ok) return throw_error(env, "napi_create_int32 failed");
  return out;
}

// streamCredit(reqID: number, n: number) — grant n pull credits to the
// in-flight pullMode RPC identified by reqID (ABI v3). Direct call into
// the Go library on the JS thread: goivm_stream_credit is a leaf-mutex
// registry lookup + cond broadcast — O(1), allocation-free, never blocks,
// never re-enters N-API — so no TSFN round-trip is needed or wanted
// (credits are upstream control flow; queueing them behind deliveries
// would deadlock the very backpressure they implement). Unknown reqIDs
// are a silent no-op on the Go side (RPC already settled).
static napi_value StreamCredit(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_status status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (status != napi_ok || argc < 2) {
    return throw_error(env, "streamCredit(reqID, n) requires 2 arguments");
  }
  if (!g_bridge.started) return throw_error(env, "bridge not started");
  double req_id = 0;
  status = napi_get_value_double(env, argv[0], &req_id);
  if (status != napi_ok) return throw_error(env, "reqID must be a number");
  int32_t n = 0;
  status = napi_get_value_int32(env, argv[1], &n);
  if (status != napi_ok) return throw_error(env, "n must be a number");
  g_bridge.stream_credit(req_id, n);
  return NULL;
}

// streamCancel(reqID: number) — cancel the pull gate for reqID (the JS
// iterator's .return()/.throw() crossing the boundary, ABI v3). Same
// direct-call rationale and no-op semantics as streamCredit; idempotent.
static napi_value StreamCancel(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_status status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (status != napi_ok || argc < 1) {
    return throw_error(env, "streamCancel(reqID) requires 1 argument");
  }
  if (!g_bridge.started) return throw_error(env, "bridge not started");
  double req_id = 0;
  status = napi_get_value_double(env, argv[0], &req_id);
  if (status != napi_ok) return throw_error(env, "reqID must be a number");
  g_bridge.stream_cancel(req_id);
  return NULL;
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_property_descriptor props[] = {
      {"start", NULL, Start, NULL, NULL, NULL, napi_default, NULL},
      {"send", NULL, Send, NULL, NULL, NULL, napi_default, NULL},
      {"abiVersion", NULL, AbiVersion, NULL, NULL, NULL, napi_default, NULL},
      {"streamCredit", NULL, StreamCredit, NULL, NULL, NULL, napi_default, NULL},
      {"streamCancel", NULL, StreamCancel, NULL, NULL, NULL, napi_default, NULL},
  };
  napi_status status = napi_define_properties(
      env, exports, sizeof(props) / sizeof(props[0]), props);
  GOIVM_FATAL_IF(status != napi_ok, "napi_define_properties failed");
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
