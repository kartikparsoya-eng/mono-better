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
//   4. Backpressure: the TSFN queue is bounded; the deliver callback uses
//      napi_tsfn_blocking, so a slow JS consumer blocks the calling Go
//      goroutine — propagating into the engine exactly like a slow socket.
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

#include <assert.h>
#include <dlfcn.h>
#include <node_api.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define EXPECTED_GOIVM_ABI 1
#define TSFN_MAX_QUEUE 8192

typedef void (*goivm_deliver_cb)(void* ctx, int32_t kind, const void* data,
                                 int32_t len);
typedef int32_t (*goivm_start_fn)(goivm_deliver_cb cb, void* ctx);
typedef int32_t (*goivm_send_fn)(const void* data, int32_t len);
typedef void (*goivm_shutdown_fn)(void);
typedef int32_t (*goivm_abi_version_fn)(void);

typedef struct {
  void* dl;
  goivm_start_fn start;
  goivm_send_fn send;
  goivm_shutdown_fn shutdown;
  goivm_abi_version_fn abi_version;
  napi_threadsafe_function tsfn;
  int started;
} bridge_state;

static bridge_state g_bridge = {0};

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
  assert(status == napi_ok);
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
      assert(status == napi_ok);
      free(item->data);
    }
  } else {
    status = napi_create_buffer_copy(env, 0, "", NULL, &buf_val);
    assert(status == napi_ok);
    free(item->data);
  }
  free(item);

  status = napi_get_undefined(env, &undefined);
  assert(status == napi_ok);
  napi_value argv[2] = {kind_val, buf_val};
  // Exceptions from the JS callback propagate as uncaught — the JS wrapper
  // (napi-transport.ts) try/catches internally, mirroring #onData's
  // load-bearing try/catch around onPartial.
  napi_call_function(env, undefined, js_cb, 2, argv, NULL);
}

// The C deliver callback Go invokes from its goroutines. Copies the payload
// and enqueues; blocking mode = backpressure into Go when JS lags.
static void deliver_from_go(void* ctx, int32_t kind, const void* data,
                            int32_t len) {
  (void)ctx;
  if (g_bridge.tsfn == NULL) return;  // shutdown race: drop
  delivery_item* item = (delivery_item*)malloc(sizeof(delivery_item));
  if (item == NULL) return;
  item->kind = kind;
  item->len = (size_t)(len > 0 ? len : 0);
  item->data = malloc(item->len > 0 ? item->len : 1);
  if (item->data == NULL) {
    free(item);
    return;
  }
  if (item->len > 0) memcpy(item->data, data, item->len);
  napi_status status =
      napi_call_threadsafe_function(g_bridge.tsfn, item, napi_tsfn_blocking);
  if (status != napi_ok) {
    free(item->data);
    free(item);
  }
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
  if (!g_bridge.start || !g_bridge.send || !g_bridge.shutdown ||
      !g_bridge.abi_version) {
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
  assert(status == napi_ok);
  status = napi_create_threadsafe_function(
      env, argv[1], NULL, resource_name, TSFN_MAX_QUEUE, 1, NULL, NULL, NULL,
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
  assert(status == napi_ok);
  return out;
}

// shutdown(): void — tears down the Go host; the library stays loaded (a Go
// runtime cannot be unloaded/restarted in-process).
static napi_value Shutdown(napi_env env, napi_callback_info info) {
  (void)info;
  if (g_bridge.started && g_bridge.shutdown != NULL) {
    g_bridge.shutdown();
  }
  if (g_bridge.tsfn != NULL) {
    napi_release_threadsafe_function(g_bridge.tsfn, napi_tsfn_abort);
    g_bridge.tsfn = NULL;
  }
  (void)env;
  return NULL;
}

// abiVersion(): number — of the LOADED library (-1 before start).
static napi_value AbiVersion(napi_env env, napi_callback_info info) {
  (void)info;
  napi_value out;
  int32_t v = g_bridge.abi_version != NULL ? g_bridge.abi_version() : -1;
  napi_status status = napi_create_int32(env, v, &out);
  assert(status == napi_ok);
  return out;
}

static napi_value Init(napi_env env, napi_value exports) {
  napi_property_descriptor props[] = {
      {"start", NULL, Start, NULL, NULL, NULL, napi_default, NULL},
      {"send", NULL, Send, NULL, NULL, NULL, napi_default, NULL},
      {"shutdown", NULL, Shutdown, NULL, NULL, NULL, napi_default, NULL},
      {"abiVersion", NULL, AbiVersion, NULL, NULL, NULL, napi_default, NULL},
  };
  napi_status status = napi_define_properties(
      env, exports, sizeof(props) / sizeof(props[0]), props);
  assert(status == napi_ok);
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
