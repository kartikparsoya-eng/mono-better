/**
 * Derives the GO_IVM_* env for a worker-spawned (or in-process napi) sidecar
 * from the goSidecar config — the O1 "one config, both processes" rule
 * (consumed by server/syncer.ts; lives here because importing syncer.ts
 * executes its module-level worker entrypoint).
 *
 * goPrimaryTrigger implies GO_IVM_ADVANCE_DRIVE (user's-audit wiring bug):
 * the trigger path sources every user-query advance via
 * advanceToHead[Stream], and the sidecar REJECTS advanceToHeadStream
 * without drive mode ("requires drive mode", go-ivm advance_to_head.go) —
 * drive is what arms the per-CG Snapshotters and binds tablesource leaves
 * to the pinned frame. Pre-fix, only advanceDrive set the env, so a spawned
 * trigger-primary sidecar failed every advance; the bug was masked in
 * production because the Docker images pair GO_PRIMARY_TRIGGER=true with
 * ADVANCE_DRIVE=true at image build time.
 */
export function deriveGoSidecarSpawnEnv(
  sc: {
    advanceToHead?: boolean | undefined;
    advanceDrive?: boolean | undefined;
    goPrimaryTrigger?: boolean | undefined;
  },
  appID: string,
): Record<string, string> {
  const wantsAdvanceToHead =
    (sc.advanceToHead ?? false) ||
    (sc.advanceDrive ?? false) ||
    (sc.goPrimaryTrigger ?? false);
  const spawnEnv: Record<string, string> = {GO_IVM_APP_ID: appID};
  if (wantsAdvanceToHead) {
    spawnEnv.GO_IVM_ADVANCE_TO_HEAD = 'true';
    spawnEnv.GO_IVM_SOURCE_MODE = 'table';
  }
  if ((sc.advanceDrive ?? false) || (sc.goPrimaryTrigger ?? false)) {
    spawnEnv.GO_IVM_ADVANCE_DRIVE = 'true';
  }
  return spawnEnv;
}
