/**
 * Derives the GO_IVM_* env for the in-process (napi) Go engine — the O1
 * "one config, both sides" rule (consumed by server/syncer.ts; lives here
 * because importing syncer.ts executes its module-level worker entrypoint).
 *
 * The engine always runs the production configuration: table-mode sources,
 * self-derived advance (advanceToHeadStream). The only per-deployment value
 * is the shard's appID (used as the permissions-table-watch fallback when
 * the wire didn't carry one).
 */
export function deriveGoSidecarSpawnEnv(appID: string): Record<string, string> {
  return {GO_IVM_APP_ID: appID};
}
