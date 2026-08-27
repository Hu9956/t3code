/**
 * DshAdapter — shape type for the DSH provider adapter.
 *
 * The driver model ({@link ../Drivers/DshDriver}) bundles one adapter
 * per instance as a captured closure, so this module only retains the shape
 * interface as a naming anchor for the driver bundle. DSH is HTTP-only and
 * carries a per-instance `baseUrl` (default http://127.0.0.1:3080).
 *
 * The shape extends the generic `ProviderAdapterShape` so every method
 * (`startSession`/`sendTurn`/`interruptTurn` …) is available, while the
 * DSH specialization is the `baseUrl` that each instance was constructed
 * with.
 *
 * @module DshAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * DshAdapterShape — per-instance DSH adapter contract.
 *
 * Carries the DSH host `baseUrl` alongside the generic adapter methods so
 * callers (driver, tests, or direct HTTP) can inspect the routing target
 * without capturing it via closure alone.
 */
export interface DshAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  /** DSH host origin (e.g. http://127.0.0.1:3080) — normalized without trailing slash. */
  readonly baseUrl: string;
}
