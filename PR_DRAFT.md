# feat(consolidation): debounce retain-auto consolidation scheduling

## Problem

`MemoryEngine._submit_post_insert_maintenance` submits one consolidation
operation after **every** fact insert (retain or document import), and an idle
worker claims a pending op within one poll interval. On a conversational bank
that means roughly one full consolidation job — an LLM batch call, dedup
adjudication, embedding work — **per retain call**. Back-to-back retains in
the same session each pay for a consolidation that mostly re-reads what the
previous one just compiled. Coalescing exists today only implicitly, when the
worker happens to be busy enough that several submissions dedupe against a
still-pending op.

This cuts directly against the consolidation scaling thesis: memory cost
should scale with *new information*, not with conversation volume. A debounce
makes consolidation cost scale with new information per quiet window instead
of per retain call — a burst of N retains in one session compiles once, with a
fuller fact batch, instead of N times.

## Fix

Two config options (server-level, following the background-maintenance config
pattern):

```
HINDSIGHT_API_CONSOLIDATION_DEBOUNCE_SECONDS            (default 0 = off)
HINDSIGHT_API_CONSOLIDATION_DEBOUNCE_MAX_DELAY_SECONDS  (default 1800)
```

With the default `0`, behavior is today's, byte-for-byte. With a window > 0
(e.g. `300` — one session-quiet window):

1. **Origin scoping.** Only the retain-auto trigger
   (`_submit_post_insert_maintenance`) submits with `debounce=True`, threaded
   explicitly through `submit_async_consolidation` →
   `_submit_async_operation`. Manual `/consolidate`, the reconcile sweep, the
   round-limit re-queue, and delete/update-triggered submissions all keep
   their immediate scheduling.

2. **Race-safe INSERT.** A debounced operation is inserted with
   `next_retry_at = now + window` **and** a
   `task_payload.debounce = {v: 1, until, created}` marker in the same INSERT
   statement. Worker claim queries already filter
   `next_retry_at IS NULL OR next_retry_at <= NOW()`, so the row is atomically
   unclaimable for the window even under a hot poller. (Setting
   `next_retry_at` with a follow-up UPDATE instead would race the poller,
   which can claim a fresh row within one poll interval.)

3. **Marked slide under the bank lock.** A debounced submit that dedupe-hits
   the pending marked operation slides its deadline to `now + window`, capped
   at `created_at + max delay` — the starvation bound: a bank that never goes
   quiet still consolidates. The slide runs on the dedupe path that is already
   serialized per bank by the `FOR NO KEY UPDATE` bank-row lock, and its
   UPDATE re-guards on `status = 'pending'`.

   **Only operations carrying the marker are ever slid.** `next_retry_at` is
   also the operator-visible retry-backoff / `DeferOperation` backpressure
   field (set without bumping `retry_count`), so `retry_count` alone cannot
   identify a debounced op — the marker both protects deferred ops from being
   moved and lets operation listings distinguish "debounced" from
   "retry-blocked/deferred".

## Behavior compatibility

- Default (`0`) preserves current scheduling exactly; the feature is opt-in.
- Facts are stored and recallable immediately either way — only the compiled
  observation layer waits for the window, and it was never same-turn-fresh
  (consolidation is already async).
- Manual and maintenance-driven consolidations are unaffected in all modes,
  and a manual submit never slides a pending debounced op.
- Scoped (`observation_scopes`) submissions are never debounced (they also
  bypass the dedupe path the slide rides on).
- Under `SyncTaskBackend` (embedded/CLI/tests) `submit_task` still executes
  the operation inline, so the debounce degrades to today's immediate
  behavior there — never to starvation.
- `HINDSIGHT_API_CONSOLIDATION_DEBOUNCE_MAX_DELAY_SECONDS` must be ≥ the
  window (validated at startup); a bounded worst-case observation lag of
  `window + max delay` is the deliberate trade for the cost cut.

## Changes

- `hindsight_api/config.py` — the two env options, defaults, dataclass
  fields, `from_env` parsing, and `validate()` checks (non-negative window;
  max delay ≥ window).
- `hindsight_api/engine/memory_engine.py` —
  `_submit_post_insert_maintenance` passes `debounce=True`;
  `submit_async_consolidation(..., debounce=False)` threads the origin flag;
  `_submit_async_operation` computes the window, stamps the marker + carries
  `next_retry_at` in the INSERT, and slides marked pending ops on dedupe
  hits via the new `_slide_debounced_operation` helper.
- `hindsight-docs/docs/developer/configuration.md` — config table entries.

## Tests

`tests/test_consolidation_debounce.py`:

- DB-backed (following `test_consolidation_submit_atomic_dedup.py`
  conventions): debounced INSERT carries `next_retry_at` + marker atomically;
  manual submit unaffected with debounce configured; default window `0`
  byte-preserves today's rows; scoped submits exempt; dedupe-hit slide moves
  the deadline; slide capped at `created_at + max delay`; manual dedupe-hit
  never slides; unmarked (DeferOperation-shaped) pending op never touched;
  retain-auto wiring passes `debounce=True` and graph maintenance stays
  non-debounced.
- Unit tests for `_slide_debounced_operation` (no DB): marker predicate,
  wrong-version marker untouched, slide/cap arithmetic, `marker.until` kept
  in sync with the column, naive `created_at` treated as UTC.

`tests/test_config_validation.py`: defaults, env parsing, negative window
rejected, max delay < window rejected, max delay unvalidated when disabled.
