# feat(recall): configurable observation recency anchor (`occurred_start` | `mentioned_at`)

## Problem

Recall reranking derives every result's recency effective-date with the same
coalesce order: `occurred_start` → `mentioned_at` → `occurred_end`
(`apply_combined_scoring`, `engine/search/reranking.py`). That order is right
for point-in-time facts, but observations aggregate their dates asymmetrically
during consolidation:

- `occurred_start` = **MIN** across supporting facts — the observation's
  *oldest* evidence, frozen forever.
- `mentioned_at` = **MAX** across supporting facts — GREATEST-refreshed every
  time a support is re-confirmed.

So an event-anchored observation the user keeps re-confirming ("still training
for the marathon", week after week) is recency-penalized to the day of its
oldest support forever, while a long-unconfirmed observation never decays
relative to it. The recency band (`_RECENCY_ALPHA`, ±10%) exists precisely to
express freshness, but for observations it is anchored to the one date that
can never move forward — the ranking is stale-blind exactly on the memory type
where staleness matters most.

Stative observations (NULL `occurred_start`) already fall through to
`mentioned_at` and self-refresh today; only event-anchored observations are
pinned. This inconsistency is invisible until a bank is old enough for
re-confirmed observations to compete with fresh raw facts.

## Fix

New config option, following the existing recency-decay config pattern:

```
HINDSIGHT_API_OBSERVATION_RECENCY_ANCHOR = occurred_start | mentioned_at
```

- **`occurred_start` (default)** — today's behavior, byte-for-byte. Every
  result keeps the historical `occurred_start` → `mentioned_at` →
  `occurred_end` coalesce.
- **`mentioned_at`** — results with `fact_type == "observation"` anchor
  mentioned-at-first: `mentioned_at` → `occurred_start` → `occurred_end`.
  Re-confirmed observations rank as fresh as their freshest support;
  long-unconfirmed ones decay naturally through the existing recency curve.
  Non-observation results are unchanged in all modes.

The anchor is applied at **both** sites that date observations during
reranking, so they can never disagree:

1. `effective_recency_date(...)` (new, shared) — the recency effective-date
   selection inside `apply_combined_scoring`.
2. The `[Date: …]` document prefix in `CrossEncoderReranker.rerank` — under a
   real cross-encoder, event-anchored observations were also *semantically*
   ranked against their oldest date before the recency multiplier ever ran.
   With the anchor on, the cross-encoder sees the same date the recency boost
   scores against.

No SQL, no schema, no API change — `mentioned_at` is already present on every
retrieval row. Zero marginal runtime cost (pure scoring).

## Why this matters at scale

Your consolidation/scaling thesis is that memory cost should scale with *new
information*, not with conversation volume — and compiled observations are the
mechanism: they are the dense, cost-efficient recall surface distilled from
many raw facts. That only pays off if observations actually *win* recall when
they should. Anchoring an observation's freshness to its oldest support
penalizes exactly the observations consolidation worked hardest on (many
supports over a long window), pushing recall back toward the raw-fact layer.
Anchoring to the freshest support (`mentioned_at` is already maintained as
GREATEST across supports — the write side has always treated re-confirmation
as freshness) lets the compiled layer carry the recall load it was built for.

## Behavior compatibility

- Default (`occurred_start`) preserves current ranking behavior exactly; the
  option is opt-in.
- `mentioned_at` mode changes ranking only for observation-type results that
  carry a `mentioned_at`; every other row keeps the historical order.
- Invalid values warn and fall back to the default (same pattern as
  `HINDSIGHT_API_RECENCY_DECAY_FUNCTION`).

## Changes

- `hindsight_api/config.py` — `ENV_OBSERVATION_RECENCY_ANCHOR`,
  `OBSERVATION_RECENCY_ANCHORS`, `DEFAULT_OBSERVATION_RECENCY_ANCHOR`,
  validator, dataclass field, `from_env` parsing (static/server-level, like
  the other recency knobs).
- `hindsight_api/engine/search/reranking.py` — shared
  `effective_recency_date()`; `apply_combined_scoring(...,
  observation_recency_anchor=...)`; `CrossEncoderReranker.rerank(...,
  observation_recency_anchor=...)` date-prefix anchoring.
- `hindsight_api/engine/memory_engine.py` — pass the configured anchor to both
  call sites.
- `hindsight-docs/docs/developer/configuration.md` — config table entry.

## Tests

- `tests/test_combined_scoring.py` — new `TestObservationRecencyAnchor`:
  default-order preservation (observations and non-observations), mentioned-at
  anchoring, fallback order, threading through `apply_combined_scoring`, and
  cross-encoder `[Date: …]` prefix behavior in both modes (including the
  "mentioned_at-only world fact stays unprefixed" historical shape).
- `tests/test_config_validation.py` — default, env parsing, invalid-value
  fallback with warning.
- Existing suites green: `test_combined_scoring.py`,
  `test_config_validation.py`, `test_recall_scoring_time.py`,
  `test_reranker_score_normalization.py`, `test_reranking_proof_count.py`
  (113 passed).
