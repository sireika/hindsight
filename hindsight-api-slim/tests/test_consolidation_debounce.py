"""Tests for the retain-auto consolidation debounce.

With HINDSIGHT_API_CONSOLIDATION_DEBOUNCE_SECONDS > 0, a retain-triggered
(auto) consolidation is inserted with ``next_retry_at`` in the future and a
``task_payload.debounce`` marker — both carried by the INSERT itself, so the
row is never claimable before the window even under a hot poller (a follow-up
UPDATE would race it). A later debounced submit that dedupe-hits the pending
marked operation slides its deadline to now + window, capped at
``created_at + max delay`` (the starvation bound).

Origin scoping: only the post-insert maintenance path submits with
``debounce=True``. Manual ``/consolidate``, the reconcile sweep, and
mutation-triggered submissions stay immediate, and they never slide a pending
debounced operation. Unmarked pending operations are never touched — their
``next_retry_at`` belongs to retry backoff / DeferOperation backpressure.
"""

import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from hindsight_api.engine.memory_engine import MemoryEngine

DEBOUNCE_SECONDS = 300
MAX_DELAY_SECONDS = 900

# Generous window for comparing DB-written timestamps against test-side clocks.
CLOCK_TOLERANCE_SECONDS = 60


@pytest.fixture
def debounce_config(monkeypatch):
    """Enable the consolidation debounce for the duration of one test."""
    from hindsight_api.config import clear_config_cache

    monkeypatch.setenv("HINDSIGHT_API_CONSOLIDATION_DEBOUNCE_SECONDS", str(DEBOUNCE_SECONDS))
    monkeypatch.setenv("HINDSIGHT_API_CONSOLIDATION_DEBOUNCE_MAX_DELAY_SECONDS", str(MAX_DELAY_SECONDS))
    clear_config_cache()
    yield
    clear_config_cache()


@pytest.fixture
def no_inline_execution(memory):
    """Stop the SyncTaskBackend from executing submitted ops inline so the pending
    rows survive for inspection (we're testing scheduling, not execution)."""

    async def _noop(_payload):
        return None

    original = memory._task_backend.submit_task
    memory._task_backend.submit_task = _noop
    yield
    memory._task_backend.submit_task = original


async def _ensure_bank(pool, bank_id: str) -> None:
    await pool.execute(
        "INSERT INTO banks (bank_id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        bank_id,
        bank_id,
    )


async def _fetch_op(pool, operation_id: str):
    return await pool.fetchrow(
        "SELECT next_retry_at, task_payload, created_at FROM async_operations WHERE operation_id = $1",
        uuid.UUID(operation_id),
    )


def _payload(row) -> dict:
    raw = row["task_payload"]
    return json.loads(raw) if isinstance(raw, str) else (raw or {})


async def _cleanup(pool, bank_id: str) -> None:
    await pool.execute("DELETE FROM async_operations WHERE bank_id = $1", bank_id)
    await pool.execute("DELETE FROM banks WHERE bank_id = $1", bank_id)


def _assert_close(actual: datetime, expected: datetime, label: str) -> None:
    delta = abs((actual - expected).total_seconds())
    assert delta <= CLOCK_TOLERANCE_SECONDS, f"{label}: expected ~{expected}, got {actual} ({delta:.1f}s off)"


# ---------------------------------------------------------------------------
# DB-backed scheduling tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_debounced_submit_carries_next_retry_at_and_marker(
    memory, request_context, no_inline_execution, debounce_config
):
    """A retain-auto (debounce=True) submit must land with next_retry_at in the
    future AND the debounce marker, atomically in the inserted row."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        result = await memory.submit_async_consolidation(
            bank_id=bank_id, request_context=request_context, debounce=True
        )
        row = await _fetch_op(pool, result["operation_id"])
        assert row["next_retry_at"] is not None, "debounced op must be unclaimable for the window"
        _assert_close(
            row["next_retry_at"],
            datetime.now(UTC) + timedelta(seconds=DEBOUNCE_SECONDS),
            "next_retry_at",
        )
        marker = _payload(row).get("debounce")
        assert isinstance(marker, dict) and marker.get("v") == 1
        assert "created" in marker
        _assert_close(datetime.fromisoformat(marker["until"]), row["next_retry_at"], "marker.until")
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_manual_submit_is_never_debounced(memory, request_context, no_inline_execution, debounce_config):
    """Manual /consolidate (debounce=False default) stays immediately claimable
    even with the debounce configured on."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        result = await memory.submit_async_consolidation(bank_id=bank_id, request_context=request_context)
        row = await _fetch_op(pool, result["operation_id"])
        assert row["next_retry_at"] is None
        assert "debounce" not in _payload(row)
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_debounce_disabled_by_default(memory, request_context, no_inline_execution):
    """With the default window (0), a debounce-eligible submit behaves exactly
    like today: immediately claimable, no marker."""
    from hindsight_api.config import clear_config_cache

    clear_config_cache()
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        result = await memory.submit_async_consolidation(
            bank_id=bank_id, request_context=request_context, debounce=True
        )
        row = await _fetch_op(pool, result["operation_id"])
        assert row["next_retry_at"] is None
        assert "debounce" not in _payload(row)
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_scoped_submit_is_never_debounced(memory, request_context, no_inline_execution, debounce_config):
    """Scoped runs are targeted and exempt: no marker, immediately claimable."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        result = await memory.submit_async_consolidation(
            bank_id=bank_id,
            request_context=request_context,
            observation_scopes=[["proj-a"]],
            debounce=True,
        )
        row = await _fetch_op(pool, result["operation_id"])
        assert row["next_retry_at"] is None
        assert "debounce" not in _payload(row)
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_second_debounced_submit_slides_deadline(memory, request_context, no_inline_execution, debounce_config):
    """A debounced submit that dedupe-hits the pending marked op slides its
    next_retry_at back out to now + window (and keeps marker.until in sync)."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        first = await memory.submit_async_consolidation(bank_id=bank_id, request_context=request_context, debounce=True)
        # Age the deadline so the slide is observable.
        stale = datetime.now(UTC) - timedelta(seconds=60)
        await pool.execute(
            "UPDATE async_operations SET next_retry_at = $2 WHERE operation_id = $1",
            uuid.UUID(first["operation_id"]),
            stale,
        )

        second = await memory.submit_async_consolidation(
            bank_id=bank_id, request_context=request_context, debounce=True
        )
        assert second.get("deduplicated") is True
        assert second["operation_id"] == first["operation_id"]

        row = await _fetch_op(pool, first["operation_id"])
        _assert_close(
            row["next_retry_at"],
            datetime.now(UTC) + timedelta(seconds=DEBOUNCE_SECONDS),
            "slid next_retry_at",
        )
        _assert_close(datetime.fromisoformat(_payload(row)["debounce"]["until"]), row["next_retry_at"], "marker.until")
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_slide_is_capped_at_created_at_plus_max_delay(
    memory, request_context, no_inline_execution, debounce_config
):
    """The slide never moves past created_at + max delay — a bank that never
    goes quiet still consolidates (starvation bound)."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        first = await memory.submit_async_consolidation(bank_id=bank_id, request_context=request_context, debounce=True)
        # Pretend the op was created long ago, so now + window exceeds the cap.
        aged_created_at = datetime.now(UTC) - timedelta(seconds=MAX_DELAY_SECONDS - 100)
        await pool.execute(
            "UPDATE async_operations SET created_at = $2 WHERE operation_id = $1",
            uuid.UUID(first["operation_id"]),
            aged_created_at,
        )

        second = await memory.submit_async_consolidation(
            bank_id=bank_id, request_context=request_context, debounce=True
        )
        assert second.get("deduplicated") is True

        row = await _fetch_op(pool, first["operation_id"])
        _assert_close(
            row["next_retry_at"],
            aged_created_at + timedelta(seconds=MAX_DELAY_SECONDS),
            "capped next_retry_at",
        )
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_manual_submit_does_not_slide_marked_op(memory, request_context, no_inline_execution, debounce_config):
    """A manual dedupe-hit against a pending debounced op must not move it."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        first = await memory.submit_async_consolidation(bank_id=bank_id, request_context=request_context, debounce=True)
        before = (await _fetch_op(pool, first["operation_id"]))["next_retry_at"]

        second = await memory.submit_async_consolidation(bank_id=bank_id, request_context=request_context)
        assert second.get("deduplicated") is True

        after = (await _fetch_op(pool, first["operation_id"]))["next_retry_at"]
        assert after == before, "manual submits must never slide a debounced op"
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_unmarked_pending_op_is_never_slid(memory, request_context, no_inline_execution, debounce_config):
    """next_retry_at on an UNMARKED pending op belongs to retry backoff /
    DeferOperation backpressure — a debounced dedupe-hit must leave it alone."""
    bank_id = f"test-debounce-{uuid.uuid4().hex[:8]}"
    pool = await memory._get_pool()
    await _ensure_bank(pool, bank_id)
    try:
        manual = await memory.submit_async_consolidation(bank_id=bank_id, request_context=request_context)
        # Simulate DeferOperation-style backpressure on the unmarked op.
        deferred_until = datetime.now(UTC) + timedelta(seconds=5)
        await pool.execute(
            "UPDATE async_operations SET next_retry_at = $2 WHERE operation_id = $1",
            uuid.UUID(manual["operation_id"]),
            deferred_until,
        )

        debounced = await memory.submit_async_consolidation(
            bank_id=bank_id, request_context=request_context, debounce=True
        )
        assert debounced.get("deduplicated") is True

        row = await _fetch_op(pool, manual["operation_id"])
        assert row["next_retry_at"] == deferred_until, "unmarked pending op must never be slid"
        assert "debounce" not in _payload(row)
    finally:
        await _cleanup(pool, bank_id)


@pytest.mark.asyncio
async def test_post_insert_maintenance_submits_with_debounce(memory, request_context):
    """The retain-auto trigger is the (only) origin that passes debounce=True;
    graph maintenance under the same trigger is not debounce-eligible."""
    from hindsight_api.config import HindsightConfig

    consolidation_calls = []

    async def _record_consolidation(**kwargs):
        consolidation_calls.append(kwargs)
        return {"operation_id": str(uuid.uuid4())}

    memory.submit_async_consolidation = _record_consolidation
    memory.submit_async_graph_maintenance = AsyncMock(return_value={"operation_id": None, "no_work": True})

    config = HindsightConfig.from_env()
    config.enable_observations = True
    config.enable_auto_consolidation = True

    await memory._submit_post_insert_maintenance("some-bank", request_context, config=config)

    assert len(consolidation_calls) == 1
    assert consolidation_calls[0]["debounce"] is True
    # Graph maintenance has no debounce parameter at all.
    memory.submit_async_graph_maintenance.assert_awaited_once()
    assert "debounce" not in memory.submit_async_graph_maintenance.await_args.kwargs


# ---------------------------------------------------------------------------
# Unit tests for the slide helper (no database)
# ---------------------------------------------------------------------------


def _op_row(created_at: datetime, operation_id=None) -> dict:
    return {"operation_id": operation_id or uuid.uuid4(), "created_at": created_at}


class TestSlideDebouncedOperation:
    """MemoryEngine._slide_debounced_operation marker + cap semantics."""

    @pytest.mark.asyncio
    async def test_unmarked_payload_is_untouched(self):
        conn = AsyncMock()
        await MemoryEngine._slide_debounced_operation(
            conn,
            _op_row(datetime.now(UTC)),
            {"type": "consolidation"},
            debounce_seconds=300,
            max_delay_seconds=900,
        )
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_wrong_marker_version_is_untouched(self):
        conn = AsyncMock()
        await MemoryEngine._slide_debounced_operation(
            conn,
            _op_row(datetime.now(UTC)),
            {"debounce": {"v": 2}},
            debounce_seconds=300,
            max_delay_seconds=900,
        )
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_marked_op_slides_to_now_plus_window(self):
        conn = AsyncMock()
        now = datetime.now(UTC)
        marker = {"v": 1, "until": now.isoformat(), "created": now.isoformat()}
        row = _op_row(created_at=now - timedelta(seconds=10))
        await MemoryEngine._slide_debounced_operation(
            conn,
            row,
            {"type": "consolidation", "debounce": marker},
            debounce_seconds=300,
            max_delay_seconds=900,
        )
        conn.execute.assert_awaited_once()
        sql, op_id, until, payload_json = conn.execute.await_args.args
        assert "status = 'pending'" in sql, "slide must re-guard on pending status"
        assert op_id == row["operation_id"]
        assert abs((until - (now + timedelta(seconds=300))).total_seconds()) < 5
        payload = json.loads(payload_json)
        assert payload["debounce"]["until"] == until.isoformat(), "marker.until must track the column"
        assert payload["debounce"]["created"] == marker["created"]

    @pytest.mark.asyncio
    async def test_slide_is_capped_by_created_at_plus_max_delay(self):
        conn = AsyncMock()
        now = datetime.now(UTC)
        created_at = now - timedelta(seconds=800)
        marker = {"v": 1, "until": now.isoformat(), "created": created_at.isoformat()}
        await MemoryEngine._slide_debounced_operation(
            conn,
            _op_row(created_at=created_at),
            {"debounce": marker},
            debounce_seconds=300,
            max_delay_seconds=900,
        )
        _sql, _op_id, until, _payload_json = conn.execute.await_args.args
        assert until == created_at + timedelta(seconds=900), "cap must win over now + window"

    @pytest.mark.asyncio
    async def test_naive_created_at_is_treated_as_utc(self):
        conn = AsyncMock()
        created_at_naive = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=800)
        marker = {"v": 1, "until": "x", "created": "x"}
        await MemoryEngine._slide_debounced_operation(
            conn,
            _op_row(created_at=created_at_naive),
            {"debounce": marker},
            debounce_seconds=300,
            max_delay_seconds=900,
        )
        _sql, _op_id, until, _payload_json = conn.execute.await_args.args
        assert until.tzinfo is not None
        assert until == created_at_naive.replace(tzinfo=UTC) + timedelta(seconds=900)
