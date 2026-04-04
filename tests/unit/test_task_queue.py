from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from config.settings import Settings
from core.orchestrator import QUEUE_JOB_NAMES, run_parallel
from core.schema import FinancialFortressData, ModuleResult, SentimentData

NUM_MODULES = len(QUEUE_JOB_NAMES)


def _job_with_result(payload: object) -> MagicMock:
    job = MagicMock()
    job.result = AsyncMock(return_value=payload)
    return job


def _fixed_dt() -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _error_payload(name: str) -> dict[str, str]:
    return {"status": "error", "name": name, "error_message": "stub"}


@pytest.mark.asyncio
async def test_queue_path_enqueues_all_jobs_with_ticker(
    tmp_path: Path,
) -> None:
    out = tmp_path / "spectre.json"
    settings = Settings().model_copy(update={"OUTPUT_PATH": out})
    t = _fixed_dt()

    p_fin = ModuleResult(
        name="financial",
        status="success",
        started_at=t,
        completed_at=t,
        financial=FinancialFortressData(headline="TestCo"),
    ).model_dump(mode="json")
    p_ip = ModuleResult(
        name="ip",
        status="error",
        started_at=t,
        completed_at=t,
        error_message="ip failed",
    ).model_dump(mode="json")
    p_err: dict[str, str] = {
        "status": "error",
        "error_message": "simulated failure",
    }

    side_effects = [
        _job_with_result(p_fin),
        _job_with_result(p_ip),
        _job_with_result(p_err),
    ]
    for name, _ in QUEUE_JOB_NAMES[3:]:
        side_effects.append(
            _job_with_result(_error_payload(name))
        )

    pool = MagicMock()
    pool.enqueue_job = AsyncMock(side_effect=side_effects)
    pool.close = AsyncMock()

    with patch(
        "core.orchestrator.create_pool", new_callable=AsyncMock
    ) as mock_pool:
        mock_pool.return_value = pool
        result = await run_parallel(
            "AAPL", use_queue=True, settings=settings
        )

    mock_pool.assert_awaited_once()
    assert pool.enqueue_job.await_count == NUM_MODULES
    pool.enqueue_job.assert_any_call("run_financial_task", "AAPL")
    pool.enqueue_job.assert_any_call("run_ip_task", "AAPL")
    pool.enqueue_job.assert_any_call("run_sentiment_task", "AAPL")
    pool.close.assert_awaited()

    assert result.ticker == "AAPL"
    assert len(result.modules) == NUM_MODULES
    by_name = {m.name: m for m in result.modules}
    assert by_name["financial"].status == "success"
    assert by_name["ip"].status == "error"
    assert out.is_file()


@pytest.mark.asyncio
async def test_one_job_result_exception_still_returns_others(
    tmp_path: Path,
) -> None:
    out = tmp_path / "spectre2.json"
    settings = Settings().model_copy(update={"OUTPUT_PATH": out})
    t = _fixed_dt()

    ok_fin = ModuleResult(
        name="financial",
        status="success",
        started_at=t,
        completed_at=t,
        financial=FinancialFortressData(headline="X"),
    ).model_dump(mode="json")
    ok_sent = ModuleResult(
        name="sentiment",
        status="success",
        started_at=t,
        completed_at=t,
        sentiment=SentimentData(compound_score=None, sample_size=2),
    ).model_dump(mode="json")

    ok_job = _job_with_result(ok_fin)
    bad_job = MagicMock()
    bad_job.result = AsyncMock(side_effect=RuntimeError("redis hiccup"))
    ok2 = _job_with_result(ok_sent)

    side_effects: list[MagicMock] = [ok_job, bad_job, ok2]
    for name, _ in QUEUE_JOB_NAMES[3:]:
        side_effects.append(
            _job_with_result(_error_payload(name))
        )

    pool = MagicMock()
    pool.enqueue_job = AsyncMock(side_effect=side_effects)
    pool.close = AsyncMock()

    with patch(
        "core.orchestrator.create_pool", new_callable=AsyncMock
    ) as mock_pool:
        mock_pool.return_value = pool
        result = await run_parallel(
            "X", use_queue=True, settings=settings
        )

    assert len(result.modules) == NUM_MODULES
    by_name = {m.name: m for m in result.modules}
    assert by_name["financial"].status == "success"
    assert by_name["ip"].status == "error"
    assert "hiccup" in (by_name["ip"].error_message or "")
    assert by_name["sentiment"].status == "success"
