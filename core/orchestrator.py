from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from arq.connections import create_pool
from arq.jobs import Job

from config.settings import Settings
from core.schema import ModuleResult, SpectreResult, utcnow
from modules.base import AbstractModule
from modules.financial import FinancialModule
from modules.ip import IPModule
from modules.pestel import PESTELModule
from modules.porters import PortersModule
from modules.sentiment import SentimentModule
from modules.vrio import VRIOModule
from utils.agent_debug_log import agent_log
from utils.logger import get_logger
from utils.task_queue import (
    arq_json_deserializer,
    arq_json_serializer,
    redis_settings_from_app_settings,
)

_log = get_logger(__name__)

REGISTERED_MODULES: list[type[AbstractModule]] = [
    FinancialModule,
    IPModule,
    SentimentModule,
    PortersModule,
    VRIOModule,
    PESTELModule,
]

QUEUE_JOB_NAMES: list[tuple[str, str]] = [
    ("financial", "run_financial_task"),
    ("ip", "run_ip_task"),
    ("sentiment", "run_sentiment_task"),
    ("porters", "run_porters_task"),
    ("vrio", "run_vrio_task"),
    ("pestel", "run_pestel_task"),
]


def _module_result_from_queue_payload(
    payload: object,
    expected_name: str,
) -> ModuleResult:
    if not isinstance(payload, dict):
        now = utcnow()
        return ModuleResult(
            name=expected_name,
            status="error",
            started_at=now,
            completed_at=now,
            error_message="non_dict_job_result",
        )
    data: dict[str, Any] = dict(payload)
    if "name" not in data:
        data["name"] = expected_name
    try:
        return ModuleResult.model_validate(data)
    except Exception as e:
        now = utcnow()
        return ModuleResult(
            name=expected_name,
            status="error",
            started_at=now,
            completed_at=now,
            error_message=str(e),
        )


async def _safe_job_result(job: Job, expected_name: str) -> ModuleResult:
    try:
        result = await job.result()
        return _module_result_from_queue_payload(result, expected_name)
    except Exception as e:
        now = utcnow()
        return ModuleResult(
            name=expected_name,
            status="error",
            started_at=now,
            completed_at=now,
            error_message=str(e),
        )


async def _run_module_in_thread(cls: type[AbstractModule], ticker: str) -> ModuleResult:
    name = getattr(cls, "name", "unknown")

    def _inner() -> ModuleResult:
        try:
            mod = cls()
            if not mod.validate():
                now = utcnow()
                return ModuleResult(
                    name=name,
                    status="error",
                    started_at=now,
                    completed_at=now,
                    error_message="validate_failed",
                )
            return mod.run(ticker)
        except Exception as e:
            now = utcnow()
            return ModuleResult(
                name=name,
                status="error",
                started_at=now,
                completed_at=now,
                error_message=str(e),
            )

    return await asyncio.to_thread(_inner)


async def _run_in_process(ticker: str) -> list[ModuleResult]:
    tasks = [_run_module_in_thread(cls, ticker) for cls in REGISTERED_MODULES]
    return list(await asyncio.gather(*tasks, return_exceptions=False))


async def _run_via_queue(ticker: str, settings: Settings) -> list[ModuleResult]:
    redis_settings = redis_settings_from_app_settings(settings)
    pool = await create_pool(
        redis_settings,
        job_serializer=arq_json_serializer,
        job_deserializer=arq_json_deserializer,
    )
    try:
        pairs: list[tuple[str, Job | None]] = []
        for name, fn in QUEUE_JOB_NAMES:
            job = await pool.enqueue_job(fn, ticker)
            pairs.append((name, job))

        async def _one(pair: tuple[str, Job | None]) -> ModuleResult:
            name, job = pair
            if job is None:
                now = utcnow()
                return ModuleResult(
                    name=name,
                    status="error",
                    started_at=now,
                    completed_at=now,
                    error_message="enqueue_failed",
                )
            return await _safe_job_result(job, name)

        return list(await asyncio.gather(*(_one(p) for p in pairs)))
    finally:
        await pool.close(close_connection_pool=True)


def write_spectre_json(path: Path, result: SpectreResult) -> None:
    # #region agent log
    agent_log(
        "H4",
        "orchestrator.py:write_spectre_json",
        "statuses_before_json_dump",
        {
            "modules": [(m.name, m.status) for m in result.modules],
        },
    )
    # #endregion
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = result.model_dump_json(indent=2)
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)


async def run_parallel(
    ticker: str,
    *,
    use_queue: bool = False,
    settings: Settings,
) -> SpectreResult:
    # #region agent log
    agent_log(
        "H3",
        "orchestrator.py:run_parallel",
        "entry_flags",
        {
            "use_queue": use_queue,
            "dry_run": settings.DRY_RUN,
            "ticker": ticker,
        },
    )
    # #endregion
    if use_queue:
        modules = await _run_via_queue(ticker, settings)
    else:
        modules = await _run_in_process(ticker)

    # #region agent log
    agent_log(
        "H2",
        "orchestrator.py:run_parallel",
        "after_module_execution",
        {
            "statuses_from_modules": [(m.name, m.status) for m in modules],
            "path": "in_process" if not use_queue else "arq_queue",
        },
    )
    # #endregion

    result = SpectreResult(ticker=ticker.upper(), modules=modules)
    write_spectre_json(settings.OUTPUT_PATH, result)
    _log.info("spectre_complete", ticker=ticker, path=str(settings.OUTPUT_PATH))
    return result
