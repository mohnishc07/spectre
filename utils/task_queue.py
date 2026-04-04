from __future__ import annotations

import json
from typing import Any

from arq.connections import RedisSettings

from config.settings import Settings
from modules.financial import FinancialModule
from modules.ip import IPModule
from modules.sentiment import SentimentModule


def arq_json_serializer(obj: Any) -> bytes:
    return json.dumps(obj, default=str).encode("utf-8")


def arq_json_deserializer(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


def redis_settings_from_app_settings(settings: Settings) -> RedisSettings:
    return RedisSettings.from_dsn(settings.REDIS_URL)


async def run_financial_task(ctx: dict[str, Any], ticker: str) -> dict[str, Any]:
    del ctx  # unused
    try:
        mod = FinancialModule()
        return mod.run(ticker).model_dump(mode="json")
    except Exception as e:
        return {"status": "error", "error_message": str(e)}


async def run_ip_task(ctx: dict[str, Any], ticker: str) -> dict[str, Any]:
    del ctx
    try:
        mod = IPModule()
        return mod.run(ticker).model_dump(mode="json")
    except Exception as e:
        return {"status": "error", "error_message": str(e)}


async def run_sentiment_task(ctx: dict[str, Any], ticker: str) -> dict[str, Any]:
    del ctx
    try:
        mod = SentimentModule()
        return mod.run(ticker).model_dump(mode="json")
    except Exception as e:
        return {"status": "error", "error_message": str(e)}


_worker_settings = Settings()


class WorkerSettings:
    redis_settings = redis_settings_from_app_settings(_worker_settings)
    max_jobs = _worker_settings.ARQ_MAX_JOBS
    job_timeout = _worker_settings.ARQ_JOB_TIMEOUT
    keep_result = 3600
    retry_jobs = True
    max_tries = 3
    job_serializer = arq_json_serializer
    job_deserializer = arq_json_deserializer
    functions = [run_financial_task, run_ip_task, run_sentiment_task]
