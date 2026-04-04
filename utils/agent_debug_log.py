"""Session debug NDJSON logger (fold entire file in editor when not debugging)."""

from __future__ import annotations

import json
import time
from pathlib import Path

_LOG_PATH = Path(__file__).resolve().parent.parent / "debug-4aa53b.log"
_SESSION = "4aa53b"


def agent_log(hypothesis_id: str, location: str, message: str, data: dict[str, object]) -> None:
    # #region agent log
    payload = {
        "sessionId": _SESSION,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    with _LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, default=str) + "\n")
    # #endregion
