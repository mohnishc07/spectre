from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    PROXY_LIST: list[str] = Field(default_factory=list)
    SENTIMENT_LOOKBACK_DAYS: int = 14
    OUTPUT_PATH: Path = Path("spectre_analysis.json")
    DRY_RUN: bool = False

    REDIS_URL: str = "redis://localhost:6379/0"
    ARQ_MAX_JOBS: int = 10
    ARQ_JOB_TIMEOUT: int = 120
    USE_TASK_QUEUE: bool = False

    HTTP_TIMEOUT_SECONDS: float = 30.0
    USER_AGENT_POOL: list[str] = Field(
        default_factory=lambda: ["Spectre/0.1 (research)"],
    )
    EDGAR_IDENTITY: str = "SpectreBot contact@example.com"

    @field_validator("PROXY_LIST", mode="before")
    @classmethod
    def split_proxy_list(cls, v: object) -> list[str]:
        if v is None or v == "":
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return []

    @field_validator("USER_AGENT_POOL", mode="before")
    @classmethod
    def split_user_agents(cls, v: object) -> list[str]:
        if v is None or v == "":
            return ["Spectre/0.1 (research)"]
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return ["Spectre/0.1 (research)"]


def get_settings() -> Settings:
    return Settings()
