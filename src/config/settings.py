"""Application settings using Pydantic BaseSettings."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    DATA_PATH: str = "data"
    DUCKDB_PATH: str = "data/macro.duckdb"
    ALERT_EMAIL: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


# Global settings instance
settings = Settings()

