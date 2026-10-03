from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "krakow_di"
    postgres_user: str = "krakow"
    postgres_password: str = "change_me"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prefix: str = "krakow"
    kafka_external_host: str = ""
    kafka_external_port: int = 9094
    kafka_teams_user: str = "teams"
    kafka_teams_password: str = "change_me"

    # Sources
    travelpayouts_token: str = ""
    travelpayouts_market: str = "sk"

    # Generator
    generator_tick_seconds: int = 300
    generator_seed: int | None = None
    generator_time_speedup: float = 1.0

    producer_id: str = "tuke-di-krakow"

    @field_validator("generator_seed", mode="before")
    @classmethod
    def _empty_seed_is_none(cls, v):
        return None if v in ("", None) else v

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
