from functools import lru_cache
from urllib.parse import quote

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

    # Zdroje
    travelpayouts_token: str = ""
    travelpayouts_market: str = "sk"

    # Generátor
    generator_tick_seconds: int = 300
    generator_seed: int | None = None
    generator_time_speedup: float = 1.0
    generator_base_rate: float = 0.004
    generator_popularity_sigma: float = 1.0
    generator_theater_base_rate: float = 0.005

    # Plánovač (intervaly v hodinách; generátor beží každých generator_tick_seconds)
    scheduler_travelpayouts_hours: float = 6
    scheduler_ryanair_hours: float = 12
    scheduler_theater_programme_hours: float = 24
    scheduler_theater_snapshots_hours: float = 3
    scheduler_heartbeat_file: str = "data/scheduler.heartbeat"

    producer_id: str = "tuke-di-krakow"

    @field_validator("generator_seed", mode="before")
    @classmethod
    def _empty_seed_is_none(cls, v):
        return None if v in ("", None) else v

    @property
    def database_url(self) -> str:
        # používateľ a heslo sa kódujú, aby znaky ako `/`, `+`, `@` nerozbili URL
        user = quote(self.postgres_user, safe="")
        password = quote(self.postgres_password, safe="")
        return (
            f"postgresql://{user}:{password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
