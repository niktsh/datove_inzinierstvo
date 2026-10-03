from krakow_di.config import Settings


def test_defaults_and_empty_seed(monkeypatch):
    monkeypatch.setenv("GENERATOR_SEED", "")
    s = Settings(_env_file=None)
    assert s.generator_seed is None
    assert s.kafka_topic_prefix == "krakow"


def test_env_override_and_database_url(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("GENERATOR_SEED", "42")
    s = Settings(_env_file=None)
    assert s.generator_seed == 42
    assert s.database_url == "postgresql://krakow:change_me@db:5433/krakow_di"
