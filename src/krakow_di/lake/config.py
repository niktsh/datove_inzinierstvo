"""Načítanie `config/lake_sources.yaml` a zostavenie adaptérov.

Hodnoty môžu obsahovať `${PREMENNA}` alebo `${PREMENNA:-predvolená}` (berie sa z prostredia
a zo súboru .env). Prihlasovacie údaje cudzích tímov patria iba do .env: v YAML sa uvádza
názov premennej (`username_env`, `password_env`).
"""

import importlib
import os
import re
from pathlib import Path

import yaml
from dotenv import dotenv_values

from krakow_di.lake.adapters.base import Adapter
from krakow_di.lake.adapters.kafka import KafkaAdapter
from krakow_di.lake.adapters.rest import RestPollAdapter
from krakow_di.lake.adapters.sse import SseAdapter
from krakow_di.lake.adapters.websocket import WebSocketAdapter

DEFAULT_PATH = Path("config/lake_sources.yaml")
_VAR = re.compile(r"\$\{(\w+)(?::-(.*?))?\}")


class LakeConfigError(ValueError):
    pass


def default_env() -> dict[str, str]:
    env = {k: v for k, v in dotenv_values(".env").items() if v is not None}
    env.update(os.environ)
    return env


def _expand(value, env: dict[str, str]):
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            if name in env:
                return env[name]
            if default is not None:
                return default
            raise LakeConfigError(f"chýba premenná prostredia {name}")
        return _VAR.sub(repl, value)
    if isinstance(value, list):
        return [_expand(v, env) for v in value]
    if isinstance(value, dict):
        return {k: _expand(v, env) for k, v in value.items()}
    return value


def _secret(entry: dict, key: str, env: dict[str, str]) -> str | None:
    name = entry.pop(f"{key}_env", None)
    if name is None:
        return None
    if name not in env:
        raise LakeConfigError(f"{entry.get('team')}: chýba premenná {name} v .env")
    return env[name]


def build_adapter(entry: dict, env: dict[str, str]) -> Adapter:
    entry = dict(_expand(entry, env))
    kind = entry.pop("type", None)
    team = entry.get("team")
    if not team or not kind:
        raise LakeConfigError(f"zdroj musí mať team a type: {entry}")
    entry.pop("enabled", None)
    entry.pop("note", None)
    username = _secret(entry, "username", env)
    password = _secret(entry, "password", env)
    token = _secret(entry, "token", env)
    headers = dict(entry.pop("headers", {}) or {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if kind == "kafka":
        return KafkaAdapter(
            team=team, bootstrap=entry["bootstrap"], group_id=entry["group_id"],
            topics=entry.get("topics"), topics_regex=entry.get("topics_regex"),
            security_protocol=entry.get("security_protocol", "PLAINTEXT"),
            sasl_mechanism=entry.get("sasl_mechanism"), username=username, password=password,
            offset_reset=entry.get("offset_reset", "earliest"))
    if kind == "sse":
        return SseAdapter(team, entry["url"], headers)
    if kind == "ws":
        return WebSocketAdapter(team, entry["url"], headers)
    if kind == "rest":
        return RestPollAdapter(team, entry["urls"], float(entry.get("interval_seconds", 300)),
                               headers)
    if kind == "custom":  # adaptér konkrétneho tímu: lake/adapters/team_XX.py
        module, _, cls = entry["class"].partition(":")
        factory = getattr(importlib.import_module(module), cls)
        kwargs = {k: v for k, v in entry.items() if k not in ("class", "team")}
        return factory(team=team, username=username, password=password, headers=headers,
                       **kwargs)
    raise LakeConfigError(f"{team}: neznámy type {kind!r}")


def load_adapters(path: Path | str = DEFAULT_PATH, env: dict[str, str] | None = None
                  ) -> list[Adapter]:
    env = default_env() if env is None else env
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    adapters = []
    for entry in raw.get("sources") or []:
        if entry.get("enabled", True) is False:
            continue
        adapters.append(build_adapter(entry, env))
    names = [a.name for a in adapters]
    if len(set(names)) != len(names):
        raise LakeConfigError(f"duplicitné zdroje tím/kanál: {names}")
    return adapters
