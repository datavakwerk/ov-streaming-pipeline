"""Environment-driven configuration shared by all services (12-factor: config in env)."""

from __future__ import annotations

import os


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name)
    if value is None or value == "":
        if default is None:
            raise RuntimeError(f"missing required environment variable {name}")
        return default
    return value


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


def env_bool(name: str, default: bool) -> bool:
    return env(name, "true" if default else "false").strip().lower() in {"1", "true", "yes", "on"}


def env_list(name: str, default: str = "") -> list[str]:
    raw = env(name, default) if default != "" else os.environ.get(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def kafka_bootstrap() -> str:
    return env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")


def postgres_dsn() -> str:
    return (
        f"postgresql://{env('POSTGRES_USER', 'ov')}:{env('POSTGRES_PASSWORD', 'ov')}"
        f"@{env('POSTGRES_HOST', 'localhost')}:{env('POSTGRES_PORT', '5432')}"
        f"/{env('POSTGRES_DB', 'ov')}"
    )
