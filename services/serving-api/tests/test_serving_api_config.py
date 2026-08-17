"""Tests for serving_api/config.py (#160)."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict
from serving_api.config import (
    DEFAULT_HOST,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_SIZE,
    DEFAULT_PORT,
    ServingApiConfig,
)

REQUIRED_ENV = {
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "de4",
    "POSTGRES_USER": "de4",
    "POSTGRES_PASSWORD": "secret",
}


def test_from_env_applies_defaults_for_optional_settings() -> None:
    config = ServingApiConfig.from_env(REQUIRED_ENV)

    assert config.host == DEFAULT_HOST
    assert config.port == DEFAULT_PORT
    assert config.pool_min_size == DEFAULT_POOL_MIN_SIZE
    assert config.pool_max_size == DEFAULT_POOL_MAX_SIZE


def test_from_env_reads_optional_settings() -> None:
    config = ServingApiConfig.from_env(
        REQUIRED_ENV
        | {
            "SERVING_API_HOST": "127.0.0.1",
            "SERVING_API_PORT": "9000",
            "SERVING_API_POOL_MIN_SIZE": "2",
            "SERVING_API_POOL_MAX_SIZE": "20",
        }
    )

    assert config.host == "127.0.0.1"
    assert config.port == 9000
    assert config.pool_min_size == 2
    assert config.pool_max_size == 20


@pytest.mark.parametrize("missing", sorted(REQUIRED_ENV))
def test_from_env_requires_every_postgres_setting(missing: str) -> None:
    env = {key: value for key, value in REQUIRED_ENV.items() if key != missing}

    with pytest.raises(ValueError, match=missing):
        ServingApiConfig.from_env(env)


def test_conninfo_keeps_values_that_need_quoting() -> None:
    # 접속 문자열을 직접 조립했다면 공백이 들어간 비밀번호에서 깨진다.
    config = ServingApiConfig.from_env(REQUIRED_ENV | {"POSTGRES_PASSWORD": "two words"})

    parsed = conninfo_to_dict(config.conninfo)

    assert parsed["host"] == "localhost"
    assert parsed["port"] == "5432"
    assert parsed["dbname"] == "de4"
    assert parsed["user"] == "de4"
    assert parsed["password"] == "two words"


def test_conninfo_pins_the_session_timezone_to_utc() -> None:
    # 고정하지 않으면 timestamptz가 서버 로컬 타임존 오프셋으로 직렬화된다.
    config = ServingApiConfig.from_env(REQUIRED_ENV)

    assert conninfo_to_dict(config.conninfo)["options"] == "-c timezone=UTC"
