"""Tests for serving_api/config.py (#160)."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict
from serving_api.config import (
    DEFAULT_HOST,
    DEFAULT_METRICS_PORT,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_SIZE,
    DEFAULT_PORT,
    DEFAULT_ROUTE_AVERAGE_WEIGHT,
    DEFAULT_ROUTE_WORST_QUARTILE_WEIGHT,
    DEFAULT_ROUTE_WORST_RATIO,
    RouteComfortConfig,
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
    assert config.metrics_port == DEFAULT_METRICS_PORT
    assert config.pool_min_size == DEFAULT_POOL_MIN_SIZE
    assert config.pool_max_size == DEFAULT_POOL_MAX_SIZE


def test_from_env_reads_optional_settings() -> None:
    config = ServingApiConfig.from_env(
        REQUIRED_ENV
        | {
            "SERVING_API_HOST": "127.0.0.1",
            "SERVING_API_PORT": "9000",
            "SERVING_API_METRICS_PORT": "9200",
            "SERVING_API_POOL_MIN_SIZE": "2",
            "SERVING_API_POOL_MAX_SIZE": "20",
        }
    )

    assert config.host == "127.0.0.1"
    assert config.port == 9000
    assert config.metrics_port == 9200
    assert config.pool_min_size == 2
    assert config.pool_max_size == 20


@pytest.mark.parametrize("missing", sorted(REQUIRED_ENV))
def test_from_env_requires_every_postgres_setting(missing: str) -> None:
    env = {key: value for key, value in REQUIRED_ENV.items() if key != missing}

    with pytest.raises(ValueError, match=missing):
        ServingApiConfig.from_env(env)


def test_conninfo_keeps_values_that_need_quoting() -> None:
    # 접속 문자열을 직접 조립했다면 공백이 들어간 비밀번호에서 깨진다.
    config = ServingApiConfig.from_env(
        REQUIRED_ENV | {"POSTGRES_PASSWORD": "two words"}
    )

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


def test_from_env_applies_the_provisional_route_comfort_policy() -> None:
    config = ServingApiConfig.from_env(REQUIRED_ENV)

    assert config.route_comfort == RouteComfortConfig(
        average_weight=DEFAULT_ROUTE_AVERAGE_WEIGHT,
        worst_quartile_weight=DEFAULT_ROUTE_WORST_QUARTILE_WEIGHT,
        worst_ratio=DEFAULT_ROUTE_WORST_RATIO,
    )


def test_from_env_reads_the_route_comfort_policy() -> None:
    # 0.7/0.3/0.25는 검증된 값이 아니라 MVP 잠정값이라 재배포 없이 바꿀 수 있어야 한다.
    config = ServingApiConfig.from_env(
        REQUIRED_ENV
        | {
            "SERVING_API_ROUTE_AVERAGE_WEIGHT": "0.5",
            "SERVING_API_ROUTE_WORST_QUARTILE_WEIGHT": "0.5",
            "SERVING_API_ROUTE_WORST_RATIO": "0.1",
        }
    )

    assert config.route_comfort.average_weight == 0.5
    assert config.route_comfort.worst_quartile_weight == 0.5
    assert config.route_comfort.worst_ratio == 0.1


@pytest.mark.parametrize(
    ("weights", "match"),
    [
        pytest.param(
            {"average_weight": 0.7, "worst_quartile_weight": 0.7}, "sum to 1", id="sum"
        ),
        pytest.param(
            {"average_weight": 1.5, "worst_quartile_weight": -0.5},
            "negative",
            id="negative",
        ),
    ],
)
def test_route_comfort_config_rejects_weights_that_leave_the_0_100_scale(
    weights: dict[str, float], match: str
) -> None:
    # 합이 1이 아니면 0~100을 벗어난 점수가 나오는데, 응답만 봐서는 알기 어렵다.
    with pytest.raises(ValueError, match=match):
        RouteComfortConfig(worst_ratio=0.25, **weights)


@pytest.mark.parametrize("worst_ratio", [0.0, -0.1, 1.5])
def test_route_comfort_config_rejects_an_out_of_range_worst_ratio(
    worst_ratio: float,
) -> None:
    # 0이면 하위 구간이 하나도 없고, 1을 넘으면 경로 길이보다 많은 구간을 뜻한다.
    with pytest.raises(ValueError, match="worst_ratio"):
        RouteComfortConfig(
            average_weight=0.7, worst_quartile_weight=0.3, worst_ratio=worst_ratio
        )
