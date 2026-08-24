"""Environment-driven configuration for the serving API (#160)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
# API port(8000)와 분리한다 — Prometheus는 이 포트로만 접근하고, API 트래픽과
# 같은 포트를 쓰면 /metrics를 공개 API 표면에 노출하게 된다.
DEFAULT_METRICS_PORT = 9101
DEFAULT_POOL_MIN_SIZE = 1
DEFAULT_POOL_MAX_SIZE = 8

# comfort-scores/batch 상한(#414) — Dashboard의 viewport 하나 분량을 담을 수 있게 크게 잡는다.
MAX_COMFORT_SCORE_BATCH_ITEMS = 1000

# 경로 평가 상한. comfort-scores/batch와 분리했고(#414) 값은 기존 그대로다.
MAX_ROUTE_SEGMENTS = 300

# 한 번에 비교할 후보 경로 수 상한. 내비게이션이 제시하는 후보는 보통 서너 개다.
MAX_ROUTES_PER_REQUEST = 10

# 경로 승차감 점수의 잠정(provisional) 정책값이다 (#269). 검증된 수치가 아니라
# MVP에서 정한 값이므로 코드에 박지 않고 환경 변수로 덮어쓸 수 있게 둔다.
DEFAULT_ROUTE_AVERAGE_WEIGHT = 0.7
DEFAULT_ROUTE_WORST_QUARTILE_WEIGHT = 0.3
DEFAULT_ROUTE_WORST_RATIO = 0.25

# 가중치 합을 부동소수점으로 비교할 때 쓰는 허용 오차. 0.7 + 0.3처럼 정확히
# 1이 되지 않는 조합을 설정 오류로 오인하지 않기 위한 값이다.
_WEIGHT_SUM_TOLERANCE = 1e-9


@dataclass(frozen=True, slots=True)
class RouteComfortConfig:
    """후보 경로 하나를 점수로 환산할 때 쓰는 정책값.

    최종 점수는 `average_weight x 전체 평균 + worst_quartile_weight x 하위
    구간 평균`이고, `worst_ratio`는 '하위 구간'으로 볼 비율이다.
    """

    average_weight: float
    worst_quartile_weight: float
    worst_ratio: float

    def __post_init__(self) -> None:
        # 두 가중치의 합이 1이 아니면 결과가 0~100 밖으로 나간다. 응답을 받은
        # 뒤에는 알아채기 어려우므로 기동 시점에 막는다.
        weight_sum = self.average_weight + self.worst_quartile_weight
        if abs(weight_sum - 1.0) > _WEIGHT_SUM_TOLERANCE:
            raise ValueError(f"route comfort weights must sum to 1, got {weight_sum}")
        if self.average_weight < 0 or self.worst_quartile_weight < 0:
            raise ValueError("route comfort weights must not be negative")
        if not 0 < self.worst_ratio <= 1:
            raise ValueError(
                f"route comfort worst_ratio must be in (0, 1], got {self.worst_ratio}"
            )

    @classmethod
    def from_env(cls, source: Mapping[str, str]) -> RouteComfortConfig:
        return cls(
            average_weight=float(
                source.get("SERVING_API_ROUTE_AVERAGE_WEIGHT")
                or DEFAULT_ROUTE_AVERAGE_WEIGHT
            ),
            worst_quartile_weight=float(
                source.get("SERVING_API_ROUTE_WORST_QUARTILE_WEIGHT")
                or DEFAULT_ROUTE_WORST_QUARTILE_WEIGHT
            ),
            worst_ratio=float(
                source.get("SERVING_API_ROUTE_WORST_RATIO") or DEFAULT_ROUTE_WORST_RATIO
            ),
        )


@dataclass(frozen=True, slots=True)
class ServingApiConfig:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    host: str
    port: int
    metrics_port: int
    pool_min_size: int
    pool_max_size: int
    route_comfort: RouteComfortConfig

    @property
    def conninfo(self) -> str:
        # 문자열을 직접 조립하지 않고 make_conninfo를 쓴다 — 비밀번호에 공백이나
        # 작은따옴표가 들어가면 수동 조립은 조용히 잘못된 접속 문자열을 만든다.
        #
        # 세션 타임존을 UTC로 고정한다. timestamptz는 세션 타임존으로 변환되어
        # 돌아오므로, 고정하지 않으면 같은 행이 서버 로컬 타임존에 따라 다른
        # 오프셋으로 직렬화된다 (KST 로컬에서는 +09:00, UTC 컨테이너에서는 Z).
        return make_conninfo(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
            options="-c timezone=UTC",
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ServingApiConfig:
        source = env if env is not None else os.environ
        return cls(
            postgres_host=_require(source, "POSTGRES_HOST"),
            postgres_port=int(_require(source, "POSTGRES_PORT")),
            postgres_db=_require(source, "POSTGRES_DB"),
            postgres_user=_require(source, "POSTGRES_USER"),
            postgres_password=_require(source, "POSTGRES_PASSWORD"),
            host=source.get("SERVING_API_HOST") or DEFAULT_HOST,
            port=int(source.get("SERVING_API_PORT") or DEFAULT_PORT),
            metrics_port=int(
                source.get("SERVING_API_METRICS_PORT") or DEFAULT_METRICS_PORT
            ),
            pool_min_size=int(
                source.get("SERVING_API_POOL_MIN_SIZE") or DEFAULT_POOL_MIN_SIZE
            ),
            pool_max_size=int(
                source.get("SERVING_API_POOL_MAX_SIZE") or DEFAULT_POOL_MAX_SIZE
            ),
            route_comfort=RouteComfortConfig.from_env(source),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value
