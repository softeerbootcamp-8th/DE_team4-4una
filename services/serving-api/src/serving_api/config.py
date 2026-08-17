"""Environment-driven configuration for the serving API (#160)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_POOL_MIN_SIZE = 1
DEFAULT_POOL_MAX_SIZE = 8

# 다건 조회 한 번에 담을 수 있는 (segment_id, vehicle_profile_id) 조합 수 상한.
# 경로 하나가 세그먼트 수백 개로 이뤄질 수 있어 300으로 둔다. 상한이 없으면
# 요청 하나가 커넥션을 오래 점유해 다른 요청까지 대기시킨다.
MAX_BATCH_ITEMS = 300


@dataclass(frozen=True, slots=True)
class ServingApiConfig:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    host: str
    port: int
    pool_min_size: int
    pool_max_size: int

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
            pool_min_size=int(
                source.get("SERVING_API_POOL_MIN_SIZE") or DEFAULT_POOL_MIN_SIZE
            ),
            pool_max_size=int(
                source.get("SERVING_API_POOL_MAX_SIZE") or DEFAULT_POOL_MAX_SIZE
            ),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value
