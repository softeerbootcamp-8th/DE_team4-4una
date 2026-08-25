"""Environment-driven configuration for the road-comfort dashboard."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

# Streamlit이 쓰던 주소와 포트를 그대로 유지한다 -- 배포 스크립트와 health
# check가 이 포트를 가리킨다.
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8501

DEFAULT_SERVING_API_URL = "http://localhost:8000"
# 요청에 프로필이 없을 때 쓰는 값이다. 고정값이 아니라 기본값이다.
DEFAULT_VEHICLE_PROFILE_ID = 0

# 0005_define_vehicle_profiles.sql의 vehicle_profile 행을 사람이 읽을 이름으로
# 옮긴 것. Serving API는 id만 주고받고 프로필 목록을 노출하지 않아 이름을 받아올
# 곳이 없다 -- 프로필이 추가되면 이 목록도 함께 고쳐야 한다.
#
# 목록이 뒤처져도 조용히 틀리지는 않는다. 없거나 비활성인 id를 보내면 Serving
# API가 프로필 0으로 내려주고 vehicle_profile_fallback으로 알려주므로, 화면에
# 대체됐다는 경고가 뜬다.
VEHICLE_PROFILES: tuple[tuple[int, str], ...] = (
    (0, "All vehicles"),
    (1, "Sedan, compact"),
    (2, "Sedan, large"),
    (3, "SUV, compact"),
    (4, "SUV, large"),
    (5, "MPV, large"),
)

# serving API의 MAX_COMFORT_SCORE_BATCH_ITEMS를 그대로 따른다(서비스 간 import는
# 안 하므로 값만 맞춰둔다).
DEFAULT_BATCH_CHUNK_SIZE = 1000
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0

# 첫 chunk 이후는 동시에 조회한다. 큰 viewport가 serving API의 작은 커넥션
# 풀을 넘치게 하지 않도록 상한을 둔다.
DEFAULT_MAX_PARALLEL_REQUESTS = 8

ROAD_SEGMENT_CACHE_TTL_SECONDS = 24 * 60 * 60

# Serving API 점수를 다시 조회하기까지의 시간. current_score_pipeline이
# zone_weather_pipeline(*/15 * * * *)의 Asset으로 깨어나므로 점수 자체가 15분
# 주기로 갱신된다. 이보다 짧게 잡으면 아직 없는 새 값을 찾아 헛조회를 한다.
# 대시보드 안에서만 쓰는 값이라 파이프라인 주기와는 무관하게 조정할 수 있다.
SCORE_CACHE_TTL_SECONDS = 15 * 60

# 응답 캐시에 남겨둘 (프로필, borough) 조합 수. borough 하나가 gzip 후 4MB대라
# 이 값이 곧 메모리 상한이 된다(약 50MB). 상한이 없으면 프로필 6개 x borough
# 5개를 전부 들고 있게 되어 130MB까지 늘어난다.
PAYLOAD_CACHE_MAX_ENTRIES = 12

NYC_MAP_CENTER = (40.7128, -74.0060)
NYC_MAP_ZOOM = 11

# Zoom used after a borough is picked. A borough covers a small part of the
# city, so staying at the city-wide zoom would leave the selection a speck.
BOROUGH_MAP_ZOOM = 12

# Sentinel for "no borough picked": the map then shows borough outlines rather
# than road segments, which is also what makes the first render cheap.
ALL_BOROUGHS = "All boroughs"

# borough를 고르면 그 안의 segment를 전부 그린다. 이 상한은 zone master가 없어
# borough 개념 자체가 없는 배포에서만 걸린다 -- 그때는 기준이 스냅샷 전체(16만 건,
# 응답 20MB 이상)뿐이라 그대로 내려보낼 수 없다.
SNAPSHOT_FALLBACK_MAX_SEGMENTS = 1000


@dataclass(frozen=True, slots=True)
class DashboardConfig:
    """Configuration needed by the dashboard.

    Database settings are intentionally absent. Comfort scores always cross the
    serving API boundary.
    """

    road_segment_s3_uri: str
    zone_master_s3_uri: str | None
    serving_api_url: str
    vehicle_profile_id: int
    batch_chunk_size: int
    request_timeout_seconds: float
    aws_region: str | None

    @property
    def batch_endpoint(self) -> str:
        return f"{self.serving_api_url.rstrip('/')}/api/v1/comfort-scores/batch"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> DashboardConfig:
        source = env if env is not None else os.environ
        road_segment_s3_uri = _require(source, "DASHBOARD_ROAD_SEGMENT_S3_URI")
        if not road_segment_s3_uri.startswith("s3://"):
            raise ValueError("DASHBOARD_ROAD_SEGMENT_S3_URI must start with s3://")

        # Optional: the map still works without it, minus the borough filter.
        zone_master_s3_uri = source.get("DASHBOARD_ZONE_MASTER_S3_URI") or None
        if zone_master_s3_uri and not zone_master_s3_uri.startswith("s3://"):
            raise ValueError("DASHBOARD_ZONE_MASTER_S3_URI must start with s3://")

        vehicle_profile_id = int(
            source.get("DASHBOARD_VEHICLE_PROFILE_ID") or DEFAULT_VEHICLE_PROFILE_ID
        )
        if vehicle_profile_id < 0:
            raise ValueError("DASHBOARD_VEHICLE_PROFILE_ID must be >= 0")

        batch_chunk_size = int(
            source.get("DASHBOARD_BATCH_CHUNK_SIZE") or DEFAULT_BATCH_CHUNK_SIZE
        )
        if batch_chunk_size < 1:
            raise ValueError("DASHBOARD_BATCH_CHUNK_SIZE must be >= 1")

        request_timeout_seconds = float(
            source.get("DASHBOARD_REQUEST_TIMEOUT_SECONDS")
            or DEFAULT_REQUEST_TIMEOUT_SECONDS
        )
        if request_timeout_seconds <= 0:
            raise ValueError("DASHBOARD_REQUEST_TIMEOUT_SECONDS must be > 0")

        return cls(
            road_segment_s3_uri=road_segment_s3_uri,
            zone_master_s3_uri=zone_master_s3_uri,
            serving_api_url=(
                source.get("DASHBOARD_SERVING_API_URL") or DEFAULT_SERVING_API_URL
            ),
            vehicle_profile_id=vehicle_profile_id,
            batch_chunk_size=batch_chunk_size,
            request_timeout_seconds=request_timeout_seconds,
            aws_region=(source.get("AWS_REGION") or source.get("AWS_DEFAULT_REGION")),
        )


def _require(source: Mapping[str, str], key: str) -> str:
    value = source.get(key)
    if not value:
        raise ValueError(f"{key} must be set")
    return value
