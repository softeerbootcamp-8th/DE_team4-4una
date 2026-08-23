"""Streamlit assembly for the NYC road comfort-score map."""

from __future__ import annotations

import httpx
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError
from streamlit_folium import st_folium

from dashboard.config import (
    ROAD_SEGMENT_CACHE_TTL_SECONDS,
    SCORE_CACHE_TTL_SECONDS,
    DashboardConfig,
)
from dashboard.map_view import build_map, join_road_segments_with_scores
from dashboard.road_geometry import RoadSegment, load_road_segments
from dashboard.serving_api_client import (
    ComfortScoreBatchResult,
    fetch_comfort_scores,
)


@st.cache_data(ttl=ROAD_SEGMENT_CACHE_TTL_SECONDS, show_spinner=False)
def _load_road_segments_cached(
    road_segment_s3_uri: str,
    aws_region: str | None,
) -> list[RoadSegment]:
    return load_road_segments(road_segment_s3_uri, aws_region)


@st.cache_data(ttl=SCORE_CACHE_TTL_SECONDS, show_spinner=False)
def _load_scores_cached(
    endpoint: str,
    vehicle_profile_id: int,
    segment_ids: tuple[str, ...],
    batch_chunk_size: int,
    timeout_seconds: float,
) -> ComfortScoreBatchResult:
    return fetch_comfort_scores(
        endpoint=endpoint,
        vehicle_profile_id=vehicle_profile_id,
        segment_ids=segment_ids,
        batch_size=batch_chunk_size,
        timeout_seconds=timeout_seconds,
    )


def main() -> None:
    st.set_page_config(page_title="NYC Road Comfort Score", layout="wide")
    st.title("NYC Road Comfort Score Map")
    st.caption(
        "Road geometry comes from the configured S3 snapshot. Comfort scores "
        "come only from the Serving API."
    )

    try:
        config = DashboardConfig.from_env()
        with st.spinner("Loading NYC road segments from S3..."):
            road_segments = _load_road_segments_cached(
                config.road_segment_s3_uri,
                config.aws_region,
            )
        segment_ids = tuple(segment.segment_id for segment in road_segments)
        with st.spinner("Loading comfort scores from the Serving API..."):
            score_result = _load_scores_cached(
                config.batch_endpoint,
                config.vehicle_profile_id,
                segment_ids,
                config.batch_chunk_size,
                config.request_timeout_seconds,
            )
    except ValueError as exc:
        st.error(f"Dashboard configuration or data is invalid: {exc}")
        st.stop()
    except (BotoCoreError, ClientError) as exc:
        st.error(f"Unable to load road segments from S3: {exc}")
        st.stop()
    except httpx.HTTPError as exc:
        st.error(f"Unable to load comfort scores from the Serving API: {exc}")
        st.stop()

    joined_segments = join_road_segments_with_scores(
        road_segments,
        score_result.scores,
    )
    scored_count = len(score_result.scores)
    total_count = len(road_segments)
    missing_count = total_count - scored_count
    coverage = scored_count / total_count * 100 if total_count else 0.0

    metric_columns = st.columns(4)
    metric_columns[0].metric("Road segments", f"{total_count:,}")
    metric_columns[1].metric("Scored", f"{scored_count:,}")
    metric_columns[2].metric("No score", f"{missing_count:,}")
    metric_columns[3].metric("Coverage", f"{coverage:.1f}%")

    if score_result.vehicle_profile_fallback:
        st.warning(
            "Serving API used vehicle profile "
            f"{score_result.effective_vehicle_profile_id} instead of requested "
            f"profile {score_result.requested_vehicle_profile_id}."
        )

    st_folium(
        build_map(joined_segments),
        width=None,
        height=720,
        returned_objects=[],
    )
    st.caption(
        f"Road geometry cache: {ROAD_SEGMENT_CACHE_TTL_SECONDS // 3600}h · "
        f"Comfort score cache: {SCORE_CACHE_TTL_SECONDS // 60}m · "
        f"Serving API chunk size: {config.batch_chunk_size:,}"
    )


if __name__ == "__main__":
    main()
