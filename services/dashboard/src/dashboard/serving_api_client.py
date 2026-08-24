"""Fetch comfort scores from the serving API in contract-sized chunks."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import httpx

from dashboard.config import DEFAULT_MAX_PARALLEL_REQUESTS


@dataclass(frozen=True, slots=True)
class ComfortScore:
    segment_id: str
    comfort_score: float
    confidence_score: float
    source: str
    weather_time: str | None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ComfortScore:
        return cls(
            segment_id=str(payload["segment_id"]),
            comfort_score=float(payload["comfort_score"]),
            confidence_score=float(payload["confidence_score"]),
            source=str(payload["source"]),
            weather_time=(
                str(payload["weather_time"])
                if payload.get("weather_time") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class ComfortScoreBatchResult:
    scores: dict[str, ComfortScore]
    not_found_segment_ids: tuple[str, ...]
    requested_vehicle_profile_id: int
    effective_vehicle_profile_id: int
    vehicle_profile_fallback: bool


def chunk_segment_ids(
    segment_ids: Iterable[str], batch_size: int
) -> list[tuple[str, ...]]:
    """Deduplicate IDs while retaining order, then split them into batches."""
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    unique_ids = list(dict.fromkeys(segment_ids))
    return [
        tuple(unique_ids[start : start + batch_size])
        for start in range(0, len(unique_ids), batch_size)
    ]


def fetch_comfort_scores(
    endpoint: str,
    vehicle_profile_id: int,
    segment_ids: Iterable[str],
    batch_size: int,
    timeout_seconds: float,
    max_parallel_requests: int = DEFAULT_MAX_PARALLEL_REQUESTS,
) -> ComfortScoreBatchResult:
    """Fetch all scores without duplicating the API's current/standard policy.

    The API caps a request at a few hundred segments, so a map-sized query turns
    into many of them and the round trips, not the queries, dominate. They are
    issued concurrently for that reason; the cap keeps the fan-out from
    exhausting the API's own connection pool. Responses are merged in request
    order so the result does not depend on which one returns first.
    """
    batches = chunk_segment_ids(segment_ids, batch_size)
    scores: dict[str, ComfortScore] = {}
    not_found: list[str] = []
    effective_profile_id: int | None = None
    profile_fallback: bool | None = None

    with httpx.Client(timeout=timeout_seconds) as client:

        def _post(batch: tuple[str, ...]) -> dict[str, Any]:
            response = client.post(
                endpoint,
                json={
                    "vehicle_profile_id": vehicle_profile_id,
                    "segment_ids": list(batch),
                },
            )
            response.raise_for_status()
            return response.json()

        if not batches:
            payloads: list[dict[str, Any]] = []
        elif len(batches) == 1:
            payloads = [_post(batches[0])]
        else:
            with ThreadPoolExecutor(
                max_workers=min(max_parallel_requests, len(batches))
            ) as pool:
                payloads = list(pool.map(_post, batches))

        for batch, payload in zip(batches, payloads, strict=True):
            chunk_effective_profile_id = int(payload["effective_vehicle_profile_id"])
            chunk_profile_fallback = bool(payload["vehicle_profile_fallback"])
            if effective_profile_id is None:
                effective_profile_id = chunk_effective_profile_id
                profile_fallback = chunk_profile_fallback
            elif (
                effective_profile_id != chunk_effective_profile_id
                or profile_fallback != chunk_profile_fallback
            ):
                raise ValueError(
                    "serving API vehicle-profile metadata changed between chunks"
                )

            requested_ids = set(batch)
            for score_payload in payload["scores"]:
                score = ComfortScore.from_payload(score_payload)
                if score.segment_id not in requested_ids:
                    raise ValueError(
                        "serving API returned an unrequested segment_id: "
                        f"{score.segment_id}"
                    )
                if score.segment_id in scores:
                    raise ValueError(
                        f"serving API returned duplicate segment_id={score.segment_id}"
                    )
                scores[score.segment_id] = score
            not_found.extend(str(value) for value in payload["not_found_segment_ids"])

    return ComfortScoreBatchResult(
        scores=scores,
        not_found_segment_ids=tuple(not_found),
        requested_vehicle_profile_id=vehicle_profile_id,
        effective_vehicle_profile_id=(
            effective_profile_id
            if effective_profile_id is not None
            else vehicle_profile_id
        ),
        vehicle_profile_fallback=(
            profile_fallback if profile_fallback is not None else False
        ),
    )
