from typing import Self

import pytest
from dashboard.serving_api_client import chunk_segment_ids, fetch_comfort_scores


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class FakeClient:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def post(self, endpoint: str, json: dict) -> FakeResponse:
        self.calls.append((endpoint, json))
        return FakeResponse(self.responses.pop(0))


def test_chunk_segment_ids_honors_serving_api_batch_size() -> None:
    segment_ids = [f"{index:07d}" for index in range(2_465)]

    chunks = chunk_segment_ids(segment_ids, batch_size=300)

    assert len(chunks) == 9
    assert [len(chunk) for chunk in chunks] == [300] * 8 + [65]
    assert [value for chunk in chunks for value in chunk] == segment_ids


def test_chunk_segment_ids_deduplicates_without_reordering() -> None:
    assert chunk_segment_ids(["2", "1", "2", "3"], batch_size=2) == [
        ("2", "1"),
        ("3",),
    ]


def test_chunk_segment_ids_rejects_non_positive_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        chunk_segment_ids(["1"], batch_size=0)


def test_fetch_comfort_scores_posts_each_chunk_using_serving_api_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = FakeClient(
        [
            {
                "effective_vehicle_profile_id": 0,
                "vehicle_profile_fallback": False,
                "scores": [
                    {
                        "segment_id": "1",
                        "comfort_score": 81.0,
                        "confidence_score": 0.9,
                        "source": "current",
                        "weather_time": "2026-08-24T00:00:00Z",
                    }
                ],
                "not_found_segment_ids": ["2"],
            },
            {
                "effective_vehicle_profile_id": 0,
                "vehicle_profile_fallback": False,
                "scores": [
                    {
                        "segment_id": "3",
                        "comfort_score": 55.0,
                        "confidence_score": 0.5,
                        "source": "standard",
                        "weather_time": None,
                    }
                ],
                "not_found_segment_ids": [],
            },
        ]
    )
    monkeypatch.setattr(
        "dashboard.serving_api_client.httpx.Client",
        lambda timeout: fake_client,
    )

    result = fetch_comfort_scores(
        endpoint="http://serving-api:8000/api/v1/comfort-scores/batch",
        vehicle_profile_id=0,
        segment_ids=["1", "2", "3"],
        batch_size=2,
        timeout_seconds=10,
    )

    assert [call[1] for call in fake_client.calls] == [
        {"vehicle_profile_id": 0, "segment_ids": ["1", "2"]},
        {"vehicle_profile_id": 0, "segment_ids": ["3"]},
    ]
    assert set(result.scores) == {"1", "3"}
    assert result.not_found_segment_ids == ("2",)
