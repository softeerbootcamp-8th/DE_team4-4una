"""`/openapi.json`에 설명이 빠지지 않았는지 확인한다 (#450).

문구를 통째로 비교하지는 않는다 -- 문장을 다듬을 때마다 테스트가 깨지면 설명을
고치는 것 자체를 꺼리게 된다. "이 정책이 어딘가에 적혀 있는가"만 본다.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from serving_api.app import create_app
from serving_api.config import ServingApiConfig

CONFIG = ServingApiConfig.from_env(
    {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "de4",
        "POSTGRES_USER": "de4",
        "POSTGRES_PASSWORD": "secret",
    }
)

SINGLE = ("/api/v1/segments/{segment_id}/comfort-scores/{vehicle_profile_id}", "get")
BATCH = ("/api/v1/comfort-scores/batch", "post")
ROUTES = ("/api/v1/routes/evaluate", "post")
HEALTH = ("/health", "get")


@pytest.fixture(scope="module")
def openapi() -> dict:
    # 문서 생성은 DB에 닿지 않으므로 풀을 열지 않는다.
    app = create_app(CONFIG, pool_factory=lambda _config: None)
    return TestClient(app).get("/openapi.json").json()


def operation(openapi: dict, endpoint: tuple[str, str]) -> dict:
    path, method = endpoint
    return openapi["paths"][path][method]


def properties(openapi: dict, name: str) -> dict:
    return openapi["components"]["schemas"][name]["properties"]


def test_the_api_and_every_tag_are_described(openapi):
    assert openapi["info"]["summary"] and openapi["info"]["description"]
    assert {tag["name"] for tag in openapi["tags"]} == {"health", "comfort-scores", "routes"}
    assert all(tag["description"] for tag in openapi["tags"])


@pytest.mark.parametrize("endpoint", [HEALTH, SINGLE, BATCH, ROUTES])
def test_every_endpoint_has_a_summary_and_description(openapi, endpoint):
    assert operation(openapi, endpoint)["summary"]
    assert operation(openapi, endpoint)["description"]


@pytest.mark.parametrize(
    ("schema", "field"),
    [
        ("ComfortScore", "segment_id"),
        ("ComfortScore", "source"),
        ("ComfortScore", "weather_time"),
        ("ComfortScore", "confidence_score"),
        ("ComfortScoreResponse", "requested_vehicle_profile_id"),
        ("ComfortScoreResponse", "effective_vehicle_profile_id"),
        ("ComfortScoreResponse", "vehicle_profile_fallback"),
        ("ComfortScoreBatchRequest", "segment_ids"),
        ("ComfortScoreBatchRequest", "vehicle_profile_id"),
        ("ComfortScoreBatchResponse", "not_found_segment_ids"),
        ("RouteCandidate", "route_id"),
        ("RouteCandidate", "segment_ids"),
        ("RouteComfortScore", "comfort_score"),
        ("RouteComfortScore", "average_comfort_score"),
        ("RouteComfortScore", "worst_quartile_comfort_score"),
        ("RouteEvaluationResponse", "recommended_route_id"),
        ("RouteEvaluationResponse", "routes"),
    ],
)
def test_key_fields_are_described(openapi, schema, field):
    assert properties(openapi, schema)[field]["description"]


def test_path_parameters_are_described(openapi):
    described = {p["name"]: p for p in operation(openapi, SINGLE)["parameters"]}

    assert described["segment_id"]["description"]
    assert described["vehicle_profile_id"]["description"]


class TestTryItOutExamples:
    """Swagger UI가 입력칸을 미리 채우려면 예시가 schema가 아니라 파라미터/본문
    레벨에 있어야 한다. Execute만 눌러도 형식이 맞는 요청이 나가야 한다."""

    def test_path_parameters_prefill_the_input(self, openapi):
        for parameter in operation(openapi, SINGLE)["parameters"]:
            assert parameter["examples"], parameter["name"]

    @pytest.mark.parametrize("endpoint", [BATCH, ROUTES])
    def test_request_bodies_prefill_the_editor(self, openapi, endpoint):
        content = operation(openapi, endpoint)["requestBody"]["content"]

        assert content["application/json"]["examples"]

    @pytest.mark.parametrize("endpoint", [BATCH, ROUTES])
    def test_the_body_example_passes_its_own_schema(self, openapi, endpoint):
        """예시가 스키마를 어기면 Execute가 바로 422로 떨어진다."""
        from serving_api.schemas import ComfortScoreBatchRequest, RouteEvaluationRequest

        model = ComfortScoreBatchRequest if endpoint == BATCH else RouteEvaluationRequest
        content = operation(openapi, endpoint)["requestBody"]["content"]
        for example in content["application/json"]["examples"].values():
            model.model_validate(example["value"])


@pytest.mark.parametrize(
    ("endpoint", "expected"),
    [
        (SINGLE, "standard"),  # current가 없으면 standard로 대체
        (BATCH, "중복"),  # 같은 구간은 한 번만 조회
        (BATCH, "순서"),  # 요청 순서 보존
        (BATCH, "not_found_segment_ids"),
        (ROUTES, "내림차순"),  # 점수 정렬
        (ROUTES, "같으면"),  # 동점 처리
        (ROUTES, "404"),  # 누락 구간은 404
    ],
)
def test_policies_are_documented_on_the_endpoint(openapi, endpoint, expected):
    assert expected in operation(openapi, endpoint)["description"]


@pytest.mark.parametrize(
    ("endpoint", "status"),
    [
        (SINGLE, "404"),
        (SINGLE, "422"),
        (SINGLE, "503"),
        (BATCH, "422"),
        (BATCH, "503"),
        (ROUTES, "404"),
        (ROUTES, "422"),
        (ROUTES, "503"),
    ],
)
def test_error_responses_use_the_common_schema(openapi, endpoint, status):
    response = operation(openapi, endpoint)["responses"][status]

    assert response["content"]["application/json"]["schema"]["$ref"].endswith("/ErrorResponse")
    assert response["description"]


def test_the_common_error_schema_matches_the_handler_output(openapi):
    detail = openapi["components"]["schemas"]["ErrorDetail"]

    assert set(detail["required"]) == {"code", "message"}
    # details는 422 핸들러만 싣는다.
    assert "details" not in detail.get("required", [])


def test_health_documents_its_own_503_shape(openapi):
    """`/health`만 공통 오류 형식을 쓰지 않는다 -- 장애를 상태로 보고한다."""
    responses = operation(openapi, HEALTH)["responses"]

    assert responses["503"]["content"]["application/json"]["example"] == {
        "status": "degraded",
        "database": "unavailable",
    }
