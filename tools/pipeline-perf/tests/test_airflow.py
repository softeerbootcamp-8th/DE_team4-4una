import pytest
from fakes import FakeResponse, FakeSession
from pipeline_perf.airflow import (
    AirflowClient,
    AirflowCredentials,
    flatten_log_content,
    parse_timestamp,
    seconds_between,
    task_gaps,
)


def _client(routes, **kwargs):
    session = FakeSession(routes)
    credentials = AirflowCredentials(base_url="http://airflow:8080", **kwargs)
    return AirflowClient(credentials, session=session), session


def test_password_is_exchanged_for_a_token_once():
    client, session = _client(
        {
            ("POST", "/auth/token"): FakeResponse({"access_token": "jwt-1"}),
            ("GET", "/dagRuns"): FakeResponse({"dag_runs": [{"dag_run_id": "r1"}]}),
        },
        username="admin",
        password="secret",
    )

    client.dag_runs("standard_score_pipeline", 5)
    client.dag_runs("standard_score_pipeline", 5)

    token_calls = [call for call in session.calls if call[0] == "POST"]
    assert len(token_calls) == 1
    assert session.calls[-1][2]["headers"]["Authorization"] == "Bearer jwt-1"


def test_a_supplied_token_skips_the_login_round_trip():
    client, session = _client(
        {("GET", "/dagRuns"): FakeResponse({"dag_runs": []})}, token="jwt-preset"
    )

    client.dag_runs("standard_score_pipeline", 5)

    assert [call[0] for call in session.calls] == ["GET"]


def test_missing_credentials_are_reported_before_any_request():
    client, _ = _client({})

    with pytest.raises(ValueError, match="인증 정보"):
        client.dag_runs("standard_score_pipeline", 5)


def test_dag_runs_filters_on_run_after_not_logical_date():
    """`run_after`가 `dag_run_id`에 박히는 시각이라 "9시 실행"과 일치한다."""
    client, session = _client({("GET", "/dagRuns"): FakeResponse({"dag_runs": []})}, token="jwt")

    client.dag_runs("standard_score_pipeline", 5)
    assert session.calls[-1][2]["params"] == {"limit": 5, "order_by": "-run_after"}

    client.dag_runs(
        "standard_score_pipeline",
        1,
        since="2026-08-25T09:00:00+00:00",
        until="2026-08-25T10:00:00+00:00",
        state="success",
    )
    assert session.calls[-1][2]["params"] == {
        "limit": 1,
        "order_by": "-run_after",
        "run_after_gte": "2026-08-25T09:00:00+00:00",
        "run_after_lte": "2026-08-25T10:00:00+00:00",
        "state": "success",
    }


def test_a_single_dag_run_is_fetched_by_id_and_a_missing_one_is_none():
    client, session = _client(
        {("GET", "/dagRuns/run-1"): FakeResponse({"dag_run_id": "run-1", "state": "success"})},
        token="jwt",
    )

    assert client.dag_run("standard_score_pipeline", "run-1")["dag_run_id"] == "run-1"
    assert session.calls[-1][1].endswith("/dags/standard_score_pipeline/dagRuns/run-1")
    # 라우트가 없으면 404 — 오타 하나 때문에 나머지 수집을 버리지 않는다.
    assert client.dag_run("standard_score_pipeline", "does-not-exist") is None


def test_xcom_value_is_json_decoded():
    client, _ = _client(
        {("GET", "/xcomEntries/return_value"): FakeResponse({"value": '{"rows": 12}'})},
        token="jwt",
    )

    assert client.xcom_value("dag", "run", "task", "return_value") == {"rows": 12}


def test_expired_xcom_is_none_rather_than_an_error():
    client, _ = _client({}, token="jwt")  # 라우트가 없으면 404

    assert client.xcom_value("dag", "run", "task", "return_value") is None


def test_missing_variable_is_none():
    client, _ = _client({}, token="jwt")

    assert client.variable("EMR_SERVERLESS_APPLICATION_ID") is None


def test_from_env_requires_a_base_url(monkeypatch):
    monkeypatch.delenv("AIRFLOW_API_BASE_URL", raising=False)

    with pytest.raises(ValueError, match="base URL"):
        AirflowCredentials.from_env()


def test_task_gaps_measure_the_space_between_consecutive_tasks():
    instances = [
        {"task_id": "b", "start_date": "2026-08-25T02:10:00Z", "end_date": "2026-08-25T02:12:00Z"},
        {"task_id": "a", "start_date": "2026-08-25T02:00:00Z", "end_date": "2026-08-25T02:05:00Z"},
        {"task_id": "never_ran", "start_date": None, "end_date": None},
    ]

    assert task_gaps(instances) == [
        {"from_task_id": "a", "to_task_id": "b", "seconds": 300.0}
    ]


def test_timestamp_helpers_accept_the_z_suffix():
    assert parse_timestamp("2026-08-25T02:00:00Z").hour == 2
    assert parse_timestamp(None) is None
    assert seconds_between("2026-08-25T02:00:00Z", "2026-08-25T02:00:30Z") == 30.0
    assert seconds_between(None, "2026-08-25T02:00:30Z") is None


def test_task_log_content_is_flattened_into_lines():
    client, _ = _client(
        {
            ("GET", "/logs/2"): FakeResponse(
                {
                    "content": [
                        {"event": "::group::Log message source details"},
                        {"event": 'PERF {"phase": "current_score.refresh", "elapsed_s": 3.0}'},
                    ]
                }
            )
        },
        token="jwt",
    )

    text = client.task_log("current_score_pipeline", "run", "run_current_score", 2)

    assert text.splitlines()[1].startswith("PERF ")


def test_plain_text_log_content_passes_through():
    assert flatten_log_content("a\nb") == "a\nb"
    assert flatten_log_content(None) == ""


def test_expired_task_log_is_empty_rather_than_an_error():
    client, _ = _client({}, token="jwt")  # 라우트가 없으면 404

    assert client.task_log("dag", "run", "task", 1) == ""
