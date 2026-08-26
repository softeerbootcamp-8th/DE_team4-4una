from __future__ import annotations

import pytest
from ops_agent.prometheus_client import (
    STREAM_PROCESSOR_HEALTH,
    STREAM_PROCESSOR_STATUS_QUERY,
    HealthCheck,
    PrometheusClient,
    PrometheusQueryError,
)


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self._response = response
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self._response


def success_payload(result: list[dict]) -> dict:
    return {"status": "success", "data": {"resultType": "vector", "result": result}}


class TestInstantQuery:
    def test_sends_the_query_to_the_prometheus_http_api(self):
        session = FakeSession(FakeResponse(success_payload([])))
        client = PrometheusClient("http://prometheus:9090", session=session)

        client.instant_query("up")

        assert session.calls[0]["url"] == "http://prometheus:9090/api/v1/query"
        assert session.calls[0]["params"] == {"query": "up"}

    def test_a_non_success_status_raises(self):
        session = FakeSession(FakeResponse({"status": "error", "error": "bad query"}))
        client = PrometheusClient("http://prometheus:9090", session=session)

        with pytest.raises(PrometheusQueryError):
            client.instant_query("up")


class TestStreamProcessorStatus:
    def test_uses_the_same_expression_as_the_spark_streaming_dashboard(self):
        # spark-streaming.json의 "Component Status" 패널 expr과 문자 그대로 같아야 한다(#437).
        import json
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[3]
        dashboard_path = (
            repo_root
            / "infra/monitoring/grafana/dashboards/spark-streaming.json"
        )
        dashboard = json.loads(dashboard_path.read_text())
        panel = next(p for p in dashboard["panels"] if p.get("title") == "Component Status")

        assert panel["targets"][0]["expr"] == STREAM_PROCESSOR_STATUS_QUERY

    def test_no_results_means_unknown_status_not_healthy(self):
        session = FakeSession(FakeResponse(success_payload([])))
        client = PrometheusClient("http://prometheus:9090", session=session)

        status = client.evaluate(STREAM_PROCESSOR_HEALTH)

        assert status.code is None
        assert status.is_healthy is False

    def test_a_healthy_result_is_reported_as_running(self):
        session = FakeSession(
            FakeResponse(
                success_payload(
                    [{"metric": {"instance": "spark-ec2:9103"}, "value": [1000, "0"]}]
                )
            )
        )
        client = PrometheusClient("http://prometheus:9090", session=session)

        status = client.evaluate(STREAM_PROCESSOR_HEALTH)

        assert status.is_healthy is True
        assert status.label == "RUNNING"
        assert status.instance == "spark-ec2:9103"

    def test_the_worst_instance_wins_when_there_are_several(self):
        session = FakeSession(
            FakeResponse(
                success_payload(
                    [
                        {"metric": {"instance": "spark-ec2-1:9103"}, "value": [1000, "0"]},
                        {"metric": {"instance": "spark-ec2-2:9103"}, "value": [1000, "4"]},
                    ]
                )
            )
        )
        client = PrometheusClient("http://prometheus:9090", session=session)

        status = client.evaluate(STREAM_PROCESSOR_HEALTH)

        assert status.code == 4
        assert status.label == "TARGET DOWN"
        assert status.instance == "spark-ec2-2:9103"


class TestEvaluate:
    def test_it_maps_the_value_through_the_checks_label_table(self):
        session = FakeSession(
            FakeResponse(success_payload([{"metric": {"instance": "a:1"}, "value": [0, "4"]}]))
        )
        client = PrometheusClient("http://prometheus:9090", session=session)
        check = HealthCheck(query="up", labels={0: "UP", 4: "TARGET DOWN"}, healthy_code=0)

        status = client.evaluate(check)

        assert status.code == 4
        assert status.label == "TARGET DOWN"
        assert status.is_healthy is False
        assert session.calls[0]["params"] == {"query": "up"}

    def test_no_data_is_never_reported_as_healthy(self):
        session = FakeSession(FakeResponse(success_payload([])))
        client = PrometheusClient("http://prometheus:9090", session=session)

        status = client.evaluate(HealthCheck(query="up", labels={0: "UP"}, healthy_code=0))

        assert status.code is None
        assert status.label == "NO DATA"
        assert status.is_healthy is False

    def test_the_worst_instance_wins_when_several_match(self):
        session = FakeSession(
            FakeResponse(
                success_payload(
                    [
                        {"metric": {"instance": "a:1"}, "value": [0, "1"]},
                        {"metric": {"instance": "b:1"}, "value": [0, "4"]},
                    ]
                )
            )
        )
        client = PrometheusClient("http://prometheus:9090", session=session)
        check = HealthCheck(query="up", labels={1: "STALE", 4: "TARGET DOWN"}, healthy_code=0)

        status = client.evaluate(check)

        assert status.code == 4
        assert status.instance == "b:1"

    def test_the_stream_processor_check_reuses_the_dashboard_query(self):
        assert STREAM_PROCESSOR_HEALTH.query == STREAM_PROCESSOR_STATUS_QUERY
        assert STREAM_PROCESSOR_HEALTH.healthy_code == 0
