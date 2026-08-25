import copy

import pytest
from pipeline_perf.render import render
from scenario import build_collector

_SECTIONS = (
    "## 1. 대상 실행과 데이터량",
    "## 2. 타임라인과 오버헤드 대 실제 계산 시간",
    "## 3. task별 상세",
    "## 4. 느린 스테이지 top 10",
    "## 5. 느린 SQL execution top 10",
    "## 6. Spark 밖 구간 (PERF 로그)",
    "## 7. 정규화 지표와 DAG run당 원가",
    "## 8. 관찰된 병목 후보",
)


@pytest.fixture
def document(lake):
    return render([build_collector(lake).collect()])


def test_report_has_all_eight_sections(document):
    assert [section for section in _SECTIONS if section in document] == list(_SECTIONS)


def test_timeline_section_shows_the_overhead_split(document):
    timeline = document.split("## 2.")[1].split("## 3.")[0]

    assert "1:10" in timeline  # 프로비저닝 70초
    assert "0:26" in timeline  # Spark 계산 26초
    assert "4.3%" in timeline  # 계산 비율


def test_slow_stage_section_reports_distribution_and_skew(document):
    stages = document.split("## 4.")[1].split("## 5.")[0]

    assert "count at NativeMethodAccessorImpl.java:0" in stages
    assert "4.00" in stages  # max/p50 skew
    assert "256.0 MiB / 128.0 MiB" in stages  # memory/disk spill


def test_perf_log_section_lists_the_postgres_phase(document):
    perf = document.split("## 6.")[1].split("## 7.")[0]

    assert "standard_score.postgres_merge" in perf
    assert "2:40" in perf
    assert "rows=184213" in perf


def test_observations_are_facts_without_recommendations(document):
    observations = document.split("## 8.")[1]

    assert "Spark 계산 구간은" in observations
    assert "프로비저닝 대기 합" in observations
    assert "spill이 발생한 스테이지" in observations
    # 최적화 방안은 이 리포트의 범위가 아니다(#462 완료 조건).
    for banned in ("해야", "권장", "제안", "줄이면", "개선하려면"):
        assert banned not in observations


def test_empty_collection_still_renders_every_section():
    document = render([{"collected_at": "2026-08-25T00:00:00Z", "dags": []}])

    assert "관찰할 것이 없다" in document
    assert all(section in document for section in _SECTIONS)


def test_missing_event_log_fields_are_disclosed(lake):
    payload = copy.deepcopy(build_collector(lake).collect())
    payload["dags"][0]["runs"][0]["tasks"][1]["spark"]["missing_metrics"] = ["JVM GC Time"]

    document = render([payload])

    assert "event log에서 확인되지 않은 필드: JVM GC Time" in document
