"""물리 플랜 요약 테스트.

플랜 문자열의 모양은 실제 EMR 8.0.0 / Spark 4.0.2-amzn-0 event log에서 가져왔다.
`description`이 `count at NativeMethodAccessorImpl.java:0`처럼 쓸모없을 때 이
요약이 그 execution의 정체를 설명하는 유일한 근거다.
"""

from pipeline_perf.plan import summarize_physical_plan

PLAN = """== Parsed Logical Plan ==
'Aggregate ['count(1) AS count#8333]
+- Filter isnull(segment_id#4851)

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[], functions=[count(1)], output=[count#8333L])
   +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=4388]
      +- ArrowEvalPython [match_segment(...)], [pythonUDF0#1], 200
         +- Window [max(_w0#4957L) windowspecdefinition(trip_id#274)]
            +- *(23) Sort [trip_id#274 ASC NULLS FIRST], false, 0
               +- FileScan parquet [event_id#272,trip_id#274] Batched: true, \
Location: InMemoryFileIndex(1 paths)[s3://de4-data-lake-473551908409-ap-northeast-2-an/bronze/sensor-events/\
event_date=2026-08-25], PartitionFilters: [], PushedFilters: []
"""


def test_summary_names_the_datasets_without_the_bucket():
    summary = summarize_physical_plan(PLAN)

    assert summary["datasets"] == ["bronze/sensor-events/event_date=2026-08-25"]


def test_summary_keeps_only_operators_that_explain_cost():
    summary = summarize_physical_plan(PLAN)

    # Project/Filter/AdaptiveSparkPlan은 어느 플랜에나 있어 신호가 없다.
    assert summary["operators"] == ["파일 읽기", "윈도우", "정렬", "셔플", "집계", "Python UDF"]


def test_summary_head_starts_at_the_physical_plan():
    head = summarize_physical_plan(PLAN)["head"]

    assert head.splitlines()[0] == "AdaptiveSparkPlan isFinalPlan=false"
    assert "Parsed Logical Plan" not in head


def test_missing_plan_yields_an_empty_summary():
    assert summarize_physical_plan(None) == {"datasets": [], "operators": [], "head": ""}
    assert summarize_physical_plan("")["operators"] == []


def test_plan_without_a_physical_section_is_still_scanned():
    """AQE 갱신처럼 마커가 없는 조각도 버리지 않는다."""
    summary = summarize_physical_plan("HashAggregate(keys=[], functions=[count(1)])")

    assert summary["operators"] == ["집계"]
