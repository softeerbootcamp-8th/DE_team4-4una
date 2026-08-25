"""current_score job의 Spark 밖 구간이 PERF 로그를 남기는지 검증한다 (#461).

current_score_pipeline은 Spark를 쓰지 않고 PythonOperator로 돈다(ADR-0007). Spark
event log가 아예 없으므로 이 job의 시간 분해는 전적으로 이 로그에 의존한다.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

from de4_core import PERF_LOG_PREFIX

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from jobs.current_score import run_current_score_job

# execute_values를 가로채는 autouse fixture — 정의 모듈 밖에서는 명시적으로
# 가져와야 활성화된다.
from test_current_score import (
    ICE_SIGNATURE,
    RULE_CONFIG,
    SNAPSHOT_DATE,
    STANDARD_ROW,
    WEATHER_TIME,
    FakeConnection,
    captured_upserts,  # noqa: F401
    config_for,
    write_road_segment_partition,
)


def _perf_phases(caplog) -> set[str]:
    return {
        json.loads(message[len(PERF_LOG_PREFIX) + 1 :])["phase"]
        for message in caplog.messages
        if message.startswith(f"{PERF_LOG_PREFIX} ")
    }


def test_job_splits_segment_zone_load_from_the_upsert_loop(tmp_path, caplog):
    """S3에서 road_segment를 읽는 시간과 Postgres UPSERT 시간이 갈려야 한다."""
    path = write_road_segment_partition(
        tmp_path, SNAPSHOT_DATE, [("12345", SNAPSHOT_DATE, 76)]
    )
    connection = FakeConnection(
        weather_rows=[(76, WEATHER_TIME, ICE_SIGNATURE)],
        standard_rows=[STANDARD_ROW],
    )

    with caplog.at_level(logging.INFO):
        run_current_score_job(
            config_for(path),
            connection,
            changed_zones_only=False,
            rule_config=RULE_CONFIG,
        )

    assert _perf_phases(caplog) == {
        "current_score.load_segment_zones",
        "current_score.upsert_loop",
    }
