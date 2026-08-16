"""Environment-driven configuration for the sensor event cleansing job."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CleansingJobConfig:
    """Runtime settings for the cleansing job, sourced entirely from the environment."""

    bronze_input_path: str
    processed_output_path: str
    quarantine_output_path: str
    processed_partition_column: str
    quarantine_partition_column: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> CleansingJobConfig:
        # 테스트에서 실제 환경변수를 건드리지 않고 값을 주입할 수 있도록 Mapping을 받는다.
        # 운영 코드는 env를 생략해 os.environ을 그대로 사용한다.
        source = env if env is not None else os.environ
        return cls(
            # stream-processor의 STREAM_BRONZE_OUTPUT_PATH 기본값과 맞춰 로컬에서 그대로 이어진다.
            bronze_input_path=source.get(
                "CLEANSING_BRONZE_INPUT_PATH", "data/local-lake/bronze/sensor-events"
            ),
            processed_output_path=source.get(
                "CLEANSING_SILVER_OUTPUT_PATH", "data/local-lake/silver/processed_sensor_event"
            ),
            quarantine_output_path=source.get(
                "CLEANSING_QUARANTINE_OUTPUT_PATH",
                "data/local-lake/silver/sensor_event_quarantine",
            ),
            # 격리 행의 event_date는 파싱 실패 시 NULL이라 격리 시각에서 파생한 컬럼으로 나눈다.
            processed_partition_column=source.get(
                "CLEANSING_SILVER_PARTITION_COLUMN", "event_date"
            ),
            quarantine_partition_column=source.get(
                "CLEANSING_QUARANTINE_PARTITION_COLUMN", "rejected_date"
            ),
        )
