"""Trip-level sensor episode detection from processed_sensor_event."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _trip_window() -> Window:
    # 같은 trip 내부에서만 이전 행을 찾도록 정렬한 Window
    return Window.partitionBy("trip_id").orderBy("trip_seq", "event_time", "event_id")


# candidate_condition이 연속 True인 구간을 episode로 묶어, 지속 시간이
# min_duration_seconds 이상이면 시작 행만 output_column=True로 표시한다
def _add_episode_start(
    df: DataFrame,
    candidate_condition: Column,
    output_column: str,
    min_duration_seconds: float,
    max_gap_seconds: float,
) -> DataFrame:
    if max_gap_seconds <= 0:
        raise ValueError("max_gap_seconds must be greater than 0")
    if min_duration_seconds < 0:
        raise ValueError("min_duration_seconds must be non-negative")

    trip_window = _trip_window()
    prev_event_time = "_prev_event_time"
    delta_time_seconds = "_delta_time_seconds"
    candidate_column = "_is_candidate"
    prev_candidate = "_prev_is_candidate"
    # processed_sensor_event에는 이미 lineage 컬럼 _run_id가 있어 이름이 겹치면 안 된다.
    episode_run_id = "_episode_run_id"
    run_duration_seconds = "_run_duration_seconds"

    # timestamp DOUBLE 캐스팅은 상쇄 오차가 있어 unix_micros(정수)로 계산한다.
    with_delta = df.withColumn(prev_event_time, F.lag("event_time").over(trip_window)).withColumn(
        delta_time_seconds,
        (F.unix_micros("event_time") - F.unix_micros(prev_event_time)) / 1_000_000.0,
    )

    is_gap = (
        F.col(delta_time_seconds).isNull()
        | (F.col(delta_time_seconds) <= 0)
        | (F.col(delta_time_seconds) > max_gap_seconds)
    )

    # candidate_condition이 NULL이면(원본 값이 NULL인 경우 등) False로 취급한다
    with_candidate = with_delta.withColumn(
        candidate_column, F.coalesce(candidate_condition, F.lit(False))
    ).withColumn(prev_candidate, F.lag(F.col(candidate_column)).over(trip_window))

    # candidate 값이 바뀌었거나(True<->False), sampling gap으로 연속성이 끊기면 새 run 시작
    is_new_run = (
        F.col(prev_candidate).isNull()
        | (F.col(candidate_column) != F.col(prev_candidate))
        | is_gap
    )
    with_run = with_candidate.withColumn(
        episode_run_id,
        F.sum(F.when(is_new_run, 1).otherwise(0)).over(
            trip_window.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )

    run_window = Window.partitionBy("trip_id", episode_run_id)
    with_duration = with_run.withColumn(
        run_duration_seconds,
        (
            F.max(F.unix_micros("event_time")).over(run_window)
            - F.min(F.unix_micros("event_time")).over(run_window)
        )
        / 1_000_000.0,
    )

    is_episode_start = is_new_run & F.col(candidate_column)
    flag = F.coalesce(
        is_episode_start & (F.col(run_duration_seconds) >= min_duration_seconds),
        F.lit(False),
    )

    return with_duration.withColumn(output_column, flag).drop(
        prev_event_time,
        delta_time_seconds,
        candidate_column,
        prev_candidate,
        episode_run_id,
        run_duration_seconds,
    )


def add_hard_acceleration_event(
    df: DataFrame,
    hard_accel_threshold_mps2: float,
    min_event_duration_seconds: float,
    max_gap_seconds: float,
) -> DataFrame:
    # accel_x >= hard_accel_threshold_mps2가 지속되는 구간을 hard_accel_event_start로 표시
    if hard_accel_threshold_mps2 <= 0:
        raise ValueError("hard_accel_threshold_mps2 must be greater than 0")

    candidate = F.col("accel_x") >= hard_accel_threshold_mps2
    return _add_episode_start(
        df, candidate, "hard_accel_event_start", min_event_duration_seconds, max_gap_seconds
    )


def add_hard_braking_event(
    df: DataFrame,
    hard_brake_threshold_mps2: float,
    min_event_duration_seconds: float,
    max_gap_seconds: float,
) -> DataFrame:
    # accel_x <= hard_brake_threshold_mps2(음수)가 지속되는 구간을 hard_brake_event_start로 표시
    if hard_brake_threshold_mps2 >= 0:
        raise ValueError("hard_brake_threshold_mps2 must be negative")

    candidate = F.col("accel_x") <= hard_brake_threshold_mps2
    return _add_episode_start(
        df, candidate, "hard_brake_event_start", min_event_duration_seconds, max_gap_seconds
    )


def add_sharp_steering_event(
    df: DataFrame,
    sharp_steer_threshold_deg_per_sec: float,
    min_event_duration_seconds: float,
    max_gap_seconds: float,
) -> DataFrame:
    # abs(steering_rate) >= sharp_steer_threshold_deg_per_sec가 지속되는 구간을
    # sharp_steer_event_start로 표시(add_steering_rate로 steering_rate를 먼저 계산해둬야 함)
    if sharp_steer_threshold_deg_per_sec <= 0:
        raise ValueError("sharp_steer_threshold_deg_per_sec must be greater than 0")

    candidate = F.abs(F.col("steering_rate")) >= sharp_steer_threshold_deg_per_sec
    return _add_episode_start(
        df, candidate, "sharp_steer_event_start", min_event_duration_seconds, max_gap_seconds
    )
