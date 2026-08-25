"""steering.py/events.py의 5개 공개 함수와 동일한 결과를, 같은 WindowSpec의 lag/누적합을 묶어 Window 수를 줄여 계산한다(#487)."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from batch_jobs.sensor_features.trip_window import trip_window

# 내부 임시 컬럼 이름 (processed_sensor_event의 실제 lineage 컬럼인 _run_id 등과 겹치지 않는 이름만 사용).
_PREV_STEERING_ANGLE = "_prev_steering_angle"
_PREV_EVENT_TIME = "_prev_event_time"
_STEERING_DELTA_SECONDS = "_steering_delta_time_seconds"

_EVENT_TIME_MICROS = "_event_time_micros"
_EPISODE_DELTA_SECONDS = "_episode_delta_time_seconds"
_IS_EPISODE_GAP = "_is_episode_gap"

_IS_STEERING_CONTINUITY_GAP = "_is_steering_continuity_gap"
_STEERING_DIRECTION = "_steering_direction"
_CONTINUITY_GROUP = "_continuity_group"
_PREV_VALID_STEERING_DIRECTION = "_prev_valid_steering_direction"

_IS_HARD_ACCEL_CANDIDATE = "_is_hard_accel_candidate"
_IS_HARD_BRAKE_CANDIDATE = "_is_hard_brake_candidate"
_IS_SHARP_STEER_CANDIDATE = "_is_sharp_steer_candidate"
_PREV_HARD_ACCEL_CANDIDATE = "_prev_hard_accel_candidate"
_PREV_HARD_BRAKE_CANDIDATE = "_prev_hard_brake_candidate"
_PREV_SHARP_STEER_CANDIDATE = "_prev_sharp_steer_candidate"
_IS_NEW_HARD_ACCEL_RUN = "_is_new_hard_accel_run"
_IS_NEW_HARD_BRAKE_RUN = "_is_new_hard_brake_run"
_IS_NEW_SHARP_STEER_RUN = "_is_new_sharp_steer_run"
_HARD_ACCEL_RUN_ID = "_hard_accel_run_id"
_HARD_BRAKE_RUN_ID = "_hard_brake_run_id"
_SHARP_STEER_RUN_ID = "_sharp_steer_run_id"
_HARD_ACCEL_RUN_DURATION = "_hard_accel_run_duration_seconds"
_HARD_BRAKE_RUN_DURATION = "_hard_brake_run_duration_seconds"
_SHARP_STEER_RUN_DURATION = "_sharp_steer_run_duration_seconds"

_INTERNAL_COLUMNS = (
    _PREV_STEERING_ANGLE,
    _PREV_EVENT_TIME,
    _STEERING_DELTA_SECONDS,
    _EVENT_TIME_MICROS,
    _EPISODE_DELTA_SECONDS,
    _IS_EPISODE_GAP,
    _IS_STEERING_CONTINUITY_GAP,
    _STEERING_DIRECTION,
    _CONTINUITY_GROUP,
    _PREV_VALID_STEERING_DIRECTION,
    _IS_HARD_ACCEL_CANDIDATE,
    _IS_HARD_BRAKE_CANDIDATE,
    _IS_SHARP_STEER_CANDIDATE,
    _PREV_HARD_ACCEL_CANDIDATE,
    _PREV_HARD_BRAKE_CANDIDATE,
    _PREV_SHARP_STEER_CANDIDATE,
    _IS_NEW_HARD_ACCEL_RUN,
    _IS_NEW_HARD_BRAKE_RUN,
    _IS_NEW_SHARP_STEER_RUN,
    _HARD_ACCEL_RUN_ID,
    _HARD_BRAKE_RUN_ID,
    _SHARP_STEER_RUN_ID,
    _HARD_ACCEL_RUN_DURATION,
    _HARD_BRAKE_RUN_DURATION,
    _SHARP_STEER_RUN_DURATION,
)


def add_steering_and_event_features(
    df: DataFrame,
    *,
    steering_max_gap_seconds: float,
    steering_rate_deadband_deg_per_sec: float,
    hard_accel_threshold_mps2: float,
    hard_brake_threshold_mps2: float,
    sharp_steer_threshold_deg_per_sec: float,
    event_max_gap_seconds: float,
    min_event_duration_seconds: float,
    sharp_steer_min_duration_seconds: float,
) -> DataFrame:
    """add_steering_rate -> add_steering_reversal -> add_hard_acceleration_event -> add_hard_braking_event -> add_sharp_steering_event 순서 호출과 동일한 결과를 낸다."""
    if steering_max_gap_seconds <= 0:
        raise ValueError("steering_max_gap_seconds must be greater than 0")
    if steering_rate_deadband_deg_per_sec < 0:
        raise ValueError("steering_rate_deadband_deg_per_sec must be non-negative")
    if hard_accel_threshold_mps2 <= 0:
        raise ValueError("hard_accel_threshold_mps2 must be greater than 0")
    if hard_brake_threshold_mps2 >= 0:
        raise ValueError("hard_brake_threshold_mps2 must be negative")
    if sharp_steer_threshold_deg_per_sec <= 0:
        raise ValueError("sharp_steer_threshold_deg_per_sec must be greater than 0")
    if event_max_gap_seconds <= 0:
        raise ValueError("event_max_gap_seconds must be greater than 0")
    if min_event_duration_seconds < 0:
        raise ValueError("min_event_duration_seconds must be non-negative")
    if sharp_steer_min_duration_seconds < 0:
        raise ValueError("sharp_steer_min_duration_seconds must be non-negative")

    window = trip_window()
    cumulative_window = window.rowsBetween(Window.unboundedPreceding, 0)

    # Window 1: lag(steering_angle)/lag(event_time)을 withColumns()로 한 번에 계산해 재사용한다(체이닝된 withColumn은 병합되지 않음, explain()으로 확인).
    with_lag = df.withColumns(
        {
            _PREV_STEERING_ANGLE: F.lag("steering_angle").over(window),
            _PREV_EVENT_TIME: F.lag("event_time").over(window),
        }
    )

    # --- steering_rate (add_steering_rate와 동일 공식, double cast 유지) ---
    steering_delta = F.col("event_time").cast("double") - F.col(_PREV_EVENT_TIME).cast("double")
    steering_is_missing_input = (
        F.col("steering_angle").isNull()
        | F.col(_PREV_STEERING_ANGLE).isNull()
        | F.col("event_time").isNull()
        | F.col(_PREV_EVENT_TIME).isNull()
    )
    steering_is_invalid_gap = (F.col(_STEERING_DELTA_SECONDS) <= 0) | (
        F.col(_STEERING_DELTA_SECONDS) > steering_max_gap_seconds
    )
    steering_delta_angle = F.col("steering_angle") - F.col(_PREV_STEERING_ANGLE)
    steering_rate = F.when(
        steering_is_missing_input | steering_is_invalid_gap,
        F.lit(None).cast("double"),
    ).otherwise(steering_delta_angle / F.col(_STEERING_DELTA_SECONDS))

    # episode는 double cast가 아닌 unix_micros 기반(둘은 미세하게 다른 값을 내므로 절대 통일하지 않는다).
    episode_delta = (
        F.unix_micros("event_time") - F.unix_micros(F.col(_PREV_EVENT_TIME))
    ) / 1_000_000.0
    is_episode_gap = (
        F.col(_EPISODE_DELTA_SECONDS).isNull()
        | (F.col(_EPISODE_DELTA_SECONDS) <= 0)
        | (F.col(_EPISODE_DELTA_SECONDS) > event_max_gap_seconds)
    )

    with_deltas = (
        with_lag.withColumn(_STEERING_DELTA_SECONDS, steering_delta)
        .withColumn("steering_rate", steering_rate)
        .withColumn(_EVENT_TIME_MICROS, F.unix_micros("event_time"))
        .withColumn(_EPISODE_DELTA_SECONDS, episode_delta)
    )
    with_gap = with_deltas.withColumn(_IS_EPISODE_GAP, is_episode_gap)

    # steering_reversal 선행 계산 + 3개 candidate flag (plain expression이라 Window 불필요).
    is_steering_continuity_gap = F.col("steering_rate").isNull()
    steering_direction = F.when(
        is_steering_continuity_gap
        | (F.abs(F.col("steering_rate")) <= steering_rate_deadband_deg_per_sec),
        F.lit(None).cast("double"),
    ).otherwise(F.signum(F.col("steering_rate")))

    is_hard_accel_candidate = F.coalesce(
        F.col("accel_x") >= hard_accel_threshold_mps2, F.lit(False)
    )
    is_hard_brake_candidate = F.coalesce(
        F.col("accel_x") <= hard_brake_threshold_mps2, F.lit(False)
    )
    is_sharp_steer_candidate = F.coalesce(
        F.abs(F.col("steering_rate")) >= sharp_steer_threshold_deg_per_sec, F.lit(False)
    )

    with_candidates = (
        with_gap.withColumn(_IS_STEERING_CONTINUITY_GAP, is_steering_continuity_gap)
        .withColumn(_STEERING_DIRECTION, steering_direction)
        .withColumn(_IS_HARD_ACCEL_CANDIDATE, is_hard_accel_candidate)
        .withColumn(_IS_HARD_BRAKE_CANDIDATE, is_hard_brake_candidate)
        .withColumn(_IS_SHARP_STEER_CANDIDATE, is_sharp_steer_candidate)
    )

    # Window 2: 3개 candidate의 이전 값(lag)을 withColumns()로 한 Window에 모은다.
    with_prev_candidates = with_candidates.withColumns(
        {
            _PREV_HARD_ACCEL_CANDIDATE: F.lag(F.col(_IS_HARD_ACCEL_CANDIDATE)).over(window),
            _PREV_HARD_BRAKE_CANDIDATE: F.lag(F.col(_IS_HARD_BRAKE_CANDIDATE)).over(window),
            _PREV_SHARP_STEER_CANDIDATE: F.lag(F.col(_IS_SHARP_STEER_CANDIDATE)).over(window),
        }
    )

    def _is_new_run(candidate_column: str, prev_candidate_column: str) -> Column:
        return (
            F.col(prev_candidate_column).isNull()
            | (F.col(candidate_column) != F.col(prev_candidate_column))
            | F.col(_IS_EPISODE_GAP)
        )

    with_new_run_flags = (
        with_prev_candidates.withColumn(
            _IS_NEW_HARD_ACCEL_RUN,
            _is_new_run(_IS_HARD_ACCEL_CANDIDATE, _PREV_HARD_ACCEL_CANDIDATE),
        )
        .withColumn(
            _IS_NEW_HARD_BRAKE_RUN,
            _is_new_run(_IS_HARD_BRAKE_CANDIDATE, _PREV_HARD_BRAKE_CANDIDATE),
        )
        .withColumn(
            _IS_NEW_SHARP_STEER_RUN,
            _is_new_run(_IS_SHARP_STEER_CANDIDATE, _PREV_SHARP_STEER_CANDIDATE),
        )
    )

    # Window 3: continuity_group + 3개 episode run id를 withColumns()로 같은 Window에 묶되, 각 run id의 의미는 독립적으로 유지한다.
    with_run_ids = with_new_run_flags.withColumns(
        {
            _CONTINUITY_GROUP: F.sum(
                F.when(F.col(_IS_STEERING_CONTINUITY_GAP), 1).otherwise(0)
            ).over(cumulative_window),
            _HARD_ACCEL_RUN_ID: F.sum(
                F.when(F.col(_IS_NEW_HARD_ACCEL_RUN), 1).otherwise(0)
            ).over(cumulative_window),
            _HARD_BRAKE_RUN_ID: F.sum(
                F.when(F.col(_IS_NEW_HARD_BRAKE_RUN), 1).otherwise(0)
            ).over(cumulative_window),
            _SHARP_STEER_RUN_ID: F.sum(
                F.when(F.col(_IS_NEW_SHARP_STEER_RUN), 1).otherwise(0)
            ).over(cumulative_window),
        }
    )

    # --- steering_reversal 전용 Window: continuity group별 마지막 유효 방향 (다른 계산과 파티션이 달라 공유 불가) ---
    continuity_preceding_window = trip_window(_CONTINUITY_GROUP).rowsBetween(
        Window.unboundedPreceding, -1
    )
    with_prev_direction = with_run_ids.withColumn(
        _PREV_VALID_STEERING_DIRECTION,
        F.last(F.col(_STEERING_DIRECTION), ignorenulls=True).over(continuity_preceding_window),
    )

    # --- episode별 run duration Window(파티션이 서로 달라 병합 불가, unix_micros 컬럼만 공유) ---
    hard_accel_run_window = Window.partitionBy("trip_id", _HARD_ACCEL_RUN_ID)
    hard_brake_run_window = Window.partitionBy("trip_id", _HARD_BRAKE_RUN_ID)
    sharp_steer_run_window = Window.partitionBy("trip_id", _SHARP_STEER_RUN_ID)

    def _run_duration_seconds(run_window: Window) -> Column:
        return (
            F.max(F.col(_EVENT_TIME_MICROS)).over(run_window)
            - F.min(F.col(_EVENT_TIME_MICROS)).over(run_window)
        ) / 1_000_000.0

    with_durations = (
        with_prev_direction.withColumn(
            _HARD_ACCEL_RUN_DURATION, _run_duration_seconds(hard_accel_run_window)
        )
        .withColumn(_HARD_BRAKE_RUN_DURATION, _run_duration_seconds(hard_brake_run_window))
        .withColumn(_SHARP_STEER_RUN_DURATION, _run_duration_seconds(sharp_steer_run_window))
    )

    # --- 최종 출력 컬럼 (add_steering_reversal/add_hard_*_event/add_sharp_steering_event와 동일 의미) ---
    is_steering_reversal = F.when(
        F.col(_STEERING_DIRECTION).isNull() | F.col(_PREV_VALID_STEERING_DIRECTION).isNull(),
        F.lit(None).cast("boolean"),
    ).otherwise(F.col(_STEERING_DIRECTION) != F.col(_PREV_VALID_STEERING_DIRECTION))

    def _episode_start(
        is_new_run_column: str, candidate_column: str, duration_column: str, min_duration: float
    ) -> Column:
        is_start = F.col(is_new_run_column) & F.col(candidate_column)
        return F.coalesce(is_start & (F.col(duration_column) >= min_duration), F.lit(False))

    result = (
        with_durations.withColumn("is_steering_reversal", is_steering_reversal)
        .withColumn(
            "hard_accel_event_start",
            _episode_start(
                _IS_NEW_HARD_ACCEL_RUN,
                _IS_HARD_ACCEL_CANDIDATE,
                _HARD_ACCEL_RUN_DURATION,
                min_event_duration_seconds,
            ),
        )
        .withColumn(
            "hard_brake_event_start",
            _episode_start(
                _IS_NEW_HARD_BRAKE_RUN,
                _IS_HARD_BRAKE_CANDIDATE,
                _HARD_BRAKE_RUN_DURATION,
                min_event_duration_seconds,
            ),
        )
        .withColumn(
            "sharp_steer_event_start",
            _episode_start(
                _IS_NEW_SHARP_STEER_RUN,
                _IS_SHARP_STEER_CANDIDATE,
                _SHARP_STEER_RUN_DURATION,
                sharp_steer_min_duration_seconds,
            ),
        )
    )

    return result.drop(*_INTERNAL_COLUMNS)
