import os
import time
from datetime import UTC, date, datetime

import pytest
from batch_jobs.schemas import HOURLY_SEGMENT_FEATURE_SCHEMA
from batch_jobs.sensor_features.aggregation import (
    build_hourly_segment_features,
    validate_hourly_segment_features,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# collect()가 돌려주는 timestamp는 이 파이썬 프로세스의 로컬 타임존으로 변환되어,
# 고정하지 않으면 실행 머신마다(Asia/Seoul vs UTC) 값이 달라진다.
os.environ["TZ"] = "UTC"
time.tzset()

SNAPSHOT = date(2026, 8, 11)
RUN_ID = "run-1"
FEATURE_VERSION = "v1"
PROCESSED_AT = datetime(2026, 8, 11, 12, 0, 0, tzinfo=UTC)

SENSOR_SCHEMA = StructType(
    [
        StructField("event_time", TimestampType(), nullable=False),
        StructField("segment_id", StringType(), nullable=True),
        StructField("vehicle_profile_id", IntegerType(), nullable=False),
        StructField("trip_id", StringType(), nullable=True),
        StructField("road_snapshot_date", DateType(), nullable=True),
        StructField("speed_mps", DoubleType(), nullable=True),
        StructField("accel_x", DoubleType(), nullable=True),
        StructField("accel_y", DoubleType(), nullable=True),
        StructField("accel_z", DoubleType(), nullable=True),
        StructField("jerk_x", DoubleType(), nullable=True),
        StructField("jerk_y", DoubleType(), nullable=True),
        StructField("jerk_z", DoubleType(), nullable=True),
        StructField("steering_rate", DoubleType(), nullable=True),
        StructField("steering_vibration", DoubleType(), nullable=True),
        StructField("hard_brake_event_start", BooleanType(), nullable=True),
        StructField("hard_accel_event_start", BooleanType(), nullable=True),
        StructField("sharp_steer_event_start", BooleanType(), nullable=True),
        StructField("is_steering_reversal", BooleanType(), nullable=True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸린다.
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def event_time(minute: int = 0, hour: int = 10) -> datetime:
    return datetime(2026, 8, 11, hour, minute, 0, tzinfo=UTC)


def sensor_row(
    minute: int = 0,
    trip_id: str = "T1",
    road_snapshot_date: date = SNAPSHOT,
    hard_accel_event_start: bool = False,
) -> tuple:
    return (
        event_time(minute),
        "S1",
        1,
        trip_id,
        road_snapshot_date,
        10.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        False,
        hard_accel_event_start,
        False,
        False,
    )


def sensor_df(spark, rows: list[tuple]):
    return spark.createDataFrame(rows, SENSOR_SCHEMA)


def test_build_combines_statistics_and_event_counts_into_canonical_schema(spark) -> None:
    rows = [
        sensor_row(0, hard_accel_event_start=True),
        sensor_row(1, trip_id="T2"),
    ]

    result = build_hourly_segment_features(
        sensor_df(spark, rows),
        feature_version=FEATURE_VERSION,
        run_id=RUN_ID,
        processed_at=PROCESSED_AT,
    )

    expected_fields = HOURLY_SEGMENT_FEATURE_SCHEMA.fields
    assert result.columns == [field.name for field in expected_fields]
    actual_types = {field.name: field.dataType for field in result.schema.fields}
    assert all(actual_types[field.name] == field.dataType for field in expected_fields)

    rows = result.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["avg_speed_mps"] == pytest.approx(10.0)
    assert row["sample_count"] == 2
    assert row["trip_count"] == 2
    assert row["hard_accel_count"] == 1
    assert row["road_snapshot_date"] == SNAPSHOT
    assert row["feature_version"] == FEATURE_VERSION
    assert row["_run_id"] == RUN_ID


def test_conflicting_road_snapshot_date_for_same_pk_is_rejected(spark) -> None:
    # aggregate_hourly_event_counts는 road_snapshot_date별로도 나뉘어 집계되므로, 같은
    # 시간·Segment·차량 프로필에 서로 다른 snapshot이 섞이면 PK가 중복되어야 한다.
    rows = [
        sensor_row(0, road_snapshot_date=SNAPSHOT),
        sensor_row(1, road_snapshot_date=date(2026, 8, 12)),
    ]

    with pytest.raises(ValueError, match="duplicate"):
        build_hourly_segment_features(
            sensor_df(spark, rows),
            feature_version=FEATURE_VERSION,
            run_id=RUN_ID,
            processed_at=PROCESSED_AT,
        )


def feature_row(**overrides: object) -> dict[str, object]:
    row = {
        "segment_id": "S1",
        "vehicle_profile_id": 1,
        "data_period_start": event_time(0),
        "data_period_end": event_time(0).replace(hour=11),
        "road_snapshot_date": SNAPSHOT,
        "avg_speed_mps": 10.0,
        "rms_accel_x": 1.0,
        "rms_accel_y": 1.0,
        "rms_accel_z": 1.0,
        "p95_abs_accel_x": 1.0,
        "p95_abs_accel_y": 1.0,
        "p95_abs_accel_z": 1.0,
        "rms_jerk_x": 1.0,
        "rms_jerk_y": 1.0,
        "rms_jerk_z": 1.0,
        "p95_abs_jerk_x": 1.0,
        "p95_abs_jerk_y": 1.0,
        "p95_abs_jerk_z": 1.0,
        "hard_brake_count": 0,
        "hard_accel_count": 0,
        "sharp_steer_count": 0,
        "steer_reversal_count": 0,
        "rms_steering_rate": 1.0,
        "rms_steering_vibration": 1.0,
        "sample_count": 10,
        "trip_count": 2,
        "feature_version": FEATURE_VERSION,
        "_processed_at": PROCESSED_AT,
        "_run_id": RUN_ID,
    }
    row.update(overrides)
    return row


# 검증 대상 컬럼 순서는 항상 스키마와 같아야 하므로, dict 대신 스키마 순서 그대로 tuple을 만든다
def feature_rows_df(spark, rows: list[dict[str, object]]):
    ordered = [
        tuple(row[field.name] for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields) for row in rows
    ]
    # nullable=False인 컬럼도 테스트에서는 의도적으로 NULL을 넣어봐야 하므로 전부 nullable로 둔다
    nullable_schema = StructType(
        [
            StructField(field.name, field.dataType, nullable=True)
            for field in HOURLY_SEGMENT_FEATURE_SCHEMA.fields
        ]
    )
    return spark.createDataFrame(ordered, nullable_schema)


def test_valid_output_passes_validation(spark) -> None:
    df = feature_rows_df(spark, [feature_row()])

    validate_hourly_segment_features(df)


def test_cache_is_released_after_successful_validation(spark) -> None:
    df = feature_rows_df(spark, [feature_row()])

    validate_hourly_segment_features(df)

    assert df.storageLevel.useMemory is False


def test_cache_is_released_after_validation_error(spark) -> None:
    df = feature_rows_df(spark, [feature_row(), feature_row()])  # 중복 PK -> 검증 실패

    with pytest.raises(ValueError, match="duplicate"):
        validate_hourly_segment_features(df)

    assert df.storageLevel.useMemory is False


@pytest.mark.parametrize(
    "build_df, match",
    [
        (lambda spark: feature_rows_df(spark, [feature_row(segment_id=None)]), "NULL"),
        (
            lambda spark: feature_rows_df(spark, [feature_row(data_period_end=event_time(0))]),
            "one hour",
        ),
        (lambda spark: feature_rows_df(spark, [feature_row(hard_brake_count=-1)]), "invalid count"),
        (
            lambda spark: feature_rows_df(spark, [feature_row(sample_count=1, trip_count=2)]),
            "invalid count",
        ),
        (
            lambda spark: feature_rows_df(spark, [feature_row(sample_count=1, hard_brake_count=2)]),
            "invalid count",
        ),
        (lambda spark: feature_rows_df(spark, [feature_row(), feature_row()]), "duplicate"),
        (
            lambda spark: feature_rows_df(spark, [feature_row()]).drop("feature_version"),
            "do not match the canonical schema",
        ),
    ],
    ids=[
        "null-required",
        "bad-period-end",
        "negative-count",
        "trip>sample",
        "event>sample",
        "duplicate-pk",
        "column-mismatch",
    ],
)
def test_rejects_invalid_rows(spark, build_df, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        validate_hourly_segment_features(build_df(spark))
