from datetime import UTC, datetime

from batch_jobs.cleansing.reader import read_bronze_sensor_events
from batch_jobs.cleansing.rules import load_cleansing_config
from batch_jobs.cleansing.validate import OUT_OF_RANGE, split_out_of_range_values
from bronze_samples import valid_value, write_bronze_parquet

RUN_ID = "cleansing-20260814-001"
REJECTED_AT = datetime(2026, 8, 14, 12, 0, 0, tzinfo=UTC)


def split(spark, path):
    bronze = read_bronze_sensor_events(spark, path)
    return split_out_of_range_values(bronze, load_cleansing_config(), RUN_ID, REJECTED_AT)


def test_value_above_its_maximum_is_quarantined(spark, tmp_path):
    # 상한을 넘은 속도가 OUT_OF_RANGE 사유로 격리되는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(), valid_value(speed_mps=99.0))

    result = split(spark, path)

    assert [row["reject_reason"] for row in result.quarantined.collect()] == [OUT_OF_RANGE]
    assert len(result.passed.collect()) == 1


def test_negative_direction_values_are_not_quarantined(spark, tmp_path):
    # 가속도, jerk, 조향각, 경도의 음수는 방향을 담은 정상 값이라 격리되지 않는다.
    path = write_bronze_parquet(
        spark,
        tmp_path,
        valid_value(
            accel_x=-29.9, accel_y=-2.9, accel_z=-0.01, jerk_x=-299.0, steering_angle=-35.0
        ),
    )

    result = split(spark, path)

    assert result.quarantined.collect() == []
    assert len(result.passed.collect()) == 1


def test_zero_speed_is_not_quarantined(spark, tmp_path):
    # 정차 상태의 속도 0은 하한과 같은 값이라 격리되지 않는다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(speed_mps=0.0))

    assert split(spark, path).quarantined.collect() == []


def test_null_optional_value_is_not_quarantined(spark, tmp_path):
    # 필수가 아닌 컬럼이 비어 있는 것은 범위 위반이 아니다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(heading=None, accel_x=None))

    assert split(spark, path).quarantined.collect() == []


def test_event_time_outside_its_bounds_is_quarantined(spark, tmp_path):
    # 허용 범위를 벗어난 연도의 event_time이 격리되는지 확인한다.
    path = write_bronze_parquet(
        spark, tmp_path, valid_value(), valid_value(event_time="1000-01-01T00:00:00+00:00")
    )

    result = split(spark, path)

    rows = result.quarantined.collect()
    assert [row["reject_reason"] for row in rows] == [OUT_OF_RANGE]
    assert rows[0]["reject_detail"] == "event_time=1000-01-01T00:00:00+00:00"
    assert len(result.passed.collect()) == 1


def test_reject_detail_names_the_violating_columns_and_values(spark, tmp_path):
    # 판정 상세에 위반한 컬럼과 실제 값이 설정 순서대로 들어가는지 확인한다.
    path = write_bronze_parquet(spark, tmp_path, valid_value(latitude=10.0, speed_mps=99.0))

    rows = split(spark, path).quarantined.collect()

    assert rows[0]["reject_detail"] == "latitude=10.0, speed_mps=99.0"
