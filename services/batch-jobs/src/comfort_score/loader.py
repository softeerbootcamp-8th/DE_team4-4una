"""Load the last N hours of `hourly_comfort_score` for Gold aggregation (#117).

다음 서브 이슈("데이터 연산", #101)가 바로 이어받을 수 있도록, 최근 168시간
윈도우의 hourly_comfort_score를 Spark DataFrame으로 반환한다. hourly_comfort_score는
Silver3 산출 단계(batch_jobs/hourly_comfort.py, #88/#89)에서 이미 자신의
trip_count를 hourly_segment_features로부터 그대로 실어 오므로(OQ-039), 여기서
별도로 join하지 않는다. 실제 집계/Shrinkage 연산과 Gold 적재는 이 함수의 범위
밖이다 (services/batch-jobs/src/comfort_score/formula.py, 후속 이슈).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from batch_jobs.schemas import HOURLY_COMFORT_SCORE_SCHEMA
from de4_core import join_uri
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

# hourly_comfort_score의 기본 키 (context/data/schema-catalog.md).
PRIMARY_KEY = ("segment_id", "vehicle_profile_id", "data_period_start")

DEFAULT_WINDOW_HOURS = 168


def load_hourly_comfort_score_for_gold(
    spark: SparkSession,
    data_lake_uri: str,
    as_of: datetime,
    window_hours: int = DEFAULT_WINDOW_HOURS,
) -> DataFrame:
    """Load `hourly_comfort_score`, windowed to the last `window_hours` hours.

    - `data_lake_uri`는 로컬 Parquet 루트(예: `file://.../data/local-lake`)와
      운영 S3 루트(`s3://...`)를 함수 수정 없이 그대로 바꿔 끼울 수 있는
      파라미터다 (join_uri가 두 스킴을 모두 처리한다).
    - 매 호출마다 [as_of - window_hours, as_of) 구간 전체를 다시 읽는다 (증분 없음).
    """
    comfort_score_uri = join_uri(data_lake_uri, "silver", "hourly_comfort_score")

    comfort_score_df = _read_validated_parquet(
        spark, comfort_score_uri, HOURLY_COMFORT_SCORE_SCHEMA
    )
    windowed = _filter_window_hours(comfort_score_df, as_of, window_hours)
    return _select_latest_scoring_version(windowed)


def _read_validated_parquet(spark: SparkSession, uri: str, expected: StructType) -> DataFrame:
    df = spark.read.parquet(uri)
    _validate_schema(df.schema, expected, uri)
    return df


def _validate_schema(actual: StructType, expected: StructType, source: str) -> None:
    """Fail clearly when `actual` is missing a column or has the wrong type.

    `spark.read.schema(...).parquet(...)`은 누락된 컬럼을 조용히 NULL로 채워서
    "명확히 실패"라는 완료 조건을 못 지키기 때문에, 실제로 읽은 스키마를 먼저
    비교한다. 여기 없는 추가 컬럼은 문제 삼지 않는다.
    """
    actual_fields = {field.name: field.dataType for field in actual.fields}

    missing = [field.name for field in expected.fields if field.name not in actual_fields]
    if missing:
        raise ValueError(f"{source}: missing required column(s): {', '.join(missing)}")

    mismatched = [
        f"{field.name} (expected {field.dataType}, got {actual_fields[field.name]})"
        for field in expected.fields
        if actual_fields[field.name] != field.dataType
    ]
    if mismatched:
        raise ValueError(f"{source}: column type mismatch: {', '.join(mismatched)}")


def _filter_window_hours(df: DataFrame, as_of: datetime, window_hours: int) -> DataFrame:
    """Keep rows with `data_period_start` in `[as_of - window_hours, as_of)`.

    상한을 배타적으로 둔다: as_of는 "지금"을 뜻하고, 그 시각에 시작하는 시간은
    아직 끝나지 않았으므로 이번 윈도우에 포함하지 않는다.
    """
    window_start = as_of - timedelta(hours=window_hours)
    return df.filter(
        (F.col("data_period_start") >= F.lit(window_start))
        & (F.col("data_period_start") < F.lit(as_of))
    )


def _select_latest_scoring_version(df: DataFrame) -> DataFrame:
    """Keep only the semver-highest `scoring_version` row per `PRIMARY_KEY`.

    "1.1.1" < "3.1.1" < "10.0.0"처럼 세미버전 규칙으로 비교해야 하므로, 점으로
    나눈 각 자리를 정수 배열로 캐스팅해 사전식으로 비교한다.
    """
    version_rank = F.split(F.col("scoring_version"), r"\.").cast("array<int>")
    ranking_window = Window.partitionBy(*PRIMARY_KEY).orderBy(version_rank.desc())
    return (
        df.withColumn("_version_rank", F.row_number().over(ranking_window))
        .filter(F.col("_version_rank") == 1)
        .drop("_version_rank")
    )
