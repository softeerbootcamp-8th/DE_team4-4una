"""One-off feature-distribution analysis for hourly comfort scoring calibration (#544).

이건 정규 파이프라인이 아니다 — Airflow DAG도, Transform2/Transform3 실행 경로도 이
모듈을 부르지 않는다. 대표 기간의 실제 Silver2(`hourly_segment_features`) 분포를 사람이
눈으로 보고 `hourly_comfort.yaml`의 anchor/threshold를 정할 때만 CLI로 수동 실행한다.
확정된 값은 이 모듈이 아니라 YAML config에 고정해서 버전으로 관리한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from batch_jobs.comfort_scoring_config import HourlyScoringConfig
from batch_jobs.hourly_comfort import RATE_SOURCE_COLUMNS

# 최소 출력으로 요구된 percentile들. 필요하면 늘릴 수 있게 튜플로 둔다.
PERCENTILES = (0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# scoring이 직접 쓰지 않지만(정규화 대상이 아님) speed band scaling에 영향을 주는
# feature라 분석 대상에 항상 포함한다(이슈 2번 "speed band scaling이 분포 압축에
# 영향을 주는지" 확인용).
ALWAYS_INCLUDED_COLUMNS = ("avg_speed_mps",)


@dataclass(frozen=True, slots=True)
class FeatureDistribution:
    """한 feature의 분포 요약. `percentiles`는 `PERCENTILES`와 같은 순서다."""

    count: int
    mean: float | None
    minimum: float | None
    maximum: float | None
    percentiles: tuple[float | None, ...]


def scoring_feature_columns(config: HourlyScoringConfig) -> tuple[str, ...]:
    """scoring이 실제로 쓰는 feature 이름 전체(정규화 대상 + rate 소스 제외).

    `config.normalizers`에서 그대로 뽑아온다 — anchor 목록이 바뀌어도 이 함수를
    따로 안 고쳐도 분석 대상이 같이 바뀐다(드리프트 방지).
    """
    return tuple(name for name, _ in config.normalizers)


def with_rate_columns(df: DataFrame, feature_names: tuple[str, ...]) -> DataFrame:
    """분석 대상에 rate feature가 있으면 hourly_comfort.py와 똑같은 식으로 값을 만든다.

    hourly_segment_features는 rate가 아니라 count(hard_brake_count 등)를 저장하므로,
    scoring이 실제로 보는 값(count / trip_count)을 여기서 재현해야 anchor가 실제
    입력 분포와 맞아떨어진다. 공식은 RATE_SOURCE_COLUMNS를 그대로 가져와 hourly_comfort.py와
    어긋나지 않게 한다.
    """
    result = df
    for rate_name, count_name in RATE_SOURCE_COLUMNS.items():
        if rate_name not in feature_names:
            continue
        result = result.withColumn(
            rate_name,
            F.when(
                F.col("trip_count") > 0,
                F.col(count_name).cast("double") / F.col("trip_count"),
            ),
        )
    return result


def build_feature_distributions(
    df: DataFrame, feature_names: tuple[str, ...]
) -> dict[str, FeatureDistribution]:
    """`feature_names` 각각의 count/mean/min/max/percentile을 계산한다.

    approxQuantile은 여러 컬럼을 한 번에 받아 한 pass로 계산한다(Spark 표준
    동작). count/mean/min/max는 별도의 agg 한 번으로 같이 얻는다 — 이 도구는
    프로덕션 파이프라인이 아니라서 action 두 번을 아끼려고 억지로 합치지 않는다
    (#539 최적화는 프로덕션 job에만 적용했다).
    """
    all_columns = list(dict.fromkeys((*feature_names, *ALWAYS_INCLUDED_COLUMNS)))
    df = with_rate_columns(df, feature_names)

    agg_exprs: list[Column] = []
    for name in all_columns:
        agg_exprs.extend(
            [
                F.count(F.col(name)).alias(f"{name}__count"),
                F.mean(F.col(name)).alias(f"{name}__mean"),
                F.min(F.col(name)).alias(f"{name}__min"),
                F.max(F.col(name)).alias(f"{name}__max"),
            ]
        )
    summary_row = df.agg(*agg_exprs).first()

    # relativeError=0은 정확한 percentile을 요구한다 — 이 도구는 대표 기간
    # 한정으로 사람이 손으로 몇 번 돌리는 용도라 근사 오차 없이 정확한 값을 쓴다.
    quantiles = df.approxQuantile(list(all_columns), list(PERCENTILES), 0.0)

    distributions: dict[str, FeatureDistribution] = {}
    for name, column_quantiles in zip(all_columns, quantiles, strict=True):
        distributions[name] = FeatureDistribution(
            count=summary_row[f"{name}__count"],
            mean=summary_row[f"{name}__mean"],
            minimum=summary_row[f"{name}__min"],
            maximum=summary_row[f"{name}__max"],
            percentiles=tuple(column_quantiles) if column_quantiles else (None,) * len(PERCENTILES),
        )
    return distributions


def filter_representative_period(
    df: DataFrame, start: datetime | None, end: datetime | None
) -> DataFrame:
    """`--start`/`--end`가 주어지면 그 구간의 `data_period_start`만 남긴다."""
    if start is not None:
        df = df.filter(F.col("data_period_start") >= F.lit(start))
    if end is not None:
        df = df.filter(F.col("data_period_start") < F.lit(end))
    return df
