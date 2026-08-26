from datetime import UTC, date, datetime, timedelta

import pytest
from batch_jobs.comfort_scoring_config import (
    DEFAULT_HOURLY_SCORING_CONFIG,
    DEFAULT_HOURLY_SCORING_CONFIG_PATH,
)
from batch_jobs.hourly_comfort import (
    CLASSIFIED_COLUMNS,
    _normalized_penalty,
    build_hourly_scoring_plan,
)
from batch_jobs.hourly_comfort_job import HourlyComfortJobConfig, run_hourly_comfort_job
from batch_jobs.hourly_comfort_storage import hour_output_path as score_hour_path
from batch_jobs.hourly_segment_feature_storage import (
    hour_output_path as feature_hour_path,
)
from batch_jobs.schemas import (
    HOURLY_COMFORT_SCORE_SCHEMA,
    HOURLY_SEGMENT_FEATURE_SCHEMA,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

# Parquet 읽기는 선언 스키마의 nullable=False를 강제하지 않는다. 파일에 NULL이 있으면
# 그대로 흘러들어오므로, 그 상황을 만들려면 느슨한 스키마로 DataFrame을 만들어야 한다.
_NULLABLE = StructType(
    [
        StructField(field.name, field.dataType, nullable=True)
        for field in HOURLY_SEGMENT_FEATURE_SCHEMA
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("hourly-comfort-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestNormalizedPenaltyBoundaries:
    """low/high 경계에서 정확히 0/1로 clamp되고, 그 사이는 선형인지 확인한다(#544)."""

    def _penalty(self, spark, value: float, low: float, high: float, scale: float = 1.0) -> float:
        df = spark.range(1).select(
            _normalized_penalty(F.lit(value), low, high, F.lit(scale)).alias("penalty")
        )
        return df.first()["penalty"]

    def test_at_or_below_low_is_zero(self, spark):
        assert self._penalty(spark, -5.0, low=0.0, high=10.0) == 0.0
        assert self._penalty(spark, 0.0, low=0.0, high=10.0) == 0.0

    def test_at_or_above_high_is_one(self, spark):
        assert self._penalty(spark, 10.0, low=0.0, high=10.0) == 1.0
        assert self._penalty(spark, 50.0, low=0.0, high=10.0) == 1.0

    def test_midpoint_is_linear(self, spark):
        assert self._penalty(spark, 5.0, low=0.0, high=10.0) == pytest.approx(0.5)

    def test_speed_scale_shrinks_the_anchors_proportionally(self, spark):
        # scale=0.5면 low/high가 반으로 줄어 같은 value가 상대적으로 더 나쁘게 잡힌다.
        assert self._penalty(spark, 5.0, low=0.0, high=10.0, scale=0.5) == 1.0

    def test_default_config_normalizers_all_satisfy_comfortable_below_uncomfortable(self):
        # __post_init__이 이미 강제하지만 실제 anchor 세트로 회귀 확인용으로 남긴다(#544).
        for name, anchors in DEFAULT_HOURLY_SCORING_CONFIG.normalizers:
            assert anchors.comfortable < anchors.uncomfortable, name

    def test_rejects_a_config_where_comfortable_is_not_below_uncomfortable(self):
        from batch_jobs.comfort_scoring_config import (
            ComponentRule,
            HourlyScoringConfig,
            NormalizationRange,
            SpeedBand,
        )

        with pytest.raises(ValueError, match="comfortable anchors must be below uncomfortable"):
            HourlyScoringConfig(
                scoring_version="9.9.9",
                compatible_feature_versions=frozenset({"v1"}),
                minimum_valid_weight=0.5,
                speed_bands=(SpeedBand(upper_mps=None, anchor_scale=1.0),),
                normalizers=(("a", NormalizationRange(comfortable=1.0, uncomfortable=1.0)),),
                components=(ComponentRule("score", (("a", 1.0),)),),
            )


class TestCalculateHourlyComfortScores:
    PERIOD_START = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)
    PROCESSED_AT = datetime(2026, 8, 15, 3, 0, tzinfo=UTC)

    def feature_row(self, segment_id: str = "100001", **overrides: object) -> dict[str, object]:
        row = {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            "data_period_start": self.PERIOD_START,
            "data_period_end": self.PERIOD_START + timedelta(hours=1),
            "road_snapshot_date": date(2026, 8, 1),
            "avg_speed_mps": 7.0,
            "rms_accel_x": 0.0,
            "rms_accel_y": 0.0,
            "rms_accel_z": 0.0,
            "p95_abs_accel_x": 0.0,
            "p95_abs_accel_y": 0.0,
            "p95_abs_accel_z": 0.0,
            "rms_jerk_x": 0.0,
            "rms_jerk_y": 0.0,
            "rms_jerk_z": 0.0,
            "p95_abs_jerk_x": 0.0,
            "p95_abs_jerk_y": 0.0,
            "p95_abs_jerk_z": 0.0,
            "hard_brake_count": 0,
            "hard_accel_count": 0,
            "sharp_steer_count": 0,
            "steer_reversal_count": 0,
            "rms_steering_rate": 0.0,
            "rms_steering_vibration": 0.0,
            "sample_count": 36_000,
            "trip_count": 10,
            "feature_version": "hourly-features-v1",
            "_processed_at": self.PROCESSED_AT,
            "_run_id": "silver2-run",
        }
        return row | overrides

    def uncomfortable_row(self, segment_id: str, output_column: str) -> dict[str, object]:
        row = self.feature_row(segment_id)
        rules = {
            rule.output_column: rule for rule in DEFAULT_HOURLY_SCORING_CONFIG.components
        }
        normalizers = dict(DEFAULT_HOURLY_SCORING_CONFIG.normalizers)
        count_columns = {
            "hard_brake_rate": "hard_brake_count",
            "hard_accel_rate": "hard_accel_count",
            "sharp_steer_rate": "sharp_steer_count",
            "steer_reversal_rate": "steer_reversal_count",
        }
        for name, _ in rules[output_column].weights:
            target = count_columns.get(name, name)
            value = normalizers[name].uncomfortable
            row[target] = int(value * row["trip_count"]) if name in count_columns else value
        return row

    def nullable_row(self, segment_id: str = "100001", **overrides: object):
        """NULL을 넣을 수 있는 행. `_NULLABLE` 스키마와 함께 쓴다."""
        return _NULLABLE, self.feature_row(segment_id, **overrides)

    def score(self, spark, *rows):
        # nullable_row()를 섞어 쓰면 그 행들은 느슨한 스키마로 만든다 — Parquet 읽기가
        # nullable=False를 강제하지 않는 상황을 그대로 재현하기 위함이다.
        schema = HOURLY_SEGMENT_FEATURE_SCHEMA
        payload = []
        for row in rows:
            if isinstance(row, tuple):
                schema, row = row
            payload.append(row)
        features = spark.createDataFrame(payload, schema)
        return build_hourly_scoring_plan(features, "silver3-run", self.PROCESSED_AT)

    def test_produces_the_declared_schema_and_metadata(self, spark):
        result = self.score(spark, self.feature_row())

        assert (
            result.scored().collect()
            == self.score(spark, self.feature_row()).scored().collect()
        )
        assert result.scored().columns == [
            field.name for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
        assert [field.dataType for field in result.scored().schema] == [
            field.dataType for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
        row = result.scored().first()
        assert all(row[column] is not None for column in result.scored().columns)
        assert row["scoring_version"] == DEFAULT_HOURLY_SCORING_CONFIG.scoring_version
        assert row["_run_id"] == "silver3-run"
        assert (row["sample_count"], row["trip_count"]) == (36_000, 10)

    def test_each_discomfort_group_only_lowers_its_directional_score(self, spark):
        rows = self.score(
            spark,
            self.feature_row("smooth"),
            self.uncomfortable_row("rough", "vertical_score"),
            self.uncomfortable_row("stop-and-go", "longitudinal_score"),
            self.uncomfortable_row("turning", "lateral_score"),
            self.feature_row("ten-trips", hard_brake_count=10, trip_count=10),
            self.feature_row("twenty-trips", hard_brake_count=20, trip_count=20),
        ).scored()
        by_segment = {row.segment_id: row for row in rows.collect()}
        smooth = by_segment["smooth"]

        assert by_segment["rough"].vertical_score < smooth.vertical_score
        assert by_segment["rough"].longitudinal_score == smooth.longitudinal_score
        assert by_segment["stop-and-go"].longitudinal_score < smooth.longitudinal_score
        assert by_segment["turning"].lateral_score < smooth.lateral_score
        assert (
            by_segment["ten-trips"].longitudinal_score
            == by_segment["twenty-trips"].longitudinal_score
        )
        assert all(
            0 <= value <= 100
            for row in by_segment.values()
            for value in (row.vertical_score, row.longitudinal_score, row.lateral_score)
        )

    def test_missing_features_follow_the_configured_weight_policy(self, spark):
        accepted = self.score(spark, self.feature_row(rms_steering_vibration=None))
        rejected = self.score(spark, self.feature_row(rms_accel_z=None, p95_abs_accel_z=None))

        assert accepted.scored().count() == 1
        assert accepted.rejected().count() == 0
        assert rejected.scored().count() == 0
        assert (
            rejected.rejected().first().reject_reason == "INSUFFICIENT_SCORING_FEATURES"
        )

    def test_caches_only_the_columns_the_two_outputs_need(self, spark):
        """분기 전에 캐시할 공통 결과는 downstream이 쓰는 컬럼만 들고 있어야 한다."""
        plan = self.score(spark, self.feature_row())

        assert plan.classified.columns == list(CLASSIFIED_COLUMNS)
        # 점수 출력이 요구하는 필드는 캐시 컬럼이거나 F.lit()으로 붙는 메타데이터뿐이다.
        supplied = set(CLASSIFIED_COLUMNS) | {
            "scoring_version",
            "_run_id",
            "_processed_at",
        }
        assert not {field.name for field in HOURLY_COMFORT_SCORE_SCHEMA} - supplied

    def test_audit_reports_both_row_counts_in_one_pass(self, spark):
        counts = self.score(
            spark,
            self.feature_row("scored"),
            self.feature_row("rejected", sample_count=0),
        ).audit()

        assert (counts.scored_count, counts.rejected_count) == (1, 1)

    def test_audit_of_an_empty_partition_reports_zero(self, spark):
        """global aggregation은 빈 입력에도 한 행을 돌려주므로 0으로 확정돼야 한다."""
        features = spark.createDataFrame([], HOURLY_SEGMENT_FEATURE_SCHEMA)
        counts = build_hourly_scoring_plan(
            features, "silver3-run", self.PROCESSED_AT
        ).audit()

        assert (counts.scored_count, counts.rejected_count) == (0, 0)

    # 데이터를 읽어야 아는 검증은 audit()의 단일 집계로 모았다. 계획을 만드는 단계는
    # 표현식만 세우므로 여기서는 아직 실패하지 않는다.
    def test_rejects_an_unsupported_feature_version(self, spark):
        plan = self.score(spark, self.feature_row(feature_version="hourly-features-v2"))

        with pytest.raises(ValueError, match="unsupported feature versions"):
            plan.audit()

    def test_rejects_a_null_feature_version(self, spark):
        """collect_set이 NULL을 버리므로 별도로 세지 않으면 조용히 통과한다."""
        plan = self.score(spark, self.nullable_row(feature_version=None))

        with pytest.raises(ValueError, match="feature_version must not be null"):
            plan.audit()

    def test_rejects_duplicate_primary_keys(self, spark):
        plan = self.score(spark, self.feature_row(), self.feature_row())

        with pytest.raises(ValueError, match="duplicate primary keys"):
            plan.audit()

    def test_a_null_in_a_non_nullable_count_is_quarantined(self, spark):
        """`nullable=False` 선언은 Parquet 읽기에서 강제되지 않는다.

        NULL이 섞이면 eligible 조건식이 NULL이 되는데, 이를 boolean으로 확정하지 않으면
        그 행은 점수 출력과 격리 출력 어디에도 못 들어가고 사라진다.
        """
        plan = self.score(
            spark,
            self.nullable_row("healthy"),
            self.nullable_row("null-sample-count", sample_count=None),
            self.nullable_row("null-trip-count", trip_count=None),
        )

        scored = {row.segment_id for row in plan.scored().collect()}
        rejected = {row.segment_id: row.reject_reason for row in plan.rejected().collect()}

        assert scored == {"healthy"}
        assert rejected == {
            "null-sample-count": "INVALID_SCORING_INPUT",
            "null-trip-count": "INVALID_SCORING_INPUT",
        }
        # 입력 3행이 출력 3행으로 보존된다 — 어느 쪽에서도 조용히 사라지지 않는다.
        assert len(scored) + len(rejected) == 3


class TestRunHourlyComfortJob:
    PERIOD_START = datetime(2026, 8, 15, 10, 0, tzinfo=UTC)
    PROCESSED_AT = datetime(2026, 8, 15, 11, 5, tzinfo=UTC)

    def feature_row(
        self,
        segment_id: str,
        sample_count: int = 36_000,
        period_start: datetime | None = None,
    ) -> dict[str, object]:
        # period_start를 바꿀 수 있어야 여러 시간 파티션을 만드는 테스트를 쓸 수 있다(#469).
        period_start = period_start or self.PERIOD_START
        return {
            "segment_id": segment_id,
            "vehicle_profile_id": 1,
            "data_period_start": period_start,
            "data_period_end": period_start + timedelta(hours=1),
            "road_snapshot_date": date(2026, 8, 1),
            "avg_speed_mps": 7.0,
            "rms_accel_x": 0.2,
            "rms_accel_y": 0.2,
            "rms_accel_z": 0.2,
            "p95_abs_accel_x": 0.5,
            "p95_abs_accel_y": 0.5,
            "p95_abs_accel_z": 0.5,
            "rms_jerk_x": 0.6,
            "rms_jerk_y": 0.6,
            "rms_jerk_z": 0.6,
            "p95_abs_jerk_x": 1.6,
            "p95_abs_jerk_y": 1.6,
            "p95_abs_jerk_z": 2.1,
            "hard_brake_count": 1,
            "hard_accel_count": 1,
            "sharp_steer_count": 1,
            "steer_reversal_count": 1,
            "rms_steering_rate": 3.0,
            "rms_steering_vibration": 0.1,
            "sample_count": sample_count,
            "trip_count": 10,
            "feature_version": "hourly-features-v1",
            "_processed_at": self.PROCESSED_AT,
            "_run_id": "silver2-run",
        }

    def write_feature_partition(self, spark, feature_root, period_start, rows):
        """Silver2의 해당 시간 파티션에 feature 행을 쓴다."""
        spark.createDataFrame(rows, HOURLY_SEGMENT_FEATURE_SCHEMA).write.parquet(
            feature_hour_path(str(feature_root), period_start)
        )

    def test_reads_scores_and_idempotently_writes_parquet(self, spark, tmp_path):
        input_path = tmp_path / "features"
        score_path = tmp_path / "scores"
        rejected_path = tmp_path / "rejected"
        self.write_feature_partition(
            spark,
            input_path,
            self.PERIOD_START,
            [self.feature_row("accepted"), self.feature_row("rejected", sample_count=0)],
        )
        config = HourlyComfortJobConfig(
            str(input_path),
            str(score_path),
            str(rejected_path),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )
        score_partition = score_hour_path(str(score_path), self.PERIOD_START)

        first_summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT, self.PERIOD_START
        )
        first_rows = spark.read.parquet(score_partition).collect()
        second_summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT, self.PERIOD_START
        )
        scores = spark.read.parquet(score_partition)

        assert first_summary == second_summary
        assert (first_summary.scored_count, first_summary.rejected_count) == (1, 1)
        assert scores.collect() == first_rows
        assert scores.columns == [field.name for field in HOURLY_COMFORT_SCORE_SCHEMA]
        assert [field.dataType for field in scores.schema] == [
            field.dataType for field in HOURLY_COMFORT_SCORE_SCHEMA
        ]
        row = scores.first()
        assert row["scoring_version"] == DEFAULT_HOURLY_SCORING_CONFIG.scoring_version
        assert row["_run_id"] == "silver3-run"
        stored_epoch = scores.select(F.unix_timestamp("_processed_at")).first()[0]
        assert stored_epoch == int(self.PROCESSED_AT.timestamp())
        rejected_partition = score_hour_path(str(rejected_path), self.PERIOD_START)
        assert spark.read.parquet(rejected_partition).first()["segment_id"] == "rejected"

    def test_reads_only_the_target_hour_partition_of_silver2(self, spark, tmp_path):
        """Silver2 루트가 아니라 target_hour 파티션만 읽는다 (#469)."""
        input_path = tmp_path / "features"
        other_hour = self.PERIOD_START + timedelta(hours=1)
        self.write_feature_partition(
            spark, input_path, self.PERIOD_START, [self.feature_row("in-scope")]
        )
        self.write_feature_partition(
            spark,
            input_path,
            other_hour,
            [self.feature_row("out-of-scope", period_start=other_hour)],
        )
        config = HourlyComfortJobConfig(
            str(input_path),
            str(tmp_path / "scores"),
            str(tmp_path / "rejected"),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )

        summary = run_hourly_comfort_job(
            spark, config, "silver3-run", self.PROCESSED_AT, self.PERIOD_START
        )

        # 전체를 읽었다면 2행이 나온다.
        assert summary.scored_count == 1
        scored = spark.read.parquet(
            score_hour_path(str(tmp_path / "scores"), self.PERIOD_START)
        )
        assert [row["segment_id"] for row in scored.collect()] == ["in-scope"]

    def test_writes_only_the_target_hour_partition_of_silver3(self, spark, tmp_path):
        """두 번째 실행이 첫 번째 시간대의 파티션을 덮어쓰지 않는다 (#469)."""
        input_path = tmp_path / "features"
        score_path = tmp_path / "scores"
        second_hour = self.PERIOD_START + timedelta(hours=1)
        self.write_feature_partition(
            spark, input_path, self.PERIOD_START, [self.feature_row("first")]
        )
        self.write_feature_partition(
            spark,
            input_path,
            second_hour,
            [self.feature_row("second", period_start=second_hour)],
        )
        config = HourlyComfortJobConfig(
            str(input_path),
            str(score_path),
            str(tmp_path / "rejected"),
            DEFAULT_HOURLY_SCORING_CONFIG_PATH,
        )

        run_hourly_comfort_job(
            spark, config, "run-1", self.PROCESSED_AT, self.PERIOD_START
        )
        run_hourly_comfort_job(spark, config, "run-2", self.PROCESSED_AT, second_hour)

        first = spark.read.parquet(score_hour_path(str(score_path), self.PERIOD_START))
        second = spark.read.parquet(score_hour_path(str(score_path), second_hour))
        assert [row["segment_id"] for row in first.collect()] == ["first"]
        assert [row["segment_id"] for row in second.collect()] == ["second"]

    def test_job_config_supports_environment_overrides(self, tmp_path):
        config_path = tmp_path / "scoring.yaml"
        config = HourlyComfortJobConfig.from_env(
            {
                "HOURLY_COMFORT_INPUT_PATH": "input",
                "HOURLY_COMFORT_OUTPUT_PATH": "output",
                "HOURLY_COMFORT_REJECTED_OUTPUT_PATH": "rejected",
                "HOURLY_COMFORT_SCORING_CONFIG_PATH": str(config_path),
            }
        )

        assert config == HourlyComfortJobConfig(
            "input", "output", "rejected", config_path
        )
