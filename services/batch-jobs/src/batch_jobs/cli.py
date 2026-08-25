"""Command-line entry points for monthly road-environment builds."""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from de4_core import perf_phase

from batch_jobs.pipeline import build_and_publish_environment
from batch_jobs.sources import fetch_reference_sources

logger = logging.getLogger(__name__)

# 각 EMR Serverless Job Run 안에서 Spark 세션을 띄우는 데 쓴 시간과 job 로직에 쓴
# 시간을 갈라 놓는다(#461). Job Run 총시간(GetJobRun)에서 이 둘을 빼면 EMR이 컨테이너를
# 띄우고 내리는 데 쓴 시간이 남아, 베이스라인(#460)의 "오버헤드 대 실제 계산" 분해가 된다.
# 액션을 새로 강제하지 않고 이미 끝난 호출의 벽시계 시간만 재므로 실행 계획은 그대로다.
_SPARK_SESSION_PHASE = "spark_session"
_POSTGRES_CONNECT_PHASE = "postgres_connect"
_JOB_PHASE = "job"
# 검증은 생산 job과 같은 Job Run에서 돈다(ADR-0012) — 계산 구간과 구분해 계측한다.
_VALIDATION_PHASE = "validation"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build versioned NYC road environments")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch-reference-data")
    fetch_parser.add_argument("--output-dir", type=Path, required=True)
    fetch_parser.add_argument("--snapshot-date", type=date.fromisoformat, required=True)
    add_bbox_argument(fetch_parser)

    build_parser = subparsers.add_parser("build-road-environment")
    add_build_arguments(build_parser)
    build_parser.add_argument("--source-dir", type=Path, required=True)

    monthly_parser = subparsers.add_parser("run-monthly")
    add_build_arguments(monthly_parser)
    add_bbox_argument(monthly_parser)

    cleanse_parser = subparsers.add_parser("cleanse-sensor-events")
    cleanse_parser.add_argument("--run-id", required=True)
    cleanse_parser.add_argument("--target-hour", type=datetime.fromisoformat, required=True)
    cleanse_parser.add_argument("--road-snapshot-date", type=date.fromisoformat, required=True)
    cleanse_parser.add_argument("--feature-version", required=True)
    cleanse_parser.add_argument("--bronze-input-path")
    cleanse_parser.add_argument("--quarantine-output-path")
    cleanse_parser.add_argument("--cleansing-config-path", type=Path)
    cleanse_parser.add_argument(
        "--road-segment-path",
        help=(
            "build-road-environment Manifest의 road_segment Artifact URI를 그대로 전달한다 "
            "(디렉터리 접두사가 아니라 실제 Parquet 파일/경로)."
        ),
    )
    cleanse_parser.add_argument("--output-path")
    cleanse_parser.add_argument("--event-config-path", type=Path)
    cleanse_parser.add_argument("--steering-config-path", type=Path)
    cleanse_parser.add_argument("--map-matching-config-path", type=Path)

    score_parser = subparsers.add_parser("score-hourly-comfort")
    # 없으면 어느 시간대를 처리할지 결정할 수 없다. 기본값을 두면 DAG 배선이 빠졌을 때
    # 조용히 엉뚱한 시간대를 처리하므로 명시적으로 실패시킨다.
    score_parser.add_argument(
        "--target-hour", type=datetime.fromisoformat, required=True
    )
    score_parser.add_argument("--input-path")
    score_parser.add_argument("--output-path")
    score_parser.add_argument("--rejected-output-path")
    score_parser.add_argument("--scoring-config-path", type=Path)
    score_parser.add_argument("--run-id")

    subparsers.add_parser("migrate-database")

    standard_parser = subparsers.add_parser("load-standard-segment-comfort-score")
    standard_parser.add_argument("--as-of", required=True)

    audit_gold_parser = subparsers.add_parser("audit-gold")
    audit_gold_parser.add_argument(
        "--table",
        required=True,
        choices=["standard_segment_comfort_score", "current_segment_comfort_score"],
    )

    return parser


def add_build_arguments(parser: argparse.ArgumentParser) -> None:
    configured_data_lake = os.getenv("REFERENCE_DATA_LAKE_URI")
    parser.add_argument(
        "--data-lake-uri",
        default=configured_data_lake,
        required=configured_data_lake is None,
    )
    parser.add_argument("--reference-date", type=date.fromisoformat, required=True)
    parser.add_argument("--road-snapshot-date", type=date.fromisoformat)
    parser.add_argument("--build-id")
    parser.add_argument("--activate", action="store_true")
    parser.add_argument("--min-pavement-match-rate", type=float, default=0.0)
    parser.add_argument("--min-hump-match-rate", type=float, default=0.0)


def add_bbox_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("WEST", "SOUTH", "EAST", "NORTH"),
        help="Optional WGS84 smoke-test extent; omit it for the complete source",
    )


def run_cleansing(arguments: argparse.Namespace) -> None:
    from dataclasses import replace

    from batch_jobs.cleansing.config import CleansingJobConfig
    from batch_jobs.cleansing.job import build_spark_session, run_cleansing_job
    from batch_jobs.hourly_segment_feature_job import HourlySegmentFeatureJobConfig
    from batch_jobs.sensor_processing_validation import (
        SensorProcessingValidationConfig,
        run_sensor_processing_validation,
    )

    cleansing_defaults = CleansingJobConfig.from_env()
    feature_defaults = HourlySegmentFeatureJobConfig.from_env()
    cleansing_config = CleansingJobConfig(
        bronze_input_path=(
            arguments.bronze_input_path or cleansing_defaults.bronze_input_path
        ),
        quarantine_output_path=(
            arguments.quarantine_output_path
            or cleansing_defaults.quarantine_output_path
        ),
        rules_config_path=(
            arguments.cleansing_config_path or cleansing_defaults.rules_config_path
        ),
    )
    feature_config = HourlySegmentFeatureJobConfig(
        road_segment_path=(
            arguments.road_segment_path or feature_defaults.road_segment_path
        ),
        output_path=arguments.output_path or feature_defaults.output_path,
        event_feature_config_path=(
            arguments.event_config_path or feature_defaults.event_feature_config_path
        ),
        steering_feature_config_path=(
            arguments.steering_config_path
            or feature_defaults.steering_feature_config_path
        ),
        map_matching_config_path=(
            arguments.map_matching_config_path
            or feature_defaults.map_matching_config_path
        ),
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    with perf_phase(logger, f"sensor_processing.{_SPARK_SESSION_PHASE}"):
        spark = build_spark_session()
    try:
        with perf_phase(logger, f"sensor_processing.{_JOB_PHASE}"):
            summary = run_cleansing_job(
                spark,
                cleansing_config,
                feature_config,
                arguments.run_id,
                arguments.target_hour,
                arguments.road_snapshot_date,
                arguments.feature_version,
                datetime.now(UTC),
            )
        print(
            json.dumps(
                {
                    "input_count": summary.input_count,
                    "processed_count": summary.processed_count,
                    "accepted_count": summary.accepted_count,
                    "cleansing_quarantined_count": summary.cleansing_quarantined_count,
                    "map_matching_quarantined_count": (
                        summary.map_matching_quarantined_count
                    ),
                    "quarantined_count": summary.quarantined_count,
                    "result_count": summary.feature_summary.result_count,
                    "output_path": summary.feature_summary.output_path,
                    "run_id": summary.feature_summary.run_id,
                },
                sort_keys=True,
            )
        )
        # 검증이 실패해도 무엇이 만들어졌는지는 남도록 print 뒤에 둔다. 경로는 env를
        # 다시 읽지 않고 방금 쓴 설정을 그대로 넘긴다 — 어긋날 여지를 없앤다.
        with perf_phase(logger, f"sensor_processing.{_VALIDATION_PHASE}"):
            run_sensor_processing_validation(
                spark,
                replace(
                    SensorProcessingValidationConfig.from_env(),
                    feature_output_path=feature_config.output_path,
                    quarantine_output_path=cleansing_config.quarantine_output_path,
                ),
                arguments.target_hour,
            )
    finally:
        spark.stop()


def run_hourly_scoring(arguments: argparse.Namespace) -> None:
    from dataclasses import replace

    from batch_jobs.hourly_comfort_job import (
        HourlyComfortJobConfig,
        build_spark_session,
        run_hourly_comfort_job,
    )
    from batch_jobs.hourly_scoring_validation import (
        HourlyScoringValidationConfig,
        run_hourly_scoring_validation,
    )

    defaults = HourlyComfortJobConfig.from_env()
    run_id = arguments.run_id or os.getenv("HOURLY_COMFORT_RUN_ID")
    if not run_id:
        raise ValueError("--run-id or HOURLY_COMFORT_RUN_ID is required")
    config = HourlyComfortJobConfig(
        feature_input_path=arguments.input_path or defaults.feature_input_path,
        score_output_path=arguments.output_path or defaults.score_output_path,
        rejected_output_path=(
            arguments.rejected_output_path or defaults.rejected_output_path
        ),
        scoring_config_path=(
            arguments.scoring_config_path or defaults.scoring_config_path
        ),
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    with perf_phase(logger, f"hourly_scoring.{_SPARK_SESSION_PHASE}"):
        spark = build_spark_session()
    try:
        with perf_phase(logger, f"hourly_scoring.{_JOB_PHASE}"):
            summary = run_hourly_comfort_job(
                spark, config, run_id, datetime.now(UTC), arguments.target_hour
            )
        print(
            json.dumps(
                {
                    "scored_count": summary.scored_count,
                    "rejected_count": summary.rejected_count,
                },
                sort_keys=True,
            )
        )
        # run_cleansing과 같은 이유로 print 뒤에 두고, 경로는 방금 쓴 설정을 넘긴다.
        with perf_phase(logger, f"hourly_scoring.{_VALIDATION_PHASE}"):
            run_hourly_scoring_validation(
                spark,
                replace(
                    HourlyScoringValidationConfig.from_env(),
                    score_output_path=config.score_output_path,
                ),
                arguments.target_hour,
            )
    finally:
        spark.stop()


def run_migrate_database() -> None:
    import psycopg2

    from batch_jobs.migrate import MigrationConfig, run_migrations

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = MigrationConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        result = run_migrations(config.migrations_dir, connection)
    finally:
        connection.close()
    print(
        json.dumps(
            {"applied": list(result.applied), "skipped": list(result.skipped)},
            sort_keys=True,
        )
    )


def run_standard_comfort_score_loading(arguments: argparse.Namespace) -> None:
    import psycopg2

    from batch_jobs.comfort_score.standard_job import (
        StandardComfortScoreJobConfig,
        build_spark_session,
        run_standard_comfort_score_job,
    )

    as_of = datetime.fromisoformat(arguments.as_of)
    if as_of.utcoffset() is None:
        raise ValueError(
            "--as-of must include a UTC offset, e.g. 2026-08-19T00:00:00+00:00"
        )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = StandardComfortScoreJobConfig.from_env()
    with perf_phase(logger, f"standard_score.{_SPARK_SESSION_PHASE}"):
        spark = build_spark_session()
    with perf_phase(logger, f"standard_score.{_POSTGRES_CONNECT_PHASE}"):
        connection = psycopg2.connect(
            host=config.postgres_host,
            port=config.postgres_port,
            dbname=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password,
        )
    try:
        with perf_phase(logger, f"standard_score.{_JOB_PHASE}"):
            summary = run_standard_comfort_score_job(spark, config, as_of, connection)
        print(
            json.dumps(
                {
                    "scored_count": summary.scored_count,
                    "merged_count": summary.merged_count,
                    "inserted_count": summary.inserted_count,
                    "updated_count": summary.updated_count,
                },
                sort_keys=True,
            )
        )
    finally:
        connection.close()


def run_gold_audit_cli(arguments: argparse.Namespace) -> None:
    import boto3
    import psycopg2

    from batch_jobs.gold_audit_validation import (
        GoldAuditValidationConfig,
        run_gold_audit,
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = GoldAuditValidationConfig.from_env()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    s3_client = boto3.client("s3")
    try:
        summary = run_gold_audit(config, connection, arguments.table, s3_client)
        print(
            json.dumps(
                {
                    "table": summary.table,
                    "row_count": summary.row_count,
                    "success": summary.success,
                },
                sort_keys=True,
            )
        )
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    if arguments.command == "cleanse-sensor-events":
        run_cleansing(arguments)
        return
    if arguments.command == "score-hourly-comfort":
        run_hourly_scoring(arguments)
        return
    if arguments.command == "migrate-database":
        run_migrate_database()
        return
    if arguments.command == "load-standard-segment-comfort-score":
        run_standard_comfort_score_loading(arguments)
        return
    if arguments.command == "audit-gold":
        run_gold_audit_cli(arguments)
        return
    if arguments.command == "fetch-reference-data":
        manifest_path = fetch_reference_sources(
            arguments.output_dir,
            arguments.snapshot_date,
            tuple(arguments.bbox) if arguments.bbox else None,
        )
        print(manifest_path)
        return

    build_id = arguments.build_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    road_snapshot_date = arguments.road_snapshot_date or arguments.reference_date
    if arguments.command == "build-road-environment":
        result = build_and_publish_environment(
            arguments.source_dir,
            arguments.data_lake_uri,
            arguments.reference_date,
            road_snapshot_date,
            build_id,
            activate=arguments.activate,
            minimum_pavement_segment_match_rate=arguments.min_pavement_match_rate,
            minimum_hump_source_match_rate=arguments.min_hump_match_rate,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="de4-reference-source-") as temporary:
            source_dir = Path(temporary)
            fetch_reference_sources(
                source_dir,
                road_snapshot_date,
                tuple(arguments.bbox) if arguments.bbox else None,
            )
            result = build_and_publish_environment(
                source_dir,
                arguments.data_lake_uri,
                arguments.reference_date,
                road_snapshot_date,
                build_id,
                activate=arguments.activate,
                minimum_pavement_segment_match_rate=arguments.min_pavement_match_rate,
                minimum_hump_source_match_rate=arguments.min_hump_match_rate,
            )
    print(
        json.dumps(
            {
                "environment_id": result.manifest.environment_id,
                "manifest_uri": result.manifest_uri,
                "active_pointer_uri": result.active_pointer_uri,
                "quality": result.manifest.quality,
            },
            indent=2,
            sort_keys=True,
        )
    )
