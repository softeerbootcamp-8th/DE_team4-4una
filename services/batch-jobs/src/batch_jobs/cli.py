"""Command-line entry points for monthly road-environment builds."""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from batch_jobs.pipeline import build_and_publish_environment
from batch_jobs.sources import fetch_reference_sources


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

    score_parser = subparsers.add_parser("score-hourly-comfort")
    score_parser.add_argument("--input-path")
    score_parser.add_argument("--output-path")
    score_parser.add_argument("--rejected-output-path")
    score_parser.add_argument("--scoring-config-path", type=Path)
    score_parser.add_argument("--run-id")

    subparsers.add_parser("migrate-database")

    gold_parser = subparsers.add_parser("load-segment-comfort-score")
    gold_parser.add_argument("--as-of", required=True)
    feature_parser = subparsers.add_parser("build-hourly-segment-features")
    feature_parser.add_argument("--target-hour", type=datetime.fromisoformat, required=True)
    feature_parser.add_argument("--road-snapshot-date", type=date.fromisoformat, required=True)
    feature_parser.add_argument("--feature-version", required=True)
    feature_parser.add_argument("--run-id")
    feature_parser.add_argument("--input-path")
    feature_parser.add_argument("--road-segment-path")
    feature_parser.add_argument("--output-path")
    feature_parser.add_argument("--event-config-path", type=Path)
    feature_parser.add_argument("--steering-config-path", type=Path)
    feature_parser.add_argument("--map-matching-config-path", type=Path)
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


def run_cleansing(run_id: str) -> None:
    from batch_jobs.cleansing.config import CleansingJobConfig
    from batch_jobs.cleansing.job import build_spark_session, run_cleansing_job

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_spark_session()
    try:
        run_cleansing_job(spark, CleansingJobConfig.from_env(), run_id, datetime.now(UTC))
    finally:
        spark.stop()


def run_hourly_scoring(arguments: argparse.Namespace) -> None:
    from batch_jobs.hourly_comfort_job import (
        HourlyComfortJobConfig,
        build_spark_session,
        run_hourly_comfort_job,
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
    spark = build_spark_session()
    try:
        summary = run_hourly_comfort_job(
            spark, config, run_id, datetime.now(UTC)
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


def run_segment_comfort_score_loading(arguments: argparse.Namespace) -> None:
    import psycopg2

    from batch_jobs.comfort_score.gold_job import (
        SegmentComfortScoreJobConfig,
        build_spark_session,
        run_segment_comfort_score_job,
    )

    as_of = datetime.fromisoformat(arguments.as_of)
    if as_of.utcoffset() is None:
        raise ValueError(
            "--as-of must include a UTC offset, e.g. 2026-08-16T00:00:00+00:00"
        )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    config = SegmentComfortScoreJobConfig.from_env()
    spark = build_spark_session()
    connection = psycopg2.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )
    try:
        summary = run_segment_comfort_score_job(spark, config, as_of, connection)
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
def run_hourly_segment_feature_building(arguments: argparse.Namespace) -> None:
    from batch_jobs.hourly_segment_feature_job import (
        HourlySegmentFeatureJobConfig,
        build_spark_session,
        run_hourly_segment_feature_job,
    )

    defaults = HourlySegmentFeatureJobConfig.from_env()
    run_id = arguments.run_id or os.getenv("HOURLY_SEGMENT_FEATURE_RUN_ID")
    if not run_id:
        raise ValueError("--run-id or HOURLY_SEGMENT_FEATURE_RUN_ID is required")
    config = HourlySegmentFeatureJobConfig(
        processed_sensor_event_path=arguments.input_path or defaults.processed_sensor_event_path,
        road_segment_path=arguments.road_segment_path or defaults.road_segment_path,
        output_path=arguments.output_path or defaults.output_path,
        event_feature_config_path=(
            arguments.event_config_path or defaults.event_feature_config_path
        ),
        steering_feature_config_path=(
            arguments.steering_config_path or defaults.steering_feature_config_path
        ),
        map_matching_config_path=(
            arguments.map_matching_config_path or defaults.map_matching_config_path
        ),
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_spark_session()
    try:
        summary = run_hourly_segment_feature_job(
            spark,
            config,
            arguments.target_hour,
            arguments.road_snapshot_date,
            arguments.feature_version,
            run_id,
            datetime.now(UTC),
        )
        print(
            json.dumps(
                {
                    "result_count": summary.result_count,
                    "output_path": summary.output_path,
                    "run_id": summary.run_id,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    if arguments.command == "cleanse-sensor-events":
        run_cleansing(arguments.run_id)
        return
    if arguments.command == "score-hourly-comfort":
        run_hourly_scoring(arguments)
        return
    if arguments.command == "migrate-database":
        run_migrate_database()
        return
    if arguments.command == "load-segment-comfort-score":
        run_segment_comfort_score_loading(arguments)
        return
    if arguments.command == "build-hourly-segment-features":
        run_hourly_segment_feature_building(arguments)
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
