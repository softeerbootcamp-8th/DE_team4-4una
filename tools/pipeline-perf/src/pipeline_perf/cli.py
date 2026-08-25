"""`pipeline-perf` command line: collect / render / compare (#462, #492).

    pipeline-perf collect --dag-id standard_score_pipeline --last 10 --out out/perf/
    pipeline-perf collect --dag-id standard_score_pipeline --run-id scheduled__...
    pipeline-perf render out/perf/*.json > docs/perf/<날짜>-<이름>.md
    pipeline-perf compare --before before.json --after after.json

`collect`가 남기는 원시 JSON은 커밋하지 않는다(`out/perf/`는 gitignore). 커밋하는
것은 `render`가 만든 마크다운이다.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from de4_core import ObjectStore

from pipeline_perf.airflow import AirflowClient, AirflowCredentials
from pipeline_perf.collector import CollectConfig, Collector
from pipeline_perf.compare import render_comparison
from pipeline_perf.render import render

_DEFAULT_OUT_DIR = Path("out/perf")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline-perf",
        description="승차감 점수 파이프라인의 성능 베이스라인을 수집하고 리포트로 만든다.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="Airflow/EMR/Spark/PERF 로그를 수집한다")
    collect.add_argument(
        "--dag-id",
        action="append",
        required=True,
        dest="dag_ids",
        help="수집할 DAG. 여러 번 지정할 수 있다.",
    )
    collect.add_argument("--last", type=int, default=5, help="DAG별로 최근 몇 건을 볼지 (기본 5)")
    collect.add_argument(
        "--run-id",
        action="append",
        dest="run_ids",
        help=(
            "수집할 DAG run을 지목한다. 여러 번 지정할 수 있고, 지정하면 "
            "--last/--since/--until은 무시한다."
        ),
    )
    collect.add_argument(
        "--since",
        type=_iso_timestamp,
        default=None,
        help="이 시각 이후에 실행된 run만 본다 (run_after 기준, ISO-8601)",
    )
    collect.add_argument(
        "--until",
        type=_iso_timestamp,
        default=None,
        help="이 시각 이전에 실행된 run만 본다 (run_after 기준, ISO-8601)",
    )
    collect.add_argument("--out", type=Path, default=_DEFAULT_OUT_DIR, help="원시 JSON을 쓸 디렉터리")
    collect.add_argument("--airflow-base-url", default=None)
    collect.add_argument("--application-id", default=None, help="EMR Serverless Application ID")
    collect.add_argument("--log-uri", default=None, help="EMR Serverless 로그 S3 루트")
    collect.add_argument("--bronze-input-uri", default=None, help="Bronze 입력 루트")
    collect.add_argument("--aws-profile", default=None)
    collect.add_argument("--aws-region", default=None)
    collect.add_argument(
        "--no-spark",
        action="store_true",
        help="Spark event log(L3) 파싱을 건너뛴다. 큰 event log를 안 읽어 빠르다.",
    )
    collect.set_defaults(handler=_run_collect)

    render_parser = subparsers.add_parser("render", help="수집 JSON을 마크다운 리포트로 만든다")
    render_parser.add_argument("paths", nargs="+", type=Path, help="collect가 만든 JSON 파일들")
    render_parser.add_argument("--title", default=None)
    render_parser.add_argument("-o", "--out", type=Path, default=None, help="기본은 표준출력")
    render_parser.set_defaults(handler=_run_render)

    compare_parser = subparsers.add_parser("compare", help="두 수집 결과의 델타 표를 낸다")
    compare_parser.add_argument("--before", required=True, type=Path)
    compare_parser.add_argument("--after", required=True, type=Path)
    compare_parser.add_argument("-o", "--out", type=Path, default=None, help="기본은 표준출력")
    compare_parser.set_defaults(handler=_run_compare)

    return parser


def _run_collect(args: argparse.Namespace) -> int:
    credentials = AirflowCredentials.from_env(args.airflow_base_url)
    config = CollectConfig(
        dag_ids=args.dag_ids,
        last=args.last,
        run_ids=tuple(args.run_ids or ()),
        since=args.since,
        until=args.until,
        application_id=args.application_id,
        log_uri=args.log_uri,
        bronze_input_uri=args.bronze_input_uri,
        with_spark=not args.no_spark,
    )
    session = _boto_session(args.aws_profile, args.aws_region)
    collector = Collector(
        airflow=AirflowClient(credentials),
        emr_client=session.client("emr-serverless"),
        object_store=ObjectStore(session.client("s3")),
        config=config,
    )
    payload = collector.collect()
    destination = _write_payload(args.out, payload)
    print(f"수집 결과를 {destination}에 저장했다.", file=sys.stderr)
    for note in payload["notes"]:
        print(f"  참고: {note}", file=sys.stderr)
    return 0


def _run_render(args: argparse.Namespace) -> int:
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in args.paths]
    document = render(payloads, args.title)
    _emit(document, args.out)
    return 0


def _run_compare(args: argparse.Namespace) -> int:
    before = json.loads(args.before.read_text(encoding="utf-8"))
    after = json.loads(args.after.read_text(encoding="utf-8"))
    _emit(render_comparison(before, after), args.out)
    return 0


def _iso_timestamp(value: str) -> str:
    """`--since`/`--until` 값을 API에 넘기기 전에 확인하고 정규화한다.

    시간대를 안 쓴 값은 UTC로 읽는다. 파이프라인의 시각 표기가 전부 UTC라 그쪽이
    사람이 리포트를 보며 입력하는 값과 맞다.
    """
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"ISO-8601 시각이어야 한다: {value!r} (예: 2026-08-25T09:00:00Z)"
        ) from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.isoformat()


def _boto_session(profile: str | None, region: str | None) -> Any:
    import boto3

    return boto3.Session(profile_name=profile, region_name=region)


def _write_payload(out_dir: Path, payload: dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = out_dir / f"collect-{stamp}.json"
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    return destination


def _emit(document: str, out: Path | None) -> None:
    if out is None:
        sys.stdout.write(document)
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(document, encoding="utf-8")
    print(f"{out}에 저장했다.", file=sys.stderr)
