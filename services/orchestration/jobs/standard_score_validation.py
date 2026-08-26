"""standard_segment_comfort_score의 in-flight 검증 (#495, ADR-0012).

검증 대상은 서빙 테이블이 아니라 기준 데이터셋인 S3 Gold snapshot이다 — manifest가
가리키는 활성 version parquet을 pandas로 읽어 GX로 본다. Spark가 필요 없어 Airflow
워커에서 그대로 돈다.

경로와 manifest 스키마는 `de4_core.gold_snapshot`이 갖는다. batch-jobs의
`standard_storage`가 같은 계약으로 쓰지만, 서비스 경계 규칙(AGENTS.md) 때문에 그
코드를 import하지 않고 읽는 쪽은 여기서 구현한다 — `road_environment.py`와 같다.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import great_expectations as gx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from de4_core import (
    ObjectStore,
    StandardGoldManifest,
    standard_manifest_uri,
    standard_snapshot_uri,
)
from great_expectations.data_context.types.base import ProgressBarsConfig

DEFAULT_SUITE_PATH = (
    Path(__file__).parent
    / "resources"
    / "expectations"
    / "standard_segment_comfort_score_suite.json"
)


class StandardScoreValidationFailed(Exception):
    """검증 실패 시 발생시켜 Airflow task를 hard fail시킨다 (ADR-0004)."""


@dataclass(frozen=True, slots=True)
class StandardScoreValidationSummary:
    row_count: int
    success: bool


def validate_standard_score(
    gold_output_uri: str,
    as_of: datetime,
    *,
    suite_path: Path = DEFAULT_SUITE_PATH,
    store: ObjectStore | None = None,
) -> StandardScoreValidationSummary:
    """이번 실행이 쓴 활성 snapshot만 검증한다(in-flight, 전체 이력 아님)."""
    active_store = store if store is not None else ObjectStore()
    suite = _load_suite(suite_path)
    snapshot_uri = _resolve_active_snapshot_uri(active_store, gold_output_uri, as_of)
    frame = _read_snapshot(active_store, snapshot_uri, _suite_columns(suite))

    if frame.empty:
        raise StandardScoreValidationFailed(
            f"{snapshot_uri}: snapshot has zero rows for as_of={as_of.isoformat()}"
        )

    if not _validate(frame, suite).success:
        raise StandardScoreValidationFailed(
            f"standard_score validation failed for as_of={as_of.isoformat()} "
            f"({snapshot_uri})"
        )
    return StandardScoreValidationSummary(row_count=len(frame), success=True)


def _load_suite(path: Path) -> gx.ExpectationSuite:
    return gx.ExpectationSuite(**json.loads(Path(path).read_text()))


def _suite_columns(suite: gx.ExpectationSuite) -> list[str]:
    """suite가 실제로 보는 컬럼만 골라낸다 — snapshot 전량을 워커 메모리에 올리지 않는다."""
    columns = sorted(
        {
            column
            for expectation in suite.expectations
            if (column := getattr(expectation, "column", None))
        }
    )
    if not columns:
        # 컬럼을 하나도 못 고르면 0컬럼 프레임을 읽어 "빈 snapshot"으로 오판한다.
        raise ValueError(f"{suite.name}: suite has no column-level expectations")
    return columns


def _resolve_active_snapshot_uri(
    store: ObjectStore, gold_output_uri: str, as_of: datetime
) -> str:
    snapshot_root_uri = standard_snapshot_uri(gold_output_uri, as_of)
    manifest_uri = standard_manifest_uri(snapshot_root_uri)
    if not store.exists(manifest_uri):
        raise ValueError(
            f"{manifest_uri}: no manifest found — run_standard_score writes it"
        )

    manifest = StandardGoldManifest.from_json(
        store.read_bytes(manifest_uri), snapshot_root_uri=snapshot_root_uri
    )
    # 직전 실행의 manifest가 남아 있는 경우를 걸러낸다.
    if manifest.score_as_of != as_of:
        raise ValueError(
            f"{manifest_uri}: manifest score_as_of {manifest.score_as_of!r} does not "
            f"match requested as_of {as_of!r}"
        )
    return manifest.snapshot_uri


def _read_snapshot(
    store: ObjectStore, snapshot_uri: str, columns: list[str]
) -> pd.DataFrame:
    tables = []
    for obj in store.list_objects(snapshot_uri):
        # Spark 출력에는 _SUCCESS 같은 비-Parquet 파일이 함께 남는다.
        if not obj.uri.endswith(".parquet"):
            continue
        with store.open_reader(obj.uri) as reader:
            tables.append(pq.read_table(reader, columns=columns))
    if not tables:
        raise StandardScoreValidationFailed(
            f"{snapshot_uri}: manifest points at a version with no parquet files"
        )
    return pa.concat_tables(tables).to_pandas()


def _validate(frame: pd.DataFrame, suite: gx.ExpectationSuite):
    context = gx.get_context(mode="ephemeral")
    # tqdm 진행바는 stderr로 나가고 Airflow supervisor는 task stderr를 내용과 무관하게
    # ERROR로 포워딩해, 정상 검증이 오류처럼 보인다 (#540). context 변수로 꺼 둔다.
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    datasource = context.data_sources.add_pandas(name="standard_score_datasource")
    asset = datasource.add_dataframe_asset(name="standard_segment_comfort_score")
    batch_definition = asset.add_batch_definition_whole_dataframe("standard_score_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": frame})
    return batch.validate(suite)
