"""PERF log lines from the Spark driver logs — the L4 collection layer (#462).

`de4_core.perf_phase`가 남기는 `PERF {json}` 한 줄이 Spark 밖 구간(psycopg2 MERGE 등)의
유일한 흔적이다.

**어느 스트림에 남는지는 실제 Job Run으로 확인했다.** batch-jobs의 로깅 설정은 이
줄을 driver의 **stdout**으로 내보낸다(`SPARK_DRIVER/stdout.gz`). stderr에는 Spark
자신의 log4j 출력만 있고 PERF 줄이 하나도 없다. 그래도 두 스트림을 모두 읽는다 —
로깅 설정이 바뀌어 stderr로 옮겨가도 수집이 조용히 비지 않게 하기 위해서다. 같은
줄이 양쪽에 다 잡히는 경우를 대비해 동일 payload는 한 번만 센다.
"""

from __future__ import annotations

import gzip
import json
import re
from collections.abc import Iterable, Iterator
from typing import Any, Protocol

from de4_core import PERF_LOG_PREFIX

# 로그 라인 앞에 붙는 타임스탬프·로거 이름을 건너뛰고 `PERF {` 이후만 잡는다.
_PERF_LINE = re.compile(rf"\b{re.escape(PERF_LOG_PREFIX)}\s+(\{{.*\}})\s*$")

# 우선순위 순서. `archived/`는 같은 내용의 회전본이라 보지 않는다.
_DRIVER_LOG_CANDIDATES = (
    "SPARK_DRIVER/stdout.gz",
    "SPARK_DRIVER/stdout",
    "SPARK_DRIVER/stderr.gz",
    "SPARK_DRIVER/stderr",
)


class Reader(Protocol):
    def read_bytes(self, uri: str) -> bytes: ...

    def exists(self, uri: str) -> bool: ...


def parse_perf_lines(lines: Iterable[str]) -> Iterator[dict[str, Any]]:
    for line in lines:
        match = _PERF_LINE.search(line)
        if match is None:
            continue
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            yield payload


def collect_perf_phases(reader: Reader, job_log_prefix: str) -> dict[str, Any]:
    """Job Run 하나의 driver 로그에서 PERF 구간을 모은다.

    로그가 아직 안 올라왔거나(실행 직후) 계측 전 이미지로 돈 Job Run이면 빈 결과와
    함께 이유를 남긴다 — 없는 것과 0초인 것을 리포트에서 구분해야 한다.
    """
    sources = _resolve_driver_logs(reader, job_log_prefix)
    phases: list[dict[str, Any]] = []
    seen: set[str] = set()
    for uri in sources:
        for payload in parse_perf_lines(_read_text(reader, uri).splitlines()):
            fingerprint = json.dumps(payload, sort_keys=True)
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            phases.append(payload)
    return {
        "source_uris": sources,
        "available": bool(sources),
        "phases": phases,
    }


def _resolve_driver_logs(reader: Reader, job_log_prefix: str) -> list[str]:
    root = job_log_prefix.rstrip("/")
    return [uri for suffix in _DRIVER_LOG_CANDIDATES if reader.exists(uri := f"{root}/{suffix}")]


def _read_text(reader: Reader, uri: str) -> str:
    raw = reader.read_bytes(uri)
    if uri.endswith(".gz"):
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", "replace")
