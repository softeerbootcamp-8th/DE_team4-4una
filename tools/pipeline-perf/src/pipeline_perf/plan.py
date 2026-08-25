"""Turns a Spark physical plan into something a human can read (#462).

`SparkListenerSQLExecutionStart.description`는 `count at
NativeMethodAccessorImpl.java:0`처럼 py4j 경계에서 끊긴 JVM 호출 지점이라, 그
execution이 무슨 일을 했는지 알려주지 않는다. `details` 스택도 py4j에서 끝나
Python 프레임이 없다(실제 event log로 확인).

대신 `physicalPlanDescription`은 평문으로 남아 있어, 어떤 데이터를 읽고 어떤
연산을 했는지 그대로 읽어낼 수 있다. 다만 실행 하나가 1.2MB에 달해 통째로 보관할
수 없으므로, 파싱 직후 데이터셋·연산자·플랜 앞부분만 남기고 버린다.
"""

from __future__ import annotations

import re
from typing import Any

_PHYSICAL_MARKER = "== Physical Plan =="

# 플랜 본문에 그대로 박히는 데이터 경로.
_S3_URI = re.compile(r"s3[an]?://[^\s,\]\"']+")

# 들여쓰기와 whole-stage codegen 표시(`*(3)`)를 건너뛰고 연산자 이름만 집는다.
_OPERATOR = re.compile(r"(?m)^\s*(?:\+- )?(?:\*\(\d+\) )?([A-Z][A-Za-z]+)")

# 비용을 설명하는 연산자만 고른다. Project·Filter·ColumnarToRow처럼 어느 플랜에나
# 있는 것은 신호가 없어 제외한다. 순서는 읽기 -> 조인 -> ... -> 쓰기로 고정해
# 실행마다 같은 순서로 읽히게 한다.
_NOTABLE_OPERATORS: tuple[tuple[str, str], ...] = (
    ("FileScan", "파일 읽기"),
    ("InMemoryTableScan", "캐시 읽기"),
    ("BroadcastHashJoin", "브로드캐스트 조인"),
    ("ShuffledHashJoin", "셔플 해시 조인"),
    ("SortMergeJoin", "정렬 병합 조인"),
    ("BroadcastNestedLoopJoin", "중첩 루프 조인"),
    ("Window", "윈도우"),
    ("WindowGroupLimit", "윈도우"),
    ("Generate", "explode"),
    ("Sort", "정렬"),
    ("Exchange", "셔플"),
    ("HashAggregate", "집계"),
    ("SortAggregate", "집계"),
    ("ObjectHashAggregate", "집계"),
    ("ArrowEvalPython", "Python UDF"),
    ("BatchEvalPython", "Python UDF"),
    ("InsertIntoHadoopFsRelationCommand", "파일 쓰기"),
)

_HEAD_LINES = 14
_HEAD_LINE_CHARS = 150


def summarize_physical_plan(plan_text: str | None) -> dict[str, Any]:
    """물리 플랜에서 데이터셋·주요 연산·플랜 앞부분을 뽑는다.

    큰 문자열은 호출 즉시 버려지고, 여기서 만든 작은 dict만 수집 결과에 남는다.
    """
    if not plan_text:
        return {"datasets": [], "operators": [], "head": ""}
    physical = plan_text.split(_PHYSICAL_MARKER, 1)[-1]
    return {
        "datasets": _datasets(physical),
        "operators": _operators(physical),
        "head": _head(physical),
    }


def _datasets(physical: str) -> list[str]:
    """플랜이 건드린 데이터 경로를 버킷 없이 짧게 만든다.

    Spark는 경로 목록이 길면 뒤를 `...`로 잘라 두는데, 그 잘린 흔적도 그대로 둔다 —
    지어내는 것보다 잘렸다는 사실이 그대로 보이는 편이 낫다.
    """
    seen = []
    for uri in _S3_URI.findall(physical):
        path = uri.split("://", 1)[1]
        # 버킷 이름은 어느 경로에나 같아서 지운다.
        _, _, key = path.partition("/")
        key = key.rstrip(".") or path
        if key not in seen:
            seen.append(key)
    return seen


def _operators(physical: str) -> list[str]:
    found = set(_OPERATOR.findall(physical))
    labels = []
    for operator, label in _NOTABLE_OPERATORS:
        if operator in found and label not in labels:
            labels.append(label)
    return labels


def _head(physical: str) -> str:
    lines = []
    for line in physical.splitlines():
        if not line.strip():
            continue
        lines.append(line[:_HEAD_LINE_CHARS])
        if len(lines) >= _HEAD_LINES:
            break
    return "\n".join(lines)
