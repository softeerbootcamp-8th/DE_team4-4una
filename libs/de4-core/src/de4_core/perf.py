"""Structured timing logs for non-Spark pipeline phases (#461).

Spark event log는 lazy 실행 구간만 담는다. psycopg2 직접 실행처럼 Spark 밖에서
즉시 실행되는 구간은 event log에 흔적이 없어, 성능 베이스라인(#460)이 그 시간을
보려면 코드가 직접 남기는 수밖에 없다.

**Spark lazy 구간에는 쓰지 않는다.** 시간을 재려고 중간에 action을 강제하면
캐시 없는 재계산과 AQE 무력화로 실행 계획이 바뀌어, 측정하려던 것과 다른
파이프라인을 재게 된다. lazy 구간은 event log의 SQL execution 단위로 본다.

이 모듈이 서비스가 아니라 de4-core에 있는 이유는, 이 로그 한 줄이 세 패키지가
공유하는 포맷 계약이기 때문이다. 생산자가 `services/batch-jobs`와
`services/orchestration` 둘이고 소비자가 `tools/pipeline-perf`(#462)라, 정의가
갈라지면 파서가 조용히 그 구간을 놓친다.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

# 파서가 driver stderr에서 이 접두사로 PERF 라인만 골라낸다(#462).
PERF_LOG_PREFIX = "PERF"


@contextmanager
def perf_phase(
    logger: logging.Logger, phase: str, **fields: object
) -> Iterator[dict[str, object]]:
    """`phase` 구간의 소요 시간을 `PERF <json>` 한 줄로 남긴다.

    `phase`에는 job 접두사를 붙인다(예: `standard_score.postgres_merge`) —
    batch-jobs와 orchestration의 이름이 충돌하지 않고, 파서가 job별로 묶기 쉽다.

    `fields`는 호출 시점에 이미 아는 값을 payload에 함께 싣는다. 비밀값을 넘기지
    않는다 — 이 로그는 S3의 driver stderr에 그대로 남는다.

    yield하는 dict에 값을 넣으면 payload에 함께 실린다. MERGE의 영향 행수처럼
    실행이 끝나야 아는 값을 담기 위한 것이다.
    """
    started = time.monotonic()
    added: dict[str, object] = {}
    ok = True
    try:
        yield added
    except BaseException:
        # 실패한 구간도 남긴다 — 오래 걸리다 실패한 구간이 베이스라인에서
        # 가장 먼저 봐야 할 대상이다. 예외는 그대로 흘려보낸다.
        ok = False
        raise
    finally:
        payload = {
            "phase": phase,
            "elapsed_s": round(time.monotonic() - started, 3),
            "ok": ok,
            **fields,
            **added,
        }
        logger.info("%s %s", PERF_LOG_PREFIX, json.dumps(payload))
