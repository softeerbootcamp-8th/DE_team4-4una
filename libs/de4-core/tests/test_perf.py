"""Tests for de4_core.perf's perf_phase() (#461)."""

from __future__ import annotations

import json
import logging

import pytest
from de4_core.perf import PERF_LOG_PREFIX, perf_phase

_LOGGER_NAME = "test_perf_phase"


def _perf_payloads(caplog) -> list[dict]:
    """caplog에 쌓인 로그 중 PERF 라인만 골라 JSON 부분을 파싱한다.

    수집 스크립트(#462)가 driver stderr에서 하는 일과 같은 방식으로 뽑아, 파서가
    실제로 읽을 수 있는 형태인지를 테스트가 함께 보장한다.
    """
    payloads = []
    for message in caplog.messages:
        if not message.startswith(f"{PERF_LOG_PREFIX} "):
            continue
        payloads.append(json.loads(message[len(PERF_LOG_PREFIX) + 1 :]))
    return payloads


def test_perf_phase_logs_prefixed_json_with_phase_and_elapsed(caplog) -> None:
    logger = logging.getLogger(_LOGGER_NAME)

    with (
        caplog.at_level(logging.INFO, logger=_LOGGER_NAME),
        perf_phase(logger, "standard_score.postgres_merge"),
    ):
        pass

    (payload,) = _perf_payloads(caplog)
    assert payload["phase"] == "standard_score.postgres_merge"
    assert payload["ok"] is True
    assert isinstance(payload["elapsed_s"], float)
    assert payload["elapsed_s"] >= 0.0


def test_perf_phase_includes_fields_given_at_the_call_site(caplog) -> None:
    logger = logging.getLogger(_LOGGER_NAME)

    with (
        caplog.at_level(logging.INFO, logger=_LOGGER_NAME),
        perf_phase(logger, "standard_score.staging_truncate", table="staging"),
    ):
        pass

    (payload,) = _perf_payloads(caplog)
    assert payload["table"] == "staging"


def test_perf_phase_includes_fields_added_inside_the_context(caplog) -> None:
    """MERGE의 영향 행수처럼 실행이 끝나야 아는 값을 담을 수 있어야 한다."""
    logger = logging.getLogger(_LOGGER_NAME)

    with (
        caplog.at_level(logging.INFO, logger=_LOGGER_NAME),
        perf_phase(logger, "standard_score.postgres_merge") as fields,
    ):
        fields["rows"] = 173402

    (payload,) = _perf_payloads(caplog)
    assert payload["rows"] == 173402


def test_perf_phase_marks_failure_and_reraises(caplog) -> None:
    """오래 걸리고 실패한 구간이야말로 베이스라인에서 봐야 할 대상이다."""
    logger = logging.getLogger(_LOGGER_NAME)

    with (
        caplog.at_level(logging.INFO, logger=_LOGGER_NAME),
        pytest.raises(ValueError, match="staging has duplicates"),
        perf_phase(logger, "standard_score.staging_validate"),
    ):
        raise ValueError("staging has duplicates")

    (payload,) = _perf_payloads(caplog)
    assert payload["phase"] == "standard_score.staging_validate"
    assert payload["ok"] is False


def test_perf_phase_is_exported_from_the_package_root() -> None:
    """batch-jobs/orchestration이 `from de4_core import perf_phase`로 쓴다."""
    import de4_core

    assert de4_core.perf_phase is perf_phase
    assert "perf_phase" in de4_core.__all__
