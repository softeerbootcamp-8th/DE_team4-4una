from __future__ import annotations

from ops_agent.incident_store import IncidentStore


class _Clock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class TestIncidentStore:
    def test_a_fingerprint_seen_for_the_first_time_may_be_attempted(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        assert store.should_attempt("fp-1", cooldown_seconds=600) is True

    def test_a_fingerprint_attempted_recently_is_blocked_until_cooldown_passes(self, tmp_path):
        clock = _Clock()
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"), now=clock)

        store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        assert store.should_attempt("fp-1", cooldown_seconds=600) is False

        clock.now += 599
        assert store.should_attempt("fp-1", cooldown_seconds=600) is False

        clock.now += 2
        assert store.should_attempt("fp-1", cooldown_seconds=600) is True

    def test_different_fingerprints_do_not_block_each_other(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")

        assert store.should_attempt("fp-2", cooldown_seconds=600) is True

    def test_state_survives_reopening_the_same_file(self, tmp_path):
        path = str(tmp_path / "incidents.sqlite3")
        clock = _Clock()
        IncidentStore(path, now=clock).record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")

        reopened = IncidentStore(path, now=clock)

        assert reopened.should_attempt("fp-1", cooldown_seconds=600) is False

    def test_attempts_accumulate_instead_of_overwriting(self, tmp_path):
        clock = _Clock()
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"), now=clock)

        for _ in range(3):
            store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
            clock.now += 1

        assert store.count_recent("fp-1", within_seconds=600) == 3

    def test_count_recent_ignores_attempts_outside_the_window(self, tmp_path):
        clock = _Clock()
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"), now=clock)

        store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        clock.now += 700

        assert store.count_recent("fp-1", within_seconds=600) == 0

    def test_count_recent_is_scoped_to_one_fingerprint(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        store.record_attempt("fp-2", "StreamProcessorDown", "restart_stream_processor")

        assert store.count_recent("fp-1", within_seconds=600) == 1

    def test_the_outcome_is_recorded_against_the_attempt_row(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        event_id = store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")
        store.record_outcome(event_id, succeeded=True, recovered=False)

        assert store.read_event(event_id) == {
            "fingerprint": "fp-1",
            "alertname": "StreamProcessorDown",
            "action": "restart_stream_processor",
            "succeeded": True,
            "recovered": False,
        }

    def test_an_attempt_that_never_reports_an_outcome_leaves_recovered_unknown(self, tmp_path):
        # 조치 도중 ops-agent가 죽은 경우다. cooldown은 이미 소진돼야 하고 recovered는 NULL로 남는다.
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        event_id = store.record_attempt("fp-1", "StreamProcessorDown", "restart_stream_processor")

        assert store.should_attempt("fp-1", cooldown_seconds=600) is False
        assert store.read_event(event_id)["recovered"] is None
