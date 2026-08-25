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

        store.record_attempt("fp-1", "restart_stream_processor")
        assert store.should_attempt("fp-1", cooldown_seconds=600) is False

        clock.now += 599
        assert store.should_attempt("fp-1", cooldown_seconds=600) is False

        clock.now += 2
        assert store.should_attempt("fp-1", cooldown_seconds=600) is True

    def test_different_fingerprints_do_not_block_each_other(self, tmp_path):
        store = IncidentStore(str(tmp_path / "incidents.sqlite3"))

        store.record_attempt("fp-1", "restart_stream_processor")

        assert store.should_attempt("fp-2", cooldown_seconds=600) is True

    def test_state_survives_reopening_the_same_file(self, tmp_path):
        path = str(tmp_path / "incidents.sqlite3")
        clock = _Clock()
        IncidentStore(path, now=clock).record_attempt("fp-1", "restart_stream_processor")

        reopened = IncidentStore(path, now=clock)

        assert reopened.should_attempt("fp-1", cooldown_seconds=600) is False
