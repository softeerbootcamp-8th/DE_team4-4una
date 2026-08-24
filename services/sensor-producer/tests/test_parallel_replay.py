from queue import Empty
from types import SimpleNamespace

import pytest
from sensor_producer.parallel_replay import _next_message


class EmptyStatusQueue:
    def get(self, timeout: int):
        assert timeout == 1
        raise Empty


def test_next_message_reports_failed_worker_without_waiting_for_others() -> None:
    processes = [
        SimpleNamespace(name="worker-0", exitcode=-9, is_alive=lambda: False),
        SimpleNamespace(name="worker-1", exitcode=None, is_alive=lambda: True),
    ]

    with pytest.raises(RuntimeError, match="worker-0"):
        _next_message(EmptyStatusQueue(), processes)
