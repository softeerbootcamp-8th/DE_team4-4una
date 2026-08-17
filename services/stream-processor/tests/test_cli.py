import signal
from types import SimpleNamespace

import pytest
from stream_processor.cli import install_shutdown_handler


@pytest.fixture(autouse=True)
def _restore_signal_handlers():
    # 테스트가 등록한 핸들러가 이후 테스트나 pytest 자체의 SIGINT 처리에 새어나가지 않도록
    # 원래 핸들러를 저장해두고 테스트가 끝나면 되돌린다.
    original_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    yield
    for sig, handler in original_handlers.items():
        signal.signal(sig, handler)


@pytest.mark.parametrize("triggered_signal", [signal.SIGINT, signal.SIGTERM])
def test_install_shutdown_handler_stops_query_before_returning(triggered_signal) -> None:
    calls = []
    query = SimpleNamespace(stop=lambda: calls.append("stopped"))

    install_shutdown_handler(query)
    handler = signal.getsignal(triggered_signal)
    handler(triggered_signal, None)

    assert calls == ["stopped"]
