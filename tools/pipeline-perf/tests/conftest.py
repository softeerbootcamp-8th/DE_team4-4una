import shutil
import sys
from pathlib import Path

import pytest

TESTS = Path(__file__).parent
FIXTURES = TESTS / "fixtures"

# fakes.py / scenario.py를 패키지로 만들지 않고 테스트끼리 공유한다.
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))


@pytest.fixture
def job_run_dir() -> Path:
    """축약 Spark event log와 driver stderr가 들어 있는 Job Run 로그 트리."""
    return FIXTURES / "job-run"


@pytest.fixture
def lake(tmp_path, job_run_dir):
    """`log_uri`가 가리키는 EMR Serverless 로그 트리와 Bronze 파티션을 깐다."""
    from scenario import APPLICATION_ID, JOB_RUN_ID

    job_logs = tmp_path / "logs" / "applications" / APPLICATION_ID / "jobs" / JOB_RUN_ID
    shutil.copytree(job_run_dir, job_logs)
    partition = tmp_path / "bronze" / "event_date=2026-08-25" / "hour=02"
    partition.mkdir(parents=True)
    (partition / "part-0.parquet").write_bytes(b"x" * 1024)
    (partition / "part-1.parquet").write_bytes(b"y" * 3072)
    return tmp_path
