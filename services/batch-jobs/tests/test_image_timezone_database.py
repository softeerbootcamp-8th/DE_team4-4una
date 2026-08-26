"""batch-jobs 이미지에 tz 데이터베이스가 포함되는지 확인한다(#582).

map matching이 mapInPandas로 timestamp 컬럼까지 Python worker에 넘기면서(#571), Arrow
배치를 pandas로 바꿀 때 pyarrow가 tz 이름("UTC")을 해석해야 한다. pandas는 tzdata를
Windows/emscripten에서만 요구하고 pytz에는 의존하지 않으므로 Linux 이미지에는 Python이
읽을 tz DB가 하나도 없을 수 있고, 그 상태의 변환은 ArrowInvalid로 실패한다.
"""

import shutil
import subprocess
import sys
import zoneinfo
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

# Dockerfile이 이미지에 설치할 의존성 목록을 만들 때 쓰는 것과 같은 인자다.
EXPORT_COMMAND = [
    "uv",
    "export",
    "--frozen",
    "--package",
    "batch-jobs",
    "--no-dev",
    "--no-emit-project",
    "--no-emit-package",
    "de4-core",
    "--no-emit-package",
    "pyspark",
    "--no-emit-package",
    "py4j",
    "--no-hashes",
]


class _BlockPytz:
    """pytz import를 막아 pytz가 없는 이미지 환경을 흉내낸다."""

    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] == "pytz":
            raise ImportError("pytz is not installed in the batch-jobs image")
        return None


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv CLI가 필요합니다")
def test_image_requirements_include_timezone_database():
    result = subprocess.run(
        EXPORT_COMMAND,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    tzdata_lines = [
        line for line in result.stdout.splitlines() if line.startswith("tzdata==")
    ]
    assert tzdata_lines, "batch-jobs 런타임 의존성에 tzdata가 없어 이미지에 tz DB가 없다"

    # "tzdata==2026.3 ; sys_platform == 'win32'"처럼 마커가 붙으면 Linux 이미지에서 빠진다.
    marked = [line for line in tzdata_lines if ";" in line]
    assert not marked, f"tzdata가 플랫폼 마커 때문에 Linux 이미지에서 빠진다: {marked}"


def test_tz_aware_timestamp_converts_without_system_tz_database(monkeypatch):
    """OS tz DB도 pytz도 없는 EMR worker 조건에서 Arrow -> pandas 변환이 되어야 한다."""
    monkeypatch.delitem(sys.modules, "pytz", raising=False)
    monkeypatch.setattr(sys, "meta_path", [_BlockPytz(), *sys.meta_path])
    # zoneinfo는 TZPATH가 비면 tzdata 패키지로 넘어간다. 컨테이너에 /usr/share/zoneinfo가
    # 없는 상황을 이 두 줄로 재현한다.
    zoneinfo.reset_tzpath(to=[])
    zoneinfo.ZoneInfo.clear_cache()
    try:
        column = pa.chunked_array(
            [pa.array([datetime(2026, 8, 26, 11, 0, tzinfo=UTC)])],
            type=pa.timestamp("us", tz="UTC"),
        )

        converted = column.to_pandas()

        assert str(converted.dt.tz) == "UTC"
    finally:
        zoneinfo.reset_tzpath()
        zoneinfo.ZoneInfo.clear_cache()
