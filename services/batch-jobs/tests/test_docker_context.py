"""루트 .dockerignore가 로컬 전용 디렉터리를 docker build context에서 제외하는지 확인한다.

실제 저장소 상태(.worktrees 존재 여부 등)에 의존하지 않도록, 루트 .dockerignore를
합성 context에 복사해 docker buildx의 로컬 출력으로 실제 필터링 결과를 검증한다.
"""

import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
EXCLUDED_DIRS = [".worktrees", ".omc", ".superpowers", ".idea", ".claude"]


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI가 필요합니다")
def test_dockerignore_excludes_local_only_dirs(tmp_path):
    context = tmp_path / "context"
    context.mkdir()
    shutil.copy(REPO_ROOT / ".dockerignore", context / ".dockerignore")
    (context / "app.py").write_text("print('hi')\n")
    for name in EXCLUDED_DIRS:
        nested = context / name / "nested"
        nested.mkdir(parents=True)
        (nested / "file.txt").write_text("dummy\n")

    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM scratch\nCOPY . /\n")
    out_dir = tmp_path / "out"

    result = subprocess.run(
        [
            "docker",
            "buildx",
            "build",
            "--file",
            str(dockerfile),
            "--output",
            f"type=local,dest={out_dir}",
            str(context),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    leaked = [name for name in EXCLUDED_DIRS if (out_dir / name).exists()]
    assert not leaked, f"docker build context에 로컬 전용 디렉터리가 포함됨: {leaked}"
    assert (out_dir / "app.py").exists()
