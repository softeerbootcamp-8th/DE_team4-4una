import json

import pytest
from pipeline_perf.cli import main
from scenario import build_collector


@pytest.fixture
def collected_file(tmp_path, lake):
    path = tmp_path / "collect-1.json"
    path.write_text(
        json.dumps(build_collector(lake).collect(), ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return path


def test_render_writes_a_markdown_report(tmp_path, collected_file):
    out = tmp_path / "docs" / "report.md"

    assert main(["render", str(collected_file), "--title", "베이스라인", "-o", str(out)]) == 0
    assert out.read_text(encoding="utf-8").startswith("# 베이스라인")


def test_render_defaults_to_stdout(capsys, collected_file):
    main(["render", str(collected_file)])

    assert "## 8. 관찰된 병목 후보" in capsys.readouterr().out


def test_render_merges_several_collections(capsys, collected_file):
    main(["render", str(collected_file), str(collected_file)])

    # 같은 파일을 두 번 넘기면 run도 두 건으로 잡힌다.
    assert "| DAG run 수 | 2 |" in capsys.readouterr().out


def test_compare_prints_a_delta_table(capsys, collected_file):
    assert main(["compare", "--before", str(collected_file), "--after", str(collected_file)]) == 0
    assert "| 지표 | before | after | 델타 | 변화율 |" in capsys.readouterr().out


def test_collect_requires_a_dag_id():
    with pytest.raises(SystemExit):
        main(["collect"])
