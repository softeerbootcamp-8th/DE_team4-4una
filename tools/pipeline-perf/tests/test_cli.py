import json

import pytest
from pipeline_perf.cli import _build_parser, main
from scenario import RUN_ID, build_collector


def _dump(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")
    return path


@pytest.fixture
def collected_file(tmp_path, lake):
    return _dump(tmp_path / "collect-1.json", build_collector(lake).collect())


@pytest.fixture
def single_run_file(tmp_path, lake):
    return _dump(
        tmp_path / "collect-single.json", build_collector(lake, run_ids=(RUN_ID,)).collect()
    )


def _collect_args(argv):
    return _build_parser().parse_args(["collect", "--dag-id", "standard_score_pipeline", *argv])


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


def test_collect_parses_the_run_selectors():
    args = _collect_args(
        ["--run-id", "run-1", "--run-id", "run-2"]
        + ["--since", "2026-08-25T09:00:00", "--until", "2026-08-25T10:00:00Z"]
    )

    assert args.run_ids == ["run-1", "run-2"]
    # 시간대를 안 붙인 값은 UTC로 읽는다 — 파이프라인의 시각 표기가 전부 UTC다.
    assert args.since == "2026-08-25T09:00:00+00:00"
    assert args.until == "2026-08-25T10:00:00+00:00"
    with pytest.raises(SystemExit):  # 형식 오류는 요청을 보내기 전에 끊는다
        _collect_args(["--since", "어제"])


def test_a_single_run_collection_renders_and_compares(capsys, single_run_file):
    """지목 수집이 목록 수집과 같은 리포트를 만들어야 최적화 전후를 같은 눈으로 본다."""
    main(["render", str(single_run_file)])
    out = capsys.readouterr().out
    assert "| DAG run 수 | 1 |" in out
    for section in range(1, 9):
        assert f"\n## {section}. " in out

    main(["compare", "--before", str(single_run_file), "--after", str(single_run_file)])
    assert "| 지표 | before | after | 델타 | 변화율 |" in capsys.readouterr().out
