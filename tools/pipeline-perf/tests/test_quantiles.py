from pipeline_perf.quantiles import DurationSummary, percentile, skew_ratio


def test_percentile_interpolates_between_samples():
    assert percentile([10, 20, 30, 40], 0.0) == 10
    assert percentile([10, 20, 30, 40], 0.5) == 25
    assert percentile([10, 20, 30, 40], 1.0) == 40


def test_percentile_of_empty_sample_is_none():
    assert percentile([], 0.5) is None


def test_summary_reports_count_and_quantiles():
    summary = DurationSummary()
    for value in (2000, 2000, 8000):
        summary.add(value)

    assert summary.summary() == {
        "count": 3,
        "p50_ms": 2000,
        "p95_ms": 7400,
        "max_ms": 8000,
        "sum_ms": 12000,
    }


def test_skew_ratio_is_none_when_median_is_zero():
    assert skew_ratio(8000, 2000) == 4.0
    assert skew_ratio(8000, 0) is None
    assert skew_ratio(None, 2000) is None
