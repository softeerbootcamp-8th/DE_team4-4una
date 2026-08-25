from datetime import UTC, datetime

from pipeline_perf.emr import describe_job_run, find_job_run_id_by_name, job_log_prefix


class FakeEmrClient:
    def __init__(self, job_run=None, job_runs=None):
        self._job_run = job_run or {}
        self._job_runs = job_runs or []
        self.list_calls = []

    def get_job_run(self, **kwargs):
        return {"jobRun": self._job_run}

    def list_job_runs(self, **kwargs):
        self.list_calls.append(kwargs)
        return {"jobRuns": self._job_runs}


def test_describe_job_run_splits_provisioning_wait_from_run_time():
    client = FakeEmrClient(
        {
            "jobRunId": "00abc",
            "applicationId": "00app",
            "name": "run_sensor_processing",
            "state": "SUCCESS",
            "createdAt": datetime(2026, 8, 25, 2, 0, 0, tzinfo=UTC),
            "startedAt": datetime(2026, 8, 25, 2, 1, 30, tzinfo=UTC),
            "endedAt": datetime(2026, 8, 25, 2, 21, 30, tzinfo=UTC),
            "totalExecutionDurationSeconds": 1200,
            "queuedDurationMilliseconds": 4200,
            "billedResourceUtilization": {
                "vCPUHour": 1.25,
                "memoryGBHour": 5.0,
                "storageGBHour": 0.4,
            },
        }
    )

    facts = describe_job_run(client, "00app", "00abc")

    assert facts["provisioning_wait_s"] == 90.0
    assert facts["run_duration_s"] == 1200.0
    assert facts["queued_duration_s"] == 4.2
    assert facts["billed_vcpu_hour"] == 1.25


def test_describe_job_run_tolerates_missing_time_fields():
    facts = describe_job_run(FakeEmrClient({"state": "RUNNING"}), "00app", "00abc")

    assert facts["provisioning_wait_s"] is None
    assert facts["run_duration_s"] is None
    assert facts["job_run_id"] == "00abc"


def test_name_match_fallback_picks_the_run_closest_to_the_task_start():
    started = datetime(2026, 8, 25, 2, 0, 0, tzinfo=UTC)
    client = FakeEmrClient(
        job_runs=[
            {"id": "far", "name": "run_hourly_scoring", "createdAt": datetime(2026, 8, 25, 2, 6, tzinfo=UTC)},
            {"id": "near", "name": "run_hourly_scoring", "createdAt": datetime(2026, 8, 25, 2, 1, tzinfo=UTC)},
            {"id": "other", "name": "run_standard_score", "createdAt": started},
        ]
    )

    assert find_job_run_id_by_name(client, "00app", "run_hourly_scoring", started) == "near"
    assert client.list_calls[0]["applicationId"] == "00app"


def test_name_match_fallback_returns_none_when_nothing_matches():
    client = FakeEmrClient(job_runs=[])

    assert (
        find_job_run_id_by_name(client, "00app", "x", datetime(2026, 8, 25, tzinfo=UTC)) is None
    )


def test_job_log_prefix_matches_the_emr_serverless_layout():
    assert (
        job_log_prefix("s3://bucket/emr-serverless/logs/", "00app", "00run")
        == "s3://bucket/emr-serverless/logs/applications/00app/jobs/00run"
    )
