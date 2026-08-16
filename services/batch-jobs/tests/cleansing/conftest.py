import os
import time

import pytest
from pyspark.sql import SparkSession

# collect()의 timestamp는 프로세스 로컬 타임존으로 변환되므로 UTC로 고정한다.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("batch-jobs-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
