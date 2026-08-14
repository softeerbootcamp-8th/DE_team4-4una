import os
import time

import pytest
from pyspark.sql import SparkSession

# collect()가 돌려주는 timestamp는 spark.sql.session.timeZone이 아니라 이 파이썬 프로세스의
# 로컬 타임존으로 변환된다. 고정하지 않으면 실행 머신마다(예: Asia/Seoul vs UTC) 값이 달라지므로
# SparkSession을 만들기 전에 프로세스 타임존 자체를 UTC로 못박는다.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    # 세션 전체에서 재사용: SparkSession 기동에 몇 초가 걸려서 테스트마다 새로 만들면 느려진다.
    # local[1]이라 실제 클러스터나 Kafka 브로커 없이 순수 DataFrame 연산만 검증한다.
    session = (
        SparkSession.builder.appName("stream-processor-tests")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
