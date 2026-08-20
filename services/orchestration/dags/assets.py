"""ADR-0007에 따라 여러 DAG가 공유하는 Airflow Asset 정의 모음.

standard_score_pipeline(#229)과 zone_weather_pipeline(#230)이 각자 만든 Asset을
발행하면, current_score_pipeline(#231)이 `AssetAny(STANDARD_SCORE_ASSET,
ZONE_WEATHER_ASSET)`으로 두 producer 중 하나만 트리거되어도 깨어난다.

Airflow는 DAG 번들 경로(로컬 배포에서는 services/orchestration/dags) 전체를
sys.path에 등록해 준다(airflow.dag_processing.dagbag.DagBag). 그래서 이 파일은
jobs/ 패키지처럼 별도 PYTHONPATH 배선 없이 각 DAG 파일에서 `from assets import
...`로 바로 가져다 쓸 수 있다 — jobs/는 scheduler 컨테이너에만 마운트되어
dag-processor/webserver의 파싱 시점에는 보이지 않는다(infra/compose/airflow.yaml
참고). 이 모듈은 각 DAG 파일이 파싱 시점(모듈 최상단)에 그대로 가져다 쓸 수
있어야 하므로, Airflow SDK 이외의 무거운 의존성을 두지 않는다.
"""

from __future__ import annotations

from airflow.sdk import Asset

# zone_weather_pipeline(#230)이 detect_changed_zones를 통과했을 때(변경된 zone이
# 있을 때)만 발행한다.
ZONE_WEATHER_ASSET = Asset(name="zone_weather_changed", uri="postgres://latest_zone_weather/")
