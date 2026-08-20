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

URI 스킴은 일부러 `postgres://`를 쓰지 않는다 — apache/airflow:3.3.1 공식 이미지에는
apache-airflow-providers-postgres가 기본 포함돼 있고, 이 provider가 `postgres`/
`postgresql` 스킴에 대해 `postgres://host:port/database/schema/table` 형태를
강제하는 URI sanitizer를 등록해 둔다(airflow.providers.postgres.assets.postgres).
이 Asset은 실제 DB 연결 정보가 아니라 스케줄링용 식별자일 뿐이라 그 형식을 맞출
이유가 없고, 로컬 uv 가상환경에는 이 provider가 없어 유닛 테스트로는 이 충돌이
잡히지 않는다(실제로 로컬 Airflow 컨테이너에 올려 dag-processor 파싱 오류로
발견했다) — 그래서 provider가 손대지 않는 스킴을 쓴다.
"""

from __future__ import annotations

from airflow.sdk import Asset

# zone_weather_pipeline(#230)이 detect_changed_zones를 통과했을 때(변경된 zone이
# 있을 때)만 발행한다.
ZONE_WEATHER_ASSET = Asset(name="zone_weather_changed", uri="asset://zone-weather/changed-zones")
