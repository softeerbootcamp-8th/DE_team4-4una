"""Postgres advisory-lock 키 레지스트리.

이 저장소에서 pg_advisory_lock을 쓰는 곳은 여기 상수를 통해서만 키를
가져온다. 직접 정수를 하드코딩하면 다른 사용처와 우연히 같은 값을 골라
서로 다른 락이 같은 자원인 것처럼 충돌할 수 있다.
"""

from __future__ import annotations

# batch_jobs.migrate가 마이그레이션 적용 중 동시 실행을 막는 데 쓴다.
MIGRATION_LOCK_KEY = 1001

# comfort_score.gold_writer가 staging 테이블 write~MERGE 구간을 보호하는 데 쓴다.
GOLD_JOB_STAGING_LOCK_KEY = 1002

# comfort_score.standard_writer가 standard staging 테이블 write~MERGE 구간을
# 보호하는 데 쓴다. Gold와 키를 나눠야 두 실행이 서로를 막지 않는다 (#198).
STANDARD_JOB_STAGING_LOCK_KEY = 1003
