-- standard_segment_comfort_score를 확정 스키마에 맞춘다 (#198).
--
-- 0006은 이미 적용된 migration이라 직접 수정하지 않고 여기서 변경한다.
-- 확정 스키마와의 차이는 세 가지다.
--   1. data_period_start/end가 NOT NULL이다 — N=0이어도 배치 윈도우
--      [as_of - window_hours, as_of)로 채워 넣으므로 NULL이 나올 수 없다.
--      따라서 둘의 NULL 짝을 강제하던 테이블 레벨 CHECK도 의미가 없어진다.
--   2. qualifying_hour_count를 저장하지 않는다 — N은 confidence_score와
--      고정된 k로부터 N = k * C / (1 - C)로 복원할 수 있고, N=0은
--      confidence_score = 0으로 식별된다.
--   3. staging 테이블이 필요하다 (아래).
--
-- standard_segment_comfort_score_check는 0006의 이름 없는 테이블 레벨
-- CHECK에 PostgreSQL이 부여한 이름이다 (컬럼 레벨 CHECK는
-- <table>_<column>_check로 따로 이름이 붙으므로 겹치지 않는다).
ALTER TABLE standard_segment_comfort_score
    DROP CONSTRAINT standard_segment_comfort_score_check;

-- 아직 이 테이블에 쓰는 경로가 없어 기존 행이 없다 — SET NOT NULL이
-- 실패할 여지가 없다.
ALTER TABLE standard_segment_comfort_score
    ALTER COLUMN data_period_start SET NOT NULL,
    ALTER COLUMN data_period_end SET NOT NULL,
    DROP COLUMN qualifying_hour_count;

-- Spark JDBC write의 staging 대상. 0002의 segment_comfort_score_staging과
-- 같은 이유로 본 테이블과 동일 타입으로 명시 생성한다 — overwrite 모드는
-- 테이블이 없으면 그냥 CREATE해버리고, 그 순간 타입은 Spark 추론값이 된다.
-- 의도적으로 PK/UNIQUE/FK가 없다: 중복 유입은 MERGE 직전 애플리케이션
-- 단(comfort_score/standard_writer.py)에서 검증한다.
CREATE TABLE standard_segment_comfort_score_staging (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    score_as_of TIMESTAMPTZ NOT NULL,
    data_period_start TIMESTAMPTZ NOT NULL,
    data_period_end TIMESTAMPTZ NOT NULL,
    vertical_score DOUBLE PRECISION NOT NULL,
    longitudinal_score DOUBLE PRECISION NOT NULL,
    lateral_score DOUBLE PRECISION NOT NULL,
    comfort_score DOUBLE PRECISION NOT NULL,
    sample_count BIGINT NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    score_version TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL
);
