-- segment_comfort_score: Gold 테이블 (#129). 컬럼 범위는 #127(formula.py)
-- 출력과 정확히 일치하는 MVP 범위다. 근거:
-- docs/superpowers/specs/2026-08-16-segment-comfort-score-gold-load-design.md §1
CREATE TABLE segment_comfort_score (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL REFERENCES vehicle_profile (vehicle_profile_id),
    comfort_score DOUBLE PRECISION NOT NULL CHECK (comfort_score BETWEEN 0 AND 100),
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    sample_count BIGINT NOT NULL CHECK (sample_count >= 0),
    score_version TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (segment_id, vehicle_profile_id)
);

-- Spark JDBC write의 staging 대상. 본 테이블과 동일 타입으로 명시 생성해서
-- Spark의 타입 추론에 맡기지 않는다 (overwrite 모드는 테이블이 없으면
-- 그냥 CREATE해버리고, 그 순간 타입은 Spark 추론값이 된다). 의도적으로
-- PK/UNIQUE가 없다 — 중복 유입은 여기서 막지 않고 MERGE 직전
-- 애플리케이션 단(comfort_score/gold_writer.py)에서 검증한다.
CREATE TABLE segment_comfort_score_staging (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    comfort_score DOUBLE PRECISION NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    sample_count BIGINT NOT NULL,
    score_version TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL
);
