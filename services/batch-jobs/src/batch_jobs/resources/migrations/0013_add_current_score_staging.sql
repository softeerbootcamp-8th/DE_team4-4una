-- execute_values 반복 UPSERT 대신 COPY + 단일 MERGE로 쓰기 위한 staging이다(#559, standard_segment_comfort_score_staging과 같은 패턴).
-- PK/UNIQUE/FK가 없는 건 한 실행 안에서 (segment_id, vehicle_profile_id)당 한 번만 계산돼 중복 유입이 없기 때문이다.
CREATE TABLE current_segment_comfort_score_staging (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    standard_score_as_of TIMESTAMPTZ NOT NULL,
    weather_time TIMESTAMPTZ,
    data_period_start TIMESTAMPTZ,
    vertical_score DOUBLE PRECISION NOT NULL,
    longitudinal_score DOUBLE PRECISION NOT NULL,
    lateral_score DOUBLE PRECISION NOT NULL,
    comfort_score DOUBLE PRECISION NOT NULL,
    sample_count BIGINT NOT NULL,
    confidence_score DOUBLE PRECISION NOT NULL,
    standard_score_version TEXT NOT NULL,
    weather_rule_version TEXT,
    weather_impact_signature TEXT,
    calculated_at TIMESTAMPTZ NOT NULL
);
