-- standard_segment_comfort_score / zone_weather_snapshot /
-- current_segment_comfort_score: 날씨 반영 comfort score 계약 (#196,
-- context/data/schema-catalog.md의 동명 절, docs/adr/0005-zone-master-
-- canonical-geometry-reference.md).
--
-- 기존 segment_comfort_score는 그대로 둔다 — comfort-score.md "Migration
-- order"의 전환이 끝나기 전까지 병행 운영한다.
--
-- road_segment/zone_master는 로컬 Parquet라 DB FK를 걸 수 없다 — segment_id/
-- location_id는 여기서는 논리적 참조로만 남는다(zone_master 쪽은 ADR 0005).
CREATE TABLE standard_segment_comfort_score (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL REFERENCES vehicle_profile (vehicle_profile_id),
    score_as_of TIMESTAMPTZ NOT NULL,
    data_period_start TIMESTAMPTZ,
    data_period_end TIMESTAMPTZ,
    vertical_score DOUBLE PRECISION NOT NULL,
    longitudinal_score DOUBLE PRECISION NOT NULL,
    lateral_score DOUBLE PRECISION NOT NULL,
    comfort_score DOUBLE PRECISION NOT NULL CHECK (comfort_score BETWEEN 0 AND 100),
    sample_count BIGINT NOT NULL CHECK (sample_count >= 0),
    qualifying_hour_count INTEGER NOT NULL CHECK (qualifying_hour_count >= 0),
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    score_version TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (segment_id, vehicle_profile_id, score_as_of),
    -- N=0이면 rollup할 qualifying hour가 없어 둘 다 NULL, 있으면 둘 다 값이 있다
    -- (schema-catalog.md "standard_segment_comfort_score").
    CHECK ((data_period_start IS NULL) = (data_period_end IS NULL))
);

-- location_id는 zone_master.location_id(로컬 Parquet)를 논리적으로만
-- 참조한다 — zone_master가 PostgreSQL이 아니라서 DB FK를 걸 수 없다.
CREATE TABLE zone_weather_snapshot (
    location_id INTEGER NOT NULL,
    weather_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    temperature_2m_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    rain_mm DOUBLE PRECISION,
    snowfall_cm DOUBLE PRECISION,
    visibility_m DOUBLE PRECISION,
    wind_speed_10m_mps DOUBLE PRECISION,
    wind_gusts_10m_mps DOUBLE PRECISION,
    weather_code INTEGER,
    weather_state TEXT NOT NULL,
    impact_signature TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (location_id, weather_time)
);

-- weather_time/weather_rule_version은 그 존의 첫 날씨 수집 전까지 둘 다
-- NULL이고(이 경우 세 방향/최종 점수는 standard 값 그대로), 날씨가 반영된
-- 뒤에는 둘 다 값이 있다.
CREATE TABLE current_segment_comfort_score (
    segment_id TEXT NOT NULL,
    vehicle_profile_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    standard_score_as_of TIMESTAMPTZ NOT NULL,
    weather_time TIMESTAMPTZ,
    data_period_start TIMESTAMPTZ,
    vertical_score DOUBLE PRECISION NOT NULL,
    longitudinal_score DOUBLE PRECISION NOT NULL,
    lateral_score DOUBLE PRECISION NOT NULL,
    comfort_score DOUBLE PRECISION NOT NULL CHECK (comfort_score BETWEEN 0 AND 100),
    sample_count BIGINT NOT NULL CHECK (sample_count >= 0),
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    standard_score_version TEXT NOT NULL,
    weather_rule_version TEXT,
    calculated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (segment_id, vehicle_profile_id),
    CHECK ((weather_time IS NULL) = (weather_rule_version IS NULL)),
    FOREIGN KEY (segment_id, vehicle_profile_id, standard_score_as_of)
        REFERENCES standard_segment_comfort_score (segment_id, vehicle_profile_id, score_as_of),
    FOREIGN KEY (location_id, weather_time)
        REFERENCES zone_weather_snapshot (location_id, weather_time)
);
