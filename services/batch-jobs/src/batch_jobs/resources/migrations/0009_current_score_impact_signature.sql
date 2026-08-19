-- current_segment_comfort_score에 마지막으로 적용한 날씨 조건을 남긴다 (#216).
--
-- latest_zone_weather는 zone당 1행만 유지하므로(#209) 수집 job이 UPSERT하는 순간
-- 직전 impact_signature는 사라진다. "15분 전과 달라졌는지"를 비교할 값이 어디에도
-- 남지 않아, 재계산 job이 자기가 마지막으로 적용한 값을 소비자 쪽에 기록한다.
-- 이렇게 두면 tick을 놓치거나 재시도로 중복 실행돼도 현재 상태만 보고 복구된다.
ALTER TABLE current_segment_comfort_score
    ADD COLUMN weather_impact_signature TEXT;

-- 아직 그 zone의 날씨를 받지 못한 행은 적용한 조건도 없다. 0006이
-- (weather_time, weather_rule_version)에 걸어둔 짝 규칙을 같은 형태로 확장한다.
ALTER TABLE current_segment_comfort_score
    ADD CONSTRAINT current_segment_comfort_score_impact_signature_check
        CHECK ((weather_time IS NULL) = (weather_impact_signature IS NULL));

-- 15분 실행은 날씨가 바뀐 zone의 행만 읽고 쓴다. PK가
-- (segment_id, vehicle_profile_id)라 location_id 조건에 쓸 인덱스가 없어 추가한다.
CREATE INDEX current_segment_comfort_score_location_id_idx
    ON current_segment_comfort_score (location_id);
