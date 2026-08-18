-- vehicle_profile을 제조사/모델 대신 차체 유형 x 크기 등급 + 반응계수로
-- 재정의한다 (#170). 이후 vehicle_profile_id 1~5의 의미가 바뀌므로 과거
-- 더미 데이터는 재생성해야 한다.
ALTER TABLE vehicle_profile
    RENAME COLUMN vehicle_class TO body_type;

ALTER TABLE vehicle_profile
    DROP COLUMN manufacturer,
    DROP COLUMN model_name,
    DROP COLUMN mass_kg,
    DROP COLUMN wheelbase_mm,
    DROP COLUMN suspension_type,
    DROP COLUMN vertical_weight,
    DROP COLUMN longitudinal_weight,
    DROP COLUMN lateral_weight;

ALTER TABLE vehicle_profile
    ADD COLUMN size_class TEXT,
    ADD COLUMN vertical_response_factor DOUBLE PRECISION,
    ADD COLUMN longitudinal_response_factor DOUBLE PRECISION,
    ADD COLUMN lateral_response_factor DOUBLE PRECISION,
    ADD COLUMN damping_factor DOUBLE PRECISION,
    ADD COLUMN steering_vibration_factor DOUBLE PRECISION,
    ADD COLUMN profile_version TEXT;

-- sentinel(0)은 차량 구분 없는 집계용이라 반응계수를 중립값(1.0)으로 채운다.
UPDATE vehicle_profile
SET size_class = 'ALL',
    vertical_response_factor = 1.0,
    longitudinal_response_factor = 1.0,
    lateral_response_factor = 1.0,
    damping_factor = 1.0,
    steering_vibration_factor = 1.0,
    profile_version = 'v1-heuristic'
WHERE vehicle_profile_id = 0;

-- sensor_producer.domain.VEHICLE_PROFILES와 동일한 값이어야 한다(같은 5개
-- 프로필을 나타냄, OQ-028 — 자동 동기화는 없으니 값을 바꾸면 두 곳 다 고친다).
INSERT INTO vehicle_profile (
    vehicle_profile_id, profile_name, body_type, size_class,
    vertical_response_factor, longitudinal_response_factor,
    lateral_response_factor, damping_factor, steering_vibration_factor,
    profile_version, is_active, created_at, updated_at
) VALUES
    (1, 'VP_SEDAN_COMPACT', 'SEDAN', 'COMPACT', 1.05, 1.00, 1.00, 0.68, 1.03, 'v1-heuristic', TRUE, '2026-08-18T00:00:00Z', '2026-08-18T00:00:00Z'),
    (2, 'VP_SEDAN_LARGE',   'SEDAN', 'LARGE',   1.00, 1.00, 1.00, 0.77, 1.00, 'v1-heuristic', TRUE, '2026-08-18T00:00:00Z', '2026-08-18T00:00:00Z'),
    (3, 'VP_SUV_COMPACT',   'SUV',   'COMPACT', 1.08, 1.00, 1.06, 0.70, 1.04, 'v1-heuristic', TRUE, '2026-08-18T00:00:00Z', '2026-08-18T00:00:00Z'),
    (4, 'VP_SUV_LARGE',     'SUV',   'LARGE',   1.01, 1.00, 1.08, 0.66, 1.00, 'v1-heuristic', TRUE, '2026-08-18T00:00:00Z', '2026-08-18T00:00:00Z'),
    (5, 'VP_MPV_LARGE',     'MPV',   'LARGE',   0.96, 1.00, 1.10, 0.61, 0.98, 'v1-heuristic', TRUE, '2026-08-18T00:00:00Z', '2026-08-18T00:00:00Z')
-- 옛 genesis/grandeur 등 모델 값이 남아있는 기존 행도 새 분류로 덮어써야 한다.
ON CONFLICT (vehicle_profile_id) DO UPDATE SET
    profile_name = EXCLUDED.profile_name,
    body_type = EXCLUDED.body_type,
    size_class = EXCLUDED.size_class,
    vertical_response_factor = EXCLUDED.vertical_response_factor,
    longitudinal_response_factor = EXCLUDED.longitudinal_response_factor,
    lateral_response_factor = EXCLUDED.lateral_response_factor,
    damping_factor = EXCLUDED.damping_factor,
    steering_vibration_factor = EXCLUDED.steering_vibration_factor,
    profile_version = EXCLUDED.profile_version,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at;

-- 모든 행이 값을 갖게 됐으니 이제 필수로 만들고, (profile_name, profile_version) 유일성을 보장한다.
ALTER TABLE vehicle_profile
    ALTER COLUMN size_class SET NOT NULL,
    ALTER COLUMN vertical_response_factor SET NOT NULL,
    ALTER COLUMN longitudinal_response_factor SET NOT NULL,
    ALTER COLUMN lateral_response_factor SET NOT NULL,
    ALTER COLUMN damping_factor SET NOT NULL,
    ALTER COLUMN steering_vibration_factor SET NOT NULL,
    ALTER COLUMN profile_version SET NOT NULL;

ALTER TABLE vehicle_profile
    ADD CONSTRAINT uq_vehicle_profile_name_version UNIQUE (profile_name, profile_version);
