-- vehicle_profile: Bronze 참조 테이블 (context/data/schema-catalog.md 145~162행).
--
-- vehicle_profile_id는 GENERATED ALWAYS AS IDENTITY/SERIAL이 아닌 plain
-- INTEGER다 — 실제 차량 프로필은 Bronze 소스가 부여한 ID를 그대로 쓰고,
-- vehicle_profile_id=0(vehicle-agnostic sentinel,
-- 0003_seed_vehicle_profile_agnostic.sql)이 OVERRIDING SYSTEM VALUE 없이
-- 들어가야 하기 때문이다.
CREATE TABLE vehicle_profile (
    vehicle_profile_id INTEGER NOT NULL PRIMARY KEY,
    profile_name TEXT NOT NULL,
    vehicle_class TEXT NOT NULL,
    manufacturer TEXT,
    model_name TEXT,
    mass_kg DOUBLE PRECISION,
    wheelbase_mm DOUBLE PRECISION,
    suspension_type TEXT,
    vertical_weight DOUBLE PRECISION NOT NULL,
    longitudinal_weight DOUBLE PRECISION NOT NULL,
    lateral_weight DOUBLE PRECISION NOT NULL,
    is_active BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
