-- zone_weather_snapshot(시계열)을 latest_zone_weather(존당 최신 1행)로 바꾼다 (#209).
-- current.weather_time은 계산 당시 기록일 뿐 최신값을 따라가면 안 되므로 weather FK는 제거한다.
ALTER TABLE current_segment_comfort_score
    DROP CONSTRAINT current_segment_comfort_score_location_id_weather_time_fkey;

-- 존당 최신 weather_time 1건만 남기고 나머지는 버린다.
DELETE FROM zone_weather_snapshot
WHERE (location_id, weather_time) NOT IN (
    SELECT location_id, MAX(weather_time)
    FROM zone_weather_snapshot
    GROUP BY location_id
);

ALTER TABLE zone_weather_snapshot DROP CONSTRAINT zone_weather_snapshot_pkey;
ALTER TABLE zone_weather_snapshot RENAME TO latest_zone_weather;
ALTER TABLE latest_zone_weather ADD PRIMARY KEY (location_id);
