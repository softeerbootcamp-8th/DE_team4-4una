import { useCallback, useEffect, useState } from "react";

import { fetchBootstrap, fetchSegments } from "./api.js";
import RoadComfortMap from "./components/RoadComfortMap.jsx";

const ALL_BOROUGHS = "All boroughs";

export default function App() {
  const [bootstrap, setBootstrap] = useState(null);
  const [bootstrapError, setBootstrapError] = useState(null);
  const [borough, setBorough] = useState(null);
  const [vehicleProfileId, setVehicleProfileId] = useState(null);
  const [segments, setSegments] = useState(null);
  const [segmentsVersion, setSegmentsVersion] = useState(0);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const controller = new AbortController();
    fetchBootstrap(controller.signal)
      .then((payload) => {
        setBootstrap(payload);
        // 배포 기본값에서 시작한다. 사용자가 고르기 전에는 서버가 쓰는 값과 같다.
        setVehicleProfileId(payload.default_vehicle_profile_id);
      })
      .catch((exc) => {
        if (exc.name !== "AbortError") setBootstrapError(exc.message);
      });
    return () => controller.abort();
  }, []);

  // borough를 고르기 전에는 outline만 보여준다. zone master가 없는 배포에서는
  // borough 개념 자체가 없으므로 스냅샷을 바로 받는다.
  const needsBorough = bootstrap != null && bootstrap.boroughs.length > 0;
  const shouldFetch =
    bootstrap != null && vehicleProfileId != null && (!needsBorough || borough != null);

  useEffect(() => {
    if (!shouldFetch) {
      setSegments(null);
      setMeta(null);
      return undefined;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchSegments({ borough, vehicleProfileId, signal: controller.signal })
      .then((payload) => {
        setSegments(payload.features);
        setSegmentsVersion((version) => version + 1);
        setMeta(payload);
      })
      .catch((exc) => {
        if (exc.name !== "AbortError") setError(exc.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    // borough를 연달아 바꾸면 진행 중이던 요청을 취소한다. 그러지 않으면 먼저
    // 보낸 요청의 늦은 응답이 나중에 고른 borough를 덮어쓴다.
    return () => controller.abort();
  }, [shouldFetch, borough, vehicleProfileId]);

  const handleSelectBorough = useCallback((name) => {
    setBorough(name === ALL_BOROUGHS ? null : name);
  }, []);

  const boroughs = bootstrap?.boroughs ?? [];
  const selected = boroughs.find((item) => item.name === borough) ?? null;
  const scopeCount = selected ? selected.segment_count : (bootstrap?.total_segment_count ?? 0);
  const scopeLabel = borough ?? "the snapshot";

  return (
    <div className="app">
      <h1 className="app__title">NYC Road Comfort Score Map</h1>
      <p className="app__caption">
        Road geometry comes from the configured S3 snapshot. Comfort scores come only from the
        Serving API.
      </p>

      {bootstrapError && <div className="banner banner--error">{bootstrapError}</div>}

      <div className="controls">
        <label>
          Borough
          <select
            value={borough ?? ALL_BOROUGHS}
            onChange={(event) => handleSelectBorough(event.target.value)}
            disabled={boroughs.length === 0}
          >
            {[ALL_BOROUGHS, ...boroughs.map((item) => item.name)].map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Vehicle profile
          <select
            value={vehicleProfileId ?? ""}
            onChange={(event) => setVehicleProfileId(Number(event.target.value))}
            disabled={bootstrap == null}
          >
            {(bootstrap?.vehicle_profiles ?? []).map((profile) => (
              <option key={profile.vehicle_profile_id} value={profile.vehicle_profile_id}>
                {profile.name}
              </option>
            ))}
          </select>
        </label>
        <div>
          <div className="metric__label">
            {selected ? `Road segments in ${selected.name}` : "Road segments in snapshot"}
          </div>
          <div className="metric__value">{scopeCount.toLocaleString()}</div>
        </div>
      </div>

      {bootstrap != null && !needsBorough && (
        <div className="banner banner--info">
          Set DASHBOARD_ZONE_MASTER_S3_URI to pick a borough. Showing part of the snapshot instead.
        </div>
      )}
      {needsBorough && borough == null && (
        <div className="banner banner--info">Click a borough to load its road segments.</div>
      )}
      {meta?.truncated && (
        <div className="banner banner--info">
          Only the first {meta.segment_count.toLocaleString()} segments of the snapshot are drawn.
          Set DASHBOARD_ZONE_MASTER_S3_URI to browse by borough instead.
        </div>
      )}
      {meta?.vehicle_profile_fallback && (
        <div className="banner banner--warning">
          Serving API used vehicle profile {meta.effective_vehicle_profile_id} instead of requested
          profile {meta.requested_vehicle_profile_id}.
        </div>
      )}
      {error && <div className="banner banner--error">{error}</div>}

      <RoadComfortMap
        boroughs={boroughs}
        selectedBorough={borough}
        onSelectBorough={handleSelectBorough}
        segments={segments}
        segmentsVersion={segmentsVersion}
        status={loading ? `Loading ${scopeLabel}...` : null}
      />

      <p className="app__caption">
        Rendered: {(meta?.segment_count ?? 0).toLocaleString()} of {scopeCount.toLocaleString()}{" "}
        segments in {scopeLabel}
      </p>
    </div>
  );
}
