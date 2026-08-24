import { useCallback, useEffect, useState } from "react";

import { fetchBootstrap, fetchSegments } from "./api.js";
import RoadComfortMap from "./components/RoadComfortMap.jsx";

const ALL_BOROUGHS = "All boroughs";

// Leaflet은 bounds를 full float precision으로 준다. 1픽셀만 움직여도 값이
// 달라지므로 ~100m로 반올림해 비교한다 -- 실제 pan은 따라가되 미세한 지터로는
// 다시 조회하지 않는다.
const VIEWPORT_PRECISION = 3;
const EDGES = ["south", "west", "north", "east"];

function sameViewport(a, b) {
  if (!a || !b) return false;
  return EDGES.every(
    (edge) => a[edge].toFixed(VIEWPORT_PRECISION) === b[edge].toFixed(VIEWPORT_PRECISION),
  );
}

export default function App() {
  const [bootstrap, setBootstrap] = useState(null);
  const [bootstrapError, setBootstrapError] = useState(null);
  const [borough, setBorough] = useState(null);
  const [viewport, setViewport] = useState(null);
  const [segments, setSegments] = useState(null);
  const [segmentsVersion, setSegmentsVersion] = useState(0);
  const [meta, setMeta] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    const controller = new AbortController();
    fetchBootstrap(controller.signal)
      .then(setBootstrap)
      .catch((exc) => {
        if (exc.name !== "AbortError") setBootstrapError(exc.message);
      });
    return () => controller.abort();
  }, []);

  // borough를 고르기 전에는 outline만 보여준다. 전체 도로망을 그리는 것이
  // 애초에 지도를 못 쓰게 만들던 원인이다.
  const needsBorough = bootstrap != null && bootstrap.boroughs.length > 0;
  const shouldFetch = viewport != null && bootstrap != null && (!needsBorough || borough != null);

  useEffect(() => {
    if (!shouldFetch) {
      setSegments(null);
      setMeta(null);
      return undefined;
    }
    const controller = new AbortController();
    setLoading(true);
    fetchSegments({ borough, viewport, signal: controller.signal })
      .then((payload) => {
        setSegments(payload.features);
        setSegmentsVersion((version) => version + 1);
        setMeta(payload);
        setError(null);
      })
      .catch((exc) => {
        if (exc.name !== "AbortError") setError(exc.message);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    // 새 viewport나 borough로 넘어가면 진행 중이던 요청을 취소한다. 그러지
    // 않으면 먼저 보낸 요청의 늦은 응답이 지도를 과거 위치로 덮어쓴다.
    return () => controller.abort();
  }, [shouldFetch, borough, viewport]);

  const handleViewportChange = useCallback((next) => {
    // 같은 영역이면 이전 객체를 그대로 돌려줘서 조회 effect가 다시 돌지 않게 한다.
    setViewport((previous) => (sameViewport(previous, next) ? previous : next));
  }, []);

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
        <div>
          <div className="metric__label">
            {selected ? `Road segments in ${selected.name}` : "Road segments in snapshot"}
          </div>
          <div className="metric__value">{scopeCount.toLocaleString()}</div>
        </div>
      </div>

      {bootstrap != null && !needsBorough && (
        <div className="banner banner--info">
          Set DASHBOARD_ZONE_MASTER_S3_URI to pick a borough. Showing every segment in the snapshot
          instead.
        </div>
      )}
      {needsBorough && borough == null && (
        <div className="banner banner--info">Click a borough to load its road segments.</div>
      )}
      {meta?.truncated && (
        <div className="banner banner--info">
          {meta.in_viewport_count.toLocaleString()} segments are in view and the first{" "}
          {meta.rendered_count.toLocaleString()} are drawn. Zoom in to see all of them.
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
        onViewportChange={handleViewportChange}
        segments={segments}
        segmentsVersion={segmentsVersion}
        status={loading ? "Loading comfort scores..." : null}
      />

      <p className="app__caption">
        Rendered: {(meta?.rendered_count ?? 0).toLocaleString()} of {scopeCount.toLocaleString()}{" "}
        segments in {scopeLabel}
        {bootstrap != null && ` · Max per viewport: ${bootstrap.max_rendered_segments.toLocaleString()}`}
      </p>
    </div>
  );
}
