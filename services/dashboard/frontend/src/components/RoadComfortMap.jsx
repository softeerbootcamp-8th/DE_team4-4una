import { useCallback, useEffect, useRef } from "react";
import { GeoJSON, MapContainer, TileLayer, useMap, useMapEvents } from "react-leaflet";

export const NYC_MAP_CENTER = [40.7128, -74.006];
export const NYC_MAP_ZOOM = 11;

// pan을 멈춘 뒤 요청까지 기다리는 시간. 드래그 중에는 아무것도 부르지 않는다.
const VIEWPORT_DEBOUNCE_MS = 250;

const BOROUGH_COLOR = "#3d6fb4";

// 색상 문자열은 백엔드(geojson.py)가 내려주는 값과 같아야 한다.
const LEGEND = [
  { color: "green", label: "80 or higher" },
  { color: "yellow", label: "60 to 79.99" },
  { color: "red", label: "below 60" },
  { color: "gray", label: "unavailable" },
];

const TOOLTIP_FIELDS = [
  "segment_id",
  "street_name",
  "comfort_score",
  "confidence_score",
  "source",
  "weather_time",
];

function segmentStyle(feature) {
  return { color: feature.properties.color, weight: 4, opacity: 0.8 };
}

function bindSegmentTooltip(feature, layer) {
  // 문자열 대신 DOM으로 만든다 -- street_name은 외부 데이터라 HTML로 넣지 않는다.
  const table = document.createElement("table");
  for (const field of TOOLTIP_FIELDS) {
    const row = table.insertRow();
    row.insertCell().textContent = `${field}:`;
    row.insertCell().textContent = feature.properties[field];
  }
  layer.bindTooltip(table, { sticky: false });
  layer.on({
    mouseover: (event) => event.target.setStyle({ weight: 7, opacity: 1 }),
    mouseout: (event) => event.target.setStyle(segmentStyle(feature)),
  });
}

/** moveend에서만, 그리고 debounce 뒤에만 viewport를 보고한다. */
function ViewportWatcher({ onViewportChange }) {
  const timer = useRef(null);

  const report = useCallback(
    (map) => {
      const bounds = map.getBounds();
      onViewportChange({
        south: bounds.getSouth(),
        west: bounds.getWest(),
        north: bounds.getNorth(),
        east: bounds.getEast(),
      });
    },
    [onViewportChange],
  );

  const map = useMapEvents({
    moveend: () => {
      clearTimeout(timer.current);
      timer.current = setTimeout(() => report(map), VIEWPORT_DEBOUNCE_MS);
    },
  });

  useEffect(() => {
    // 최초 렌더에는 moveend가 없으므로 지금 보고 있는 영역을 직접 보고한다.
    report(map);
    return () => clearTimeout(timer.current);
  }, [map, report]);

  return null;
}

/** 선택이 바뀌면 지도를 옮긴다. 지도 자체는 다시 만들지 않는다. */
function BoroughFocus({ borough }) {
  const map = useMap();

  useEffect(() => {
    if (!borough) {
      map.setView(NYC_MAP_CENTER, NYC_MAP_ZOOM);
      return;
    }
    const [minLon, minLat, maxLon, maxLat] = borough.bounds;
    map.fitBounds([
      [minLat, minLon],
      [maxLat, maxLon],
    ]);
  }, [map, borough]);

  return null;
}

function boroughFeatures(boroughs) {
  return {
    type: "FeatureCollection",
    features: boroughs.map((borough) => ({
      type: "Feature",
      geometry: borough.geometry,
      properties: { borough: borough.name },
    })),
  };
}

export default function RoadComfortMap({
  boroughs,
  selectedBorough,
  onSelectBorough,
  onViewportChange,
  segments,
  segmentsVersion,
  status,
}) {
  const selected = boroughs.find((borough) => borough.name === selectedBorough) ?? null;
  const selectable = boroughs.filter((borough) => borough.name !== selectedBorough);

  return (
    <div className="map">
      <MapContainer
        center={NYC_MAP_CENTER}
        zoom={NYC_MAP_ZOOM}
        className="map__canvas"
        // 1000개 폴리라인을 SVG DOM 노드로 그리면 pan/zoom이 눈에 띄게 밀린다.
        preferCanvas
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        />

        {selectable.length > 0 && (
          <GeoJSON
            key={`boroughs-${selectedBorough ?? "none"}`}
            data={boroughFeatures(selectable)}
            style={{
              color: BOROUGH_COLOR,
              weight: 2,
              fillColor: BOROUGH_COLOR,
              fillOpacity: 0.15,
            }}
            onEachFeature={(feature, layer) => {
              layer.bindTooltip(feature.properties.borough, { sticky: false });
              layer.on({
                mouseover: (event) => event.target.setStyle({ fillOpacity: 0.35 }),
                mouseout: (event) => event.target.setStyle({ fillOpacity: 0.15 }),
                click: () => onSelectBorough(feature.properties.borough),
              });
            }}
          />
        )}

        {selected && (
          // 채우지 않는다 -- 채운 폴리곤은 안쪽 segment로 갈 클릭을 전부 삼킨다.
          <GeoJSON
            key={`selected-${selected.name}`}
            data={boroughFeatures([selected])}
            style={{ color: BOROUGH_COLOR, weight: 2, fill: false }}
            interactive={false}
          />
        )}

        {segments && (
          // key가 바뀔 때만 레이어를 새로 만든다. react-leaflet의 GeoJSON은
          // data prop이 바뀌어도 스스로 갱신하지 않는다.
          <GeoJSON
            key={`segments-${segmentsVersion}`}
            data={segments}
            style={segmentStyle}
            onEachFeature={bindSegmentTooltip}
          />
        )}

        <BoroughFocus borough={selected} />
        <ViewportWatcher onViewportChange={onViewportChange} />
      </MapContainer>

      {status && <div className="map__status">{status}</div>}

      <div className="legend">
        <strong>Comfort Score</strong>
        {LEGEND.map(({ color, label }) => (
          <div className="legend__row" key={color}>
            <span className="legend__swatch" style={{ background: color }} />
            {label}
          </div>
        ))}
      </div>
    </div>
  );
}
