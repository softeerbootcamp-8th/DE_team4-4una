import { useEffect } from "react";
import { GeoJSON, MapContainer, TileLayer, useMap } from "react-leaflet";

export const NYC_MAP_CENTER = [40.7128, -74.006];
export const NYC_MAP_ZOOM = 11;

// borough가 화면 가장자리에 딱 붙지 않도록 두는 여백(px).
const FOCUS_PADDING = [40, 40];
// borough 하나를 보는 데 적당한 확대 한계. Staten Island처럼 작은 borough가
// 과하게 당겨지는 것을 막는다.
const FOCUS_MAX_ZOOM = 12;
const FOCUS_DURATION_SECONDS = 0.8;

const BOROUGH_COLOR = "#3d6fb4";

// 색상 문자열은 백엔드(geojson.py)가 내려주는 값과 같아야 한다.
const LEGEND = [
  { color: "green", label: "80 or higher" },
  { color: "yellow", label: "60 to 79.99" },
  { color: "red", label: "below 60" },
  { color: "gray", label: "unavailable" },
];

const TOOLTIP_ROWS = [
  ["Segment", "segment_id"],
  ["Street", "street_name"],
  ["Comfort", "comfort_score"],
  ["Confidence", "confidence_score"],
  ["Source", "source"],
];

// weather_time은 UTC로 내려온다. 이 지도는 뉴욕 도로를 보여주므로 보는 사람의
// 로컬 타임존이 아니라 뉴욕 시각으로 고정해 보여준다 -- 서울에서 열어도 "그
// 도로에 언제 관측된 날씨인가"가 궁금한 값이기 때문이다.
const NYC_TIME_FORMAT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  dateStyle: "medium",
  timeStyle: "short",
});

function formatWeatherTime(value) {
  if (!value || value === "N/A") return "N/A";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return `${NYC_TIME_FORMAT.format(parsed)} (NYC)`;
}

function segmentStyle(feature) {
  return { color: feature.properties.color, weight: 4, opacity: 0.8 };
}

/** 차량 프로필 이름을 툴팁에 함께 싣기 위해 바인더를 만들어 쓴다.
 *
 * 프로필은 요청 하나에 하나뿐이라 feature마다 넣으면 같은 값을 수만 번 실어
 * 보내게 된다. 그래서 응답 본문이 아니라 여기서 붙인다.
 */
function makeSegmentTooltipBinder(vehicleProfileName) {
  return (feature, layer) => {
    // 문자열 대신 DOM으로 만든다 -- street_name은 외부 데이터라 HTML로 넣지 않는다.
    const table = document.createElement("table");
    table.className = "tooltip";
    const addRow = (label, value) => {
      const row = table.insertRow();
      row.insertCell().textContent = label;
      row.insertCell().textContent = value;
    };
    for (const [label, field] of TOOLTIP_ROWS) {
      addRow(label, feature.properties[field]);
    }
    addRow("Weather time", formatWeatherTime(feature.properties.weather_time));
    if (vehicleProfileName) {
      addRow("Vehicle profile", vehicleProfileName);
    }

    layer.bindTooltip(table, { sticky: false });
    layer.on({
      mouseover: (event) => event.target.setStyle({ weight: 7, opacity: 1 }),
      mouseout: (event) => event.target.setStyle(segmentStyle(feature)),
    });
  };
}

/** 선택이 바뀌면 지도를 옮긴다. 지도 자체는 다시 만들지 않는다. */
function BoroughFocus({ borough }) {
  const map = useMap();

  useEffect(() => {
    if (!borough) {
      map.flyTo(NYC_MAP_CENTER, NYC_MAP_ZOOM, { duration: FOCUS_DURATION_SECONDS });
      return;
    }
    const [minLon, minLat, maxLon, maxLat] = borough.bounds;
    map.flyToBounds(
      [
        [minLat, minLon],
        [maxLat, maxLon],
      ],
      {
        padding: FOCUS_PADDING,
        maxZoom: FOCUS_MAX_ZOOM,
        duration: FOCUS_DURATION_SECONDS,
      },
    );
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
  segments,
  segmentsVersion,
  vehicleProfileName,
  loading,
  loadingLabel,
}) {
  const selected = boroughs.find((borough) => borough.name === selectedBorough) ?? null;
  const selectable = boroughs.filter((borough) => borough.name !== selectedBorough);

  return (
    <div className="map">
      <MapContainer
        center={NYC_MAP_CENTER}
        zoom={NYC_MAP_ZOOM}
        className="map__canvas"
        // 수만 개 폴리라인을 SVG DOM 노드로 그리면 pan/zoom이 눈에 띄게 밀린다.
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
            onEachFeature={makeSegmentTooltipBinder(vehicleProfileName)}
          />
        )}

        <BoroughFocus borough={selected} />
      </MapContainer>

      {loading && (
        // 지도를 덮어 흐리게 만든다 -- 아래 남아 있는 것은 이전 선택의 결과라,
        // 그대로 두면 이미 바뀐 것처럼 읽힌다. 특히 차량 프로필만 바꾸면 지도가
        // 움직이지 않아 예전 색이 최신으로 보인다.
        <div className="map__loading" role="status" aria-live="polite">
          <div className="map__loading-card">
            <span className="spinner" aria-hidden="true" />
            <span>{loadingLabel}</span>
          </div>
        </div>
      )}

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
