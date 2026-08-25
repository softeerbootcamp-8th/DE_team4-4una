// 백엔드는 항상 같은 origin에 있다. 개발 중에는 vite dev server가 프록시한다.

async function getJson(path, signal) {
  const response = await fetch(path, { signal });
  if (!response.ok) {
    throw new Error(`${response.status} ${await response.text()}`);
  }
  return response.json();
}

export function fetchBootstrap(signal) {
  return getJson("/api/bootstrap", signal);
}

export function fetchSegments({ borough, vehicleProfileId, signal }) {
  const params = new URLSearchParams();
  if (borough) {
    params.set("borough", borough);
  }
  if (vehicleProfileId != null) {
    params.set("vehicle_profile_id", vehicleProfileId);
  }
  return getJson(`/api/segments?${params}`, signal);
}
