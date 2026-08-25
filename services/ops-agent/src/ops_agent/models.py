# Grafana webhook payload(Prometheus Alertmanager와 같은 형태)를 Incident로 변환해, 이후 로직이 Grafana 스키마를 몰라도 되게 한다.

from __future__ import annotations

from dataclasses import dataclass

FIRING = "firing"
RESOLVED = "resolved"


@dataclass(frozen=True, slots=True)
class Incident:
    """하나의 alert를 Ops Agent 내부에서 다루기 위한 최소 표현."""

    alertname: str
    service: str | None
    severity: str | None
    status: str
    fingerprint: str
    auto_remediate: bool
    annotations: dict[str, str]

    @property
    def is_firing(self) -> bool:
        return self.status == FIRING

    @classmethod
    def from_grafana_alert(cls, alert: dict) -> Incident:
        # alertname/fingerprint가 없으면 ValueError — 호출부가 이 alert만 건너뛰고 나머지는 계속 처리한다.
        labels = alert.get("labels") or {}
        annotations = alert.get("annotations") or {}
        alertname = labels.get("alertname")
        fingerprint = alert.get("fingerprint")
        if not alertname or not fingerprint:
            raise ValueError("alert is missing required 'alertname' label or 'fingerprint'")
        status = alert.get("status") or FIRING
        return cls(
            alertname=alertname,
            service=labels.get("service"),
            severity=labels.get("severity"),
            status=status,
            fingerprint=fingerprint,
            # 명시적으로 opt-in해야 자동 조치 대상이 된다 — 기본값은 항상 escalation-only.
            auto_remediate=labels.get("auto_remediate", "").lower() == "true",
            annotations=dict(annotations),
        )


def parse_grafana_webhook(payload: dict) -> list[Incident]:
    # 개별 alert 파싱 실패는 그 alert만 건너뛰고 전체 요청은 실패시키지 않는다.
    incidents: list[Incident] = []
    for alert in payload.get("alerts") or []:
        try:
            incidents.append(Incident.from_grafana_alert(alert))
        except ValueError:
            continue
    return incidents
