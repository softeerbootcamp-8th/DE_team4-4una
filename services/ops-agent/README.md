# Ops Agent

Grafana alert를 받아 Prometheus로 재검증하고, 허용된 범위의 경미한 장애만 1차
자동 조치(remediation)한 뒤 결과를 Slack으로 알립니다(#447).

## 알림 경로가 두 갈래인 이유

Grafana는 alert가 firing되면 ops-agent 웹훅과 Slack을 **병렬로** 호출합니다
(`infra/monitoring/grafana/provisioning/alerting/contact-points.yaml`의 같은
contact point 안에 webhook receiver와 slack receiver를 둘 다 둠). 그래서:

- **"장애 발생" 알림**은 Grafana가 직접 Slack으로 보냅니다. ops-agent가 죽어
  있거나 SSH/Prometheus 조회가 오래 걸려도 이 알림은 영향받지 않습니다.
- **진단/조치/복구 결과 알림**은 ops-agent가 처리를 마친 뒤 별도로 보냅니다
  (`orchestrator.py`의 `_notify`).

알림 채널이 자동복구 시스템(ops-agent)의 생사에 의존하면 안 된다는 원칙 때문에
이렇게 나눴습니다. Slack이 ops-agent를 호출하는 방향은 없습니다 — Grafana가
Slack과 ops-agent를 각각 독립적으로 호출하는 구조입니다.

## Incident flow

```text
Grafana Alert
  -> (병렬) Slack: 🚨 장애 감지 알림 (Grafana가 직접 전송, ops-agent 관여 없음)
  -> (병렬) POST /webhooks/grafana
  -> Incident로 변환 (Grafana raw payload에 직접 결합하지 않음)
  -> firing이 아니면 종료
  -> Prometheus 재검증 (Grafana가 stale할 수 있어 그대로 믿지 않음)
     -> 이미 정상이면 Slack 알림 후 종료 (조치 없음 — 침묵하지 않는다)
  -> 진단 수집 (컨테이너 상태 / 재시작 횟수 / 최근 로그, SSH)
  -> 조치 허용 여부 판정 (policy.py의 allowlist, auto_remediate 라벨)
     -> 허용 안 됨 -> Slack 알림 + 담당자 escalation, 조치 없음
  -> 최근에 이미 시도했는가 (SQLite 기반 cooldown)
     -> cooldown 중 -> Slack 알림 + escalation, 조치 없음
  -> 허용된 조치 실행 (고정된 argv만 SSH로 실행, 임의 쉘 문자열 없음)
  -> 복구 대기하며 폴링 (docker restart 직후는 아직 안 뜬 상태라 즉시 판정하지 않음)
  -> Slack 알림 (성공/실패 모두, 실패 시에만 담당자 멘션)
     실행한 명령이 읽기/변경으로 나뉘어 원문 그대로 실린다.
     로그 tail은 본문이 아니라 스레드 답글로 분리된다.
  -> 조치 이력을 append-only로 누적 (알림에 최근 7일 실행 횟수가 함께 나간다)
```

각 단계는 별도 모듈이 책임집니다: `models.py`(파싱), `prometheus_client.py`(재검증),
`diagnostics.py`(진단), `policy.py`(허용 판정), `incident_store.py`(중복 방지 + 실행 이력),
`remediation.py`(조치 실행), `owners.py`/`slack_notifier.py`(전송),
`notification.py`(알림 본문 조립), `orchestrator.py`(위 전체를 순서대로 엮음).

## 자동 조치 가능 범위 / 의도적으로 자동화하지 않은 범위

- 무엇을 자동 실행해도 되는지의 판정 기준은
  [ADR-0013](../../docs/adr/0013-immediate-remediation-without-slack-approval.md)에 있다.
  Slack 예/아니오 승인 게이트를 두지 않기로 한 근거도 같은 문서에 있다.
- 현재 구현된 조치는 `restart_stream_processor` (`docker restart stream-processor`)
  뿐입니다. `RemediationAction`에 `RESTART_SERVING_API`, `RESTART_AIRFLOW_SCHEDULER`가
  이미 정의돼 있지만 `IMPLEMENTED_ACTIONS`에는 포함되지 않아 실제로는 항상
  escalation으로 빠집니다 — 구현을 늘릴 때는 그 실행 함수를 추가하고
  `IMPLEMENTED_ACTIONS`/`ALLOWED_ACTIONS_BY_ALERTNAME`에 등록하면 됩니다.
- Kafka broker 재시작, Kafka offset reset, EMR job 재실행, 데이터/S3 삭제, DB
  스키마 변경, 인프라 변경, 임의 쉘 실행은 `policy.ESCALATION_ONLY_ACTIONS`에
  이름만 문서화돼 있고 실행 코드는 존재하지 않습니다. 이런 장애는 항상 Slack
  escalation으로만 처리됩니다.
- SSH로 실행하는 명령은 항상 코드에 고정된 `argv` 리스트이며, alert payload에서
  온 값으로 명령 문자열을 조립하지 않습니다(`ssh.py` 참고).
- `auto_remediate` 라벨이 없거나 `"true"`가 아닌 alert는 allowlist에 있어도
  조치하지 않고 escalation만 합니다.
- Component Status 값 3(QUERY STOPPED)/4(TARGET DOWN)만 `StreamProcessorDown`
  alert로 재시작 대상입니다. 값 1(EVENT DATA STALE)/2(PROGRESS STALE)는 별도
  `StreamProcessorStale` alert로 분리돼 있고 `auto_remediate` 라벨이 없어 항상
  알림만 갑니다 — 잠깐의 지연으로 컨테이너를 재시작하지 않기 위함입니다
  (`infra/monitoring/grafana/provisioning/alerting/rules.yaml`).
- restart 직후 바로 상태를 확인하지 않고, Spark JVM 기동/Prometheus scrape
  지연을 감안해 `OPS_AGENT_RECOVERY_POLL_INTERVAL_SECONDS` 간격으로
  `OPS_AGENT_RECOVERY_WAIT_SECONDS` 동안 폴링한 뒤 복구 여부를 판정합니다.

## 환경변수

| 변수 | 필수 | 기본값 | 설명 |
| --- | --- | --- | --- |
| `OPS_AGENT_HOST` | - | `0.0.0.0` | FastAPI bind host |
| `OPS_AGENT_PORT` | - | `8080` | FastAPI bind port |
| `PROMETHEUS_URL` | - | `http://localhost:9090` | 재검증에 쓸 Prometheus |
| `OPS_AGENT_COOLDOWN_SECONDS` | - | `600` | 같은 incident(fingerprint)의 재조치 금지 기간 |
| `OPS_AGENT_RECOVERY_POLL_INTERVAL_SECONDS` | - | `10` | 조치 후 복구 확인 폴링 간격 |
| `OPS_AGENT_RECOVERY_WAIT_SECONDS` | - | `90` | 조치 후 복구 확인을 포기하기까지 총 대기 시간 |
| `OPS_AGENT_INCIDENT_STORE_PATH` | - | `ops_agent_incidents.sqlite3` | 중복 방지 상태 저장 경로 |
| `DAG_OWNERS_CONFIG_PATH` | - | `config/dag_owners.yaml` | 서비스 담당자 조회에 쓸 YAML |
| `SLACK_BOT_TOKEN` | ✅ | - | `xoxb-...`. Airflow의 `slack_api_default`와 별개 |
| `SLACK_ALERT_CHANNEL` | ✅ | - | 알림을 보낼 채널 |
| `STREAM_PROCESSOR_SSH_HOST` | ✅ | - | stream-processor가 떠 있는 EC2. 기본값을 두지 않음(아래 참고) |
| `STREAM_PROCESSOR_SSH_USER` | - | `ec2-user` | |
| `STREAM_PROCESSOR_SSH_KEY_PATH` | ✅ | - | 위 host에 접속할 개인키 경로 |

`STREAM_PROCESSOR_SSH_HOST`에 기본값이 없는 이유: `context/architecture.md`는
Spark Streaming EC2를 Project EC2와 별도 인스턴스로 설명하지만,
`.github/workflows/deploy-stream-processor.yml`은 전용 host 변수 없이 `EC2_HOST`를
그대로 씁니다. 실제로 같은 EC2인지 확인 후 값을 채워야 하므로 잘못 추측하지 않도록
기본값을 비워 뒀습니다.

## 로컬 실행

```bash
export PROMETHEUS_URL=http://localhost:9090
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_ALERT_CHANNEL=alerts
export STREAM_PROCESSOR_SSH_HOST=1.2.3.4
export STREAM_PROCESSOR_SSH_KEY_PATH=/path/to/key.pem

uv run --package ops-agent ops-agent
```

```bash
curl -X POST localhost:8080/webhooks/grafana -H 'Content-Type: application/json' -d '{
  "status": "firing",
  "alerts": [{
    "status": "firing",
    "labels": {"alertname": "StreamProcessorDown", "service": "stream-processor", "severity": "high", "auto_remediate": "true"},
    "annotations": {"summary": "stream-processor down"},
    "fingerprint": "example-fp"
  }]
}'
```

## 테스트

```bash
uv run --package ops-agent pytest services/ops-agent/tests
```

실제 Prometheus/SSH/Slack을 호출하지 않고 fake/mock으로 대체합니다
(`tests/conftest.py`).

## 실제 연결 시 사람이 해야 할 설정

1. **Monitoring EC2**: `infra/monitoring/.env`에 `OPS_AGENT_SLACK_BOT_TOKEN`,
   `OPS_AGENT_SLACK_ALERT_CHANNEL`, `STREAM_PROCESSOR_SSH_HOST`,
   `STREAM_PROCESSOR_SSH_USER`를 채운다(`infra/monitoring/.env.example` 참고).
   이 Slack 값은 ops-agent뿐 아니라 Grafana의 최초 장애 알림에도 그대로
   재사용된다(`infra/compose/monitoring.yaml`의 `grafana` 서비스 environment 참고).
2. **SSH 개인키**: stream-processor가 떠 있는 EC2에 접속할 개인키를 Monitoring
   EC2의 `infra/monitoring/ops-agent/stream_processor.pem`에 직접 준비한다
   (저장소에는 커밋되지 않음 — `.gitignore`의 `*.pem`). 그 EC2의
   `~/.ssh/authorized_keys`에 대응하는 공개키를 등록해야 한다.
3. **Slack 담당자**: `config/dag_owners.yaml`의 `services:` 아래에 서비스별
   `owner`(같은 파일의 `users:`에 등록된 이름)와 `severity`를 등록한다.
4. **Grafana**: `infra/monitoring/grafana/provisioning/alerting/`에 contact
   point(`ops-agent`, webhook → `http://ops-agent:8080/webhooks/grafana`),
   notification policy, alert rule 2개(`StreamProcessorDown`은 재시작 대상,
   `StreamProcessorStale`은 알림만)를 이미 provisioning으로 구성해 뒀다 —
   배포되면 자동으로 적용된다. 추가 alert rule이 필요하면 이 디렉터리에 규칙을
   더 추가하면 된다.
5. **배포**: `.github/workflows/deploy-monitoring.yml`이 저장소 전체를
   Monitoring EC2로 보내고 `docker compose up -d --build`로 ops-agent 이미지를
   그 자리에서 빌드한다. 위 1, 2번 설정이 인스턴스에 없으면 배포 단계에서 명시적으로
   실패한다.
