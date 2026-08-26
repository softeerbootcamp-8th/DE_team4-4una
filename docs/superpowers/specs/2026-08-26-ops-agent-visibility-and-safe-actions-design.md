# Ops Agent 가시성 개선 + 무위험 조치 2종 추가 설계

## 배경

`services/ops-agent`(#447)는 Grafana alert를 받아 Prometheus로 재검증하고,
allowlist된 조치만 실행한 뒤 Slack으로 결과를 알린다. 현재 실제로 자동 실행되는
조치는 `docker restart stream-processor` 하나뿐이다(`remediation.py:19`).

운영하면서 두 가지 문제가 드러났다.

1. **Agent가 무엇을 실행하는지 밖에서 보이지 않는다.** Slack 알림은
   `실행한 조치: restart_stream_processor (succeeded=True)` 한 줄이 전부라, agent가
   읽기만 한 명령과 상태를 바꾼 명령을 구분할 수 없고 실행된 명령 원문도 알 수 없다.
2. **조치 범위가 좁다.** stream-processor 재시작 외에는 전부 escalation으로 빠진다.
   위험이 사실상 없는데도 사람이 SSH로 들어가 처리해야 하는 장애가 남아 있다.

관련 이슈: #447 (Ops Agent MVP), #512 (closed — 조치 확대 요구사항을 이 설계가 인수)
관련 문서: `services/ops-agent/README.md`,
`infra/monitoring/grafana/provisioning/alerting/rules.yaml`

> #512는 조치 확대를 다뤘고 이 설계가 그 범위를 이어받는다. 다만 두 지점이 다르다.
> #512의 "Airflow Scheduler heartbeat 장애 시 1회 restart"는 실행 중인 task에 영향을
> 주므로 이 설계의 무위험 기준에 맞지 않아 **제외**한다(§2-3). 반대로 #512가 요구한
> "DB 상태 확인 후 API만 문제일 때 restart"는 타당하므로 §2-1에 반영했다.

> 이 설계는 브레인스토밍에서 "Slack 예/아니오 승인 게이트"를 검토한 끝에 **만들지
> 않기로** 결론냈다. 근거는 §0에 남긴다. 그 결정이 §3의 조치 선택 기준을 그대로
> 규정하므로 먼저 읽어야 한다.

## 0. 먼저 확정한 것 — Slack 승인 게이트는 만들지 않는다

> 이 결정은
> [ADR-0013](../../adr/0013-immediate-remediation-without-slack-approval.md)으로
> 남겼다. 저위험 판정의 다섯 조건과 조치별 판정 결과는 그쪽이 정본이고, 이 절은
> 그 결정에 이르는 과정과 구현 수준의 상세를 담는다.

검토한 안은 셋이었다.

- **A. 즉시 실행 유지 + 가시성 강화** ← 채택
- **B. 모든 조치에 Slack 예/아니오 버튼**
- **C. 위험도 등급을 나눠 저위험은 즉시 실행, 고위험은 승인**

**B를 버린 이유는 비용이 아니라 가치다.** 승인 게이트가 막아주는 위험은 조치의
위험도에 비례하는데, 현재 유일한 조치인 `docker restart stream-processor`는
위험도가 최하다. 되돌릴 수 있고, Structured Streaming이 checkpoint에서 재개하므로
데이터 손실이 없고, blast radius가 컨테이너 1개이며, 이미 5겹으로 게이팅돼 있다
(opt-in 라벨 → alertname allowlist → 미구현 action 차단 → Prometheus 재검증 →
15분 cooldown). 여기에 사람 승인을 붙이면 새벽 장애 시 사람이 깰 때까지 복구가
지연되어 자동화의 핵심 가치가 사라진다.

**C를 버린 이유는 YAGNI다.** §3에서 추가하는 조치 2종도 전부 저위험 등급이므로,
"승인 필요" 등급의 멤버가 하나도 없는 등급 enum을 미리 만드는 셈이 된다.

### B의 비용 — 나중에 필요해질 때를 위한 기록

승인 게이트가 실제로 필요해지는 시점(Kafka broker 재시작, EMR job 재실행, 디스크
정리처럼 되돌리기 어렵거나 blast radius가 큰 조치를 추가할 때)에 아래가 전부
필요하다. 지금 만들지 않을 뿐 사라지는 비용이 아니다.

1. **인바운드 경로.** `ops-agent`는 현재 포트를 publish하지 않는다
   (`infra/compose/monitoring.yaml` — Grafana가 `de4-monitoring` 네트워크 안에서
   `http://ops-agent:8080`으로 붙기 때문). Slack 버튼 콜백을 받으려면 인터넷에서
   도달 가능한 HTTPS 엔드포인트가 필요하고, `terraform/`은 아직 빈 placeholder라
   보안그룹이 수동 관리다. **Slack Socket Mode**(outbound WebSocket만 사용, 공개
   엔드포인트 불필요, app-level token 필요)가 이 저장소 상황에서는 훨씬 싸다.
2. **요청 서명 검증** — HTTP 방식이면 `X-Slack-Signature` + timestamp 검증 필수.
3. **동기 흐름의 분리.** `orchestrator.handle()`은 현재 webhook 핸들러 안에서
   진단 → 조치 → 복구 폴링까지 동기로 끝낸다. 승인을 넣으면 "진단하고 버튼 올리고
   리턴" / "클릭 이벤트로 재개" 두 단계로 쪼개지고, pending incident의 진단 결과를
   보관할 저장소가 필요하다.
4. **정하지 않으면 사고나는 정책 4가지** — ⓐ 아무도 누르지 않으면 어떻게 되는가
   (자동 실행 fallback / 만료 / 재알림) ⓑ 누가 눌러도 되는가(채널의 아무나 vs
   `config/dag_owners.yaml`의 담당자만) ⓒ 대기 중 Grafana가 재알림을 보내면
   pending이 중복 생성되는가 ⓓ 이미 저절로 복구된 뒤 버튼을 누르면 어떻게 되는가.

§3의 조치 레지스트리는 이 게이트를 나중에 붙일 자리를 자연스럽게 만들어 준다
(레지스트리 항목에 등급 필드를 추가하는 형태).

---

## 1. 확정된 결정 — 가시성

블랙박스처럼 느껴지는 원인 5가지를 코드에서 확인했고, 각각에 대응한다.

### 1-1. 재검증 결과가 정상일 때의 침묵을 없앤다

**현재:** `orchestrator.py:83`에서 Prometheus 재검증 결과가 healthy이면 `_notify`를
호출하지 않고 곧바로 return한다. 그러면 Slack에는 Grafana가 직접 보낸
"🚨 감지 — Ops Agent가 재검증 후 진단/조치를 진행합니다"만 남고 후속 메시지가
영원히 오지 않는다. 사용자 입장에서는 agent가 죽었는지 판단해서 넘어갔는지
구분할 수 없다. **이것이 블랙박스 체감의 가장 큰 원인이다.**

**변경:** 이 경로에서도 알림을 보낸다. 내용은 "재검증 결과 이미 정상(`RUNNING`),
조치하지 않음 — Grafana alert가 stale이었던 것으로 보임".

`for: 2m` 조건과 재검증이 함께 걸려 있어 이 경로 자체가 드물기 때문에 알림 소음
증가는 크지 않다고 판단한다. 실제로 잦아지면 그때 alert rule의 `for`를 조정한다.

### 1-2. 수집한 로그를 버리지 않는다

**현재:** `collect_stream_processor_diagnostics`가 `docker logs --tail 50`을
수집해 `StreamProcessorDiagnostics.recent_logs`에 담지만, `_notify`는
`container_status`와 `restart_count`만 쓴다. `recent_logs`를 읽는 프로덕션 코드는
저장소 전체에 존재하지 않는다(테스트와 conftest의 fake에서만 참조).

**변경:** 로그 tail을 Slack에 노출한다. 다만 **메인 메시지가 아니라 스레드 답글로**
보내고 길이를 제한한다 — 근거는 §5의 위험 항목 참고.

### 1-3. 실행한 명령의 원문을 노출한다

**현재:** 어떤 호스트에서 어떤 명령이 돌았는지 알림에 없다.

**변경:** `SshResult`에 실제 실행한 **원격 argv**와 대상 호스트를 담아 알림에
그대로 싣는다. 표시 형태는 로컬 ssh 래퍼가 아니라 원격에서 실행된 것만 보여준다.

```text
ec2-user@spark-ec2 $ docker restart stream-processor
```

`ssh -i /run/ops-agent/stream_processor.pem -o BatchMode=yes ...` 같은 로컬 래퍼는
표시하지 않는다. 읽는 사람에게 의미가 없고 키 경로가 불필요하게 드러난다.

### 1-4. 읽기 명령과 변경 명령을 구분해서 보여준다

**현재:** agent는 read-only 명령 2개(`docker inspect`, `docker logs`)와 mutating
명령 1개(`docker restart`)를 실행하는데 알림에서 구분되지 않는다.

**변경:** Slack 메시지를 Block Kit으로 재구성하고 아래 순서로 고정한다.

| 블록 | 내용 |
| --- | --- |
| 헤더 | alertname / service / severity |
| 판단 근거 | Prometheus 재검증 값(`status.label`), 컨테이너 상태, `restart_count` |
| 읽기만 한 명령 | 실행된 read-only argv 목록 |
| ⚠️ 변경한 명령 | 실행된 mutating argv (없으면 "변경한 명령 없음") |
| 결과 | 종료 코드와 stdout/stderr 요약 |
| 복구 여부 | 성공 / 실패 |
| 이력 | 최근 7일 동일 fingerprint 실행 횟수 (§1-5) |
| 담당자 | 멘션 (실패 시에만 멘션, 성공 시에는 이름만) |

읽기/변경 구분은 명령을 실행하는 쪽이 분류를 함께 넘기는 방식으로 만든다. 호출부가
분류를 잊으면 안 되므로 `run_remote_command`의 인자로 필수화한다.

### 1-5. 실행 이력을 남긴다 (append-only 감사 테이블)

**현재:** `IncidentStore`의 `remediation_attempts` 테이블은 fingerprint를 PRIMARY
KEY로 두고 `ON CONFLICT DO UPDATE`로 **마지막 1건만 덮어쓴다**. "이번 주에 몇 번
재시작했나"를 알 방법이 없다.

**변경:** append-only 테이블 `remediation_events`를 새로 만든다.

```sql
CREATE TABLE IF NOT EXISTS remediation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    alertname TEXT NOT NULL,
    action TEXT NOT NULL,
    attempted_at REAL NOT NULL,
    succeeded INTEGER NOT NULL,
    recovered INTEGER
);
CREATE INDEX IF NOT EXISTS idx_remediation_events_fingerprint_time
    ON remediation_events (fingerprint, attempted_at);
```

- cooldown 판정은 이 테이블의 `MAX(attempted_at)`로 대체한다. 별도 테이블을 두 개
  유지할 이유가 없다.
- **행은 조치를 실행하기 전에 넣고, 실행이 끝난 뒤 같은 행의 `succeeded`/
  `recovered`를 갱신한다.** 현재 `orchestrator.py:140`이 `record_attempt`를
  `_remediate` 앞에서 호출하는 순서를 그대로 지키는 것이다. 조치 도중 ops-agent가
  죽어도 cooldown은 소진된 것으로 남아, 재기동 후 같은 incident를 무한히 다시
  건드리지 않는다. `recovered`가 NULL로 남은 행은 "실행 결과를 확인하지 못함"을
  뜻하며, 이 상태 자체가 조사할 가치가 있는 신호다.
- 기존 `remediation_attempts` 테이블은 **삭제하지 않는다**(AGENTS.md의 임의
  DROP 금지). 더 이상 읽거나 쓰지 않고 그대로 둔다.
- 상태 파일은 docker volume `ops-agent-data`에 있고 담는 것이 cooldown 상태뿐이라,
  이전 데이터를 옮기지 않아도 최악의 경우 조치가 한 번 더 허용되는 정도다.
  마이그레이션을 만들지 않는다.

### 1-6. 구조화 로그

각 단계(재검증 / 진단 / 정책 판정 / cooldown / 조치 / 복구 폴링)의 결과를
ops-agent stdout에 한 줄 JSON으로 남긴다. Slack에 실리지 않는 상세(전체 stderr 등)를
사후에 추적할 수 있게 하는 것이 목적이다.

---

## 2. 확정된 결정 — 추가할 조치 2종

선정 기준은 "실패해도 데이터가 사라지지 않고, 되돌릴 필요가 없으며, blast radius가
컨테이너 하나"다.

### 2-1. `restart_serving_api` — Project EC2의 `serving-api` 재시작

- `RemediationAction.RESTART_SERVING_API`는 이미 `policy.py:11`에 정의돼 있으나
  `IMPLEMENTED_ACTIONS`에 없어 항상 escalation으로 빠진다. 이번에 구현한다.
- **왜 무위험인가:** serving-api는 읽기 전용 stateless FastAPI다. 재시작해도 유실될
  상태가 없고, 진행 중이던 요청만 끊기며 대시보드가 재시도한다.
- **컨테이너 이름을 하드코딩하지 않는다.** `deploy-serving-api.yml:50`이
  `vars.SERVING_API_CONTAINER_NAME || 'serving-api'`로 이름을 덮어쓸 수 있게 해
  뒀으므로, ops-agent도 env로 받고 기본값 `serving-api`를 쓴다.
- **DB 장애와 API 장애를 반드시 구분한다.** DB가 죽었는데 API만 재시작하는 것은
  무의미하다. 이 구분은 #512(closed, 이 이슈가 인수)에서 제기된 요구사항이다.

  다행히 **추가 구현 없이 가능하다.** `serving-api`의 `/health`가 이미 DB까지
  확인해 두 경우를 다른 응답으로 구분한다(`services/serving-api/src/serving_api/routes.py:142`).

  | 상황 | `/health` 응답 | 판단 |
  | --- | --- | --- |
  | 정상 | `200 {"status":"ok","database":"ok"}` | 조치 없음 |
  | API 프로세스만 죽음 | 연결 실패 (응답 없음) | **재시작 대상** |
  | API는 살아있고 DB가 죽음 | `503 {"status":"degraded","database":"unavailable"}` | **재시작 금지, escalation** |

- **신규 alert rule 2개가 필요하다.** 현재 `rules.yaml`에 serving-api 관련 규칙이
  전혀 없다. 위 구분을 alert 단계에서 갈라 두면 ops-agent가 판별 로직을 따로 갖지
  않아도 된다 — `NodeDown`을 SSH 성공 여부로 가르는 것과 같은 접근이다.

  판별에는 **기존 `blackbox-exporter`를 재사용한다.** 이미 Grafana/Prometheus/
  ops-agent의 `/health`를 probe하고 있으므로(`infra/monitoring/prometheus/prometheus.yml`의
  `blackbox-self-health` job), 대상 목록에 serving-api를 추가하기만 하면 된다.
  `probe_http_status_code`가 실제 응답 코드를 그대로 노출하므로 503과 연결 실패(0)가
  구분된다. ops-agent에 새 네트워크 경로나 HTTP 클라이언트를 만들 필요가 없다.

  - `ServingApiDown` — probe 실패이면서 상태 코드가 503이 **아닌** 경우.
    라벨 `service: serving-api`, `severity: high`, `auto_remediate: "true"`.
  - `ServingApiDatabaseUnavailable` — 상태 코드가 503인 경우.
    라벨 `service: serving-api`, `severity: critical`, `auto_remediate` **없음**
    (알림만). DB 장애는 `ESCALATION_ONLY_ACTIONS`의 영역이다.
  - 두 규칙 모두 `for: 2m` — 배포 중 재시작을 장애로 오인하지 않기 위해
    `StreamProcessorDown`과 같은 값을 쓴다.
  - `service`가 `infrastructure`가 아니므로 기존 notification policy의 기본
    라우팅에 따라 자동으로 `ops-agent` contact point로 간다. 라우팅 변경 불필요.

### 2-2. `restart_node_exporter` — 죽은 node-exporter 컨테이너 재시작

- **왜 무위험인가:** node-exporter는 관측 계층이다. 재시작해도 데이터 파이프라인에
  아무 영향이 없다. 그런데 이것이 죽으면 `NodeDown`/`HighCPU`/`HighMemory`/
  `DiskWarning`/`DiskCritical`이 전부 no-data가 되어 **관측 자체가 멈춘다.**
- **SSH가 호스트 장애와 exporter 장애를 자동으로 구분해 준다.** 현재 `NodeDown`
  alert는 "호스트가 죽은 것"과 "exporter만 죽은 것"을 구분하지 못한다. 그런데 SSH를
  시도하면 자연히 갈린다 — 붙으면 exporter만 죽은 것이니 재시작하고, 안 붙으면
  호스트가 죽은 것이니 SSH 실패로 escalation된다. 별도 판별 로직이 필요 없다.
- 컨테이너 이름은 Project EC2와 Spark EC2 모두 `node-exporter`다
  (`infra/compose/exporters.yaml:4`, `infra/compose/spark-exporters.yaml:5`).

**두 가지 제약을 명시한다.**

1. **`monitoring-node`는 대상에서 제외한다.** Monitoring EC2의 exporter는 컨테이너
   이름이 `monitoring-node-exporter`이고, ops-agent가 같은 호스트에 떠 있으므로
   재시작하려면 컨테이너에 docker socket을 마운트해야 한다. 그것은 호스트 root
   권한과 동등하므로 이 설계의 "무위험" 기준에 맞지 않는다. 이 경우는 escalation만
   한다.
2. **alertname만으로는 조치를 결정할 수 없다.** `NodeDown` 하나가 세 호스트를 모두
   커버하므로(`up{job=~"project-node|spark-node|monitoring-node"}`), 어느 호스트인지
   `job` 라벨을 봐야 조치 대상과 SSH 대상이 정해진다. §3의 레지스트리가 alertname에
   더해 라벨 매칭을 지원해야 하는 이유다.

**라우팅 변경이 필요하다.** `NodeDown`은 `service: infrastructure` 라벨을 달고
있어서, 현재 `notification-policies.yaml`의 매처에 걸려 `infra-slack`으로만 가고
**ops-agent를 아예 거치지 않는다.** `NodeDown`만 `ops-agent` contact point로
가도록 라우팅을 분리하고 `auto_remediate: "true"` 라벨을 추가한다. `HighCPU` 등
나머지 infrastructure alert는 지금처럼 `infra-slack`으로 남긴다.

### 2-3. 명시적으로 추가하지 않는 조치

- **디스크 정리(`docker image prune` 등).** 문제 자체는 실재한다 — Monitoring EC2는
  배포마다 저장소 전체를 `docker compose up -d --build`로 빌드해 dangling image와
  build cache가 쌓인다. 그러나 `DiskWarning`은 EC2 3대 어디서든 발생하고, Monitoring
  EC2 자신을 정리하려면 위 제약 1과 같은 docker socket 문제에 부딪힌다. 위험 대비
  비용이 맞지 않아 제외한다.
- **Kafka broker 재시작, Kafka offset reset, Airflow scheduler 재시작, EMR job
  재실행.** 진행 중인 작업에 영향을 주거나 되돌리기 어렵다. `policy.py`의
  `ESCALATION_ONLY_ACTIONS`에 그대로 남긴다.

---

## 3. 구조 변경 — 조치 레지스트리

조치가 3종, SSH 대상이 2곳이 되면 현재 구조가 맞지 않는다. `OpsAgentOrchestrator`는
생성자에서 **`ssh_target` 하나와 `diagnose`/`remediate` 콜백 하나씩**만 받고,
`_wait_for_recovery`와 최초 재검증이 `prometheus.stream_processor_status()`를 직접
호출한다(`orchestrator.py:82`, `orchestrator.py:162`). stream-processor 전용으로
하드코딩된 상태다.

**alert → 조치 명세 레지스트리로 바꾼다.** 항목이 담아야 하는 것:

| 필드 | 목적 |
| --- | --- |
| alertname | 1차 매칭 키 |
| 라벨 매처 | `NodeDown`을 `job` 라벨로 갈라내기 위해 필요 (§2-2 제약 2) |
| 상태 PromQL + healthy 판정 | 최초 재검증과 복구 폴링에 공용으로 쓴다 |
| SSH 대상 키 | `spark` / `project` — 미해결이면 escalation |
| 진단 대상 컨테이너 이름 | `docker inspect` / `docker logs` 인자 |
| 조치 대상 컨테이너 이름 | `docker restart` 인자 |

이 구조가 부수적으로 코드를 줄인다. 조치 3종이 결국 전부 "컨테이너 하나 재시작"
이므로, `restart_stream_processor` / `restart_serving_api` /
`restart_node_exporter`를 각각 만들지 않고 **컨테이너 이름을 인자로 받는 함수 하나**로
합친다. `collect_stream_processor_diagnostics`도 하드코딩된
`STREAM_PROCESSOR_CONTAINER_NAME`을 인자로 바꿔 일반화한다.

**`ssh.py`의 불변식은 유지한다.** argv는 여전히 코드에 고정된 값으로만 구성하고,
컨테이너 이름도 레지스트리/환경변수에서 오는 값만 쓴다. **alert payload에서 온 값이
argv에 들어가는 경로를 만들지 않는다**(`ssh.py:1`의 주석이 명시한 유일한 방어선).
`job` 라벨은 레지스트리 항목을 **고르는 데만** 쓰고 명령에 직접 넣지 않는다.

**승인 게이트의 자리.** 나중에 §0의 B가 필요해지면 이 레지스트리 항목에 등급
필드를 하나 추가하고, `policy.decide()`의 반환을 "허용/불허" 2분기에서
"즉시 실행 / 승인 필요 / 금지" 3분기로 넓히면 된다. 지금은 만들지 않는다.

---

## 4. 새로 필요한 설정

| 항목 | 내용 |
| --- | --- |
| `PROJECT_SSH_HOST` | serving-api / node-exporter가 있는 Project EC2 |
| `PROJECT_SSH_USER` | 기본 `ec2-user` |
| `PROJECT_SSH_KEY_PATH` | 위 호스트 접속용 개인키 경로 |
| `SERVING_API_CONTAINER_NAME` | 기본 `serving-api` (§2-1) |
| compose 볼륨 | Project EC2 접속용 `.pem`을 `monitoring.yaml`에 추가 마운트 |

기존 `STREAM_PROCESSOR_SSH_*`와 같은 방식을 따른다. 즉 **필수 값에 기본값을 두지
않는다** — 값이 없으면 기동 시점에 명시적으로 실패한다(`config.py:87`의 `_require`).

`.pem` 파일은 커밋하지 않는다(`.gitignore`의 `*.pem`). Monitoring EC2에 직접
준비하고, Project EC2의 `~/.ssh/authorized_keys`에 대응 공개키를 등록해야 한다.

---

## 5. 위험과 대응

| 위험 | 대응 |
| --- | --- |
| **로그 tail에 민감정보가 섞여 Slack에 노출** — `docker logs --tail 50`의 내용을 통제할 수 없다 | 메인 메시지가 아닌 **스레드 답글**로 분리하고 길이를 제한한다. 알림 채널이 이미 비공개 운영 채널이라는 전제를 README에 명시한다 |
| **ops-agent의 SSH 접근 범위가 EC2 2대로 늘어난다** | 개인키를 호스트별로 분리하고, 원격에서 실행 가능한 argv를 레지스트리에 고정된 `docker` 하위 명령으로만 제한한다. §3의 불변식 유지 |
| **`NodeDown` 라우팅 변경으로 기존 infra 알림이 누락** | `NodeDown`만 분리하고 나머지 infrastructure alert는 `infra-slack`에 그대로 둔다. 배포 후 Grafana UI에서 두 경로가 모두 살아 있는지 확인한다 |
| **§1-1의 알림 추가로 소음 증가** | `for: 2m` + 재검증 조합상 드물 것으로 본다. 잦아지면 alert rule의 `for`를 조정한다 |
| **`ServingApiDown`이 배포 중 재시작을 오탐** | `for: 2m`으로 배포 재시작 시간을 넘긴다. 오탐이 나면 `deploy-serving-api.yml`의 health 대기 시간과 맞춰 조정한다 |

---

## 6. 테스트

기존 방식을 그대로 따른다 — 실제 Prometheus/SSH/Slack을 호출하지 않고
`tests/conftest.py`의 fake/mock으로 대체한다.

- **레지스트리 선택**: alertname + `job` 라벨 조합이 올바른 항목을 고르는지.
  특히 `NodeDown` + `job="monitoring-node"`가 **조치 없이 escalation**되는지.
- **SSH 대상 미해결**: Project EC2 설정이 없을 때 조치하지 않고 escalation하는지.
- **§1-1 침묵 구간**: 재검증이 healthy일 때 알림이 **발송되는지**(현재는 안 됨).
- **읽기/변경 분류**: 진단 명령이 read-only로, 조치 명령이 mutating으로 분류돼
  알림 블록에 나뉘어 들어가는지.
- **감사 테이블**: 같은 fingerprint로 여러 번 실행하면 행이 누적되는지, cooldown이
  `MAX(attempted_at)` 기준으로 동작하는지.
- **payload 주입 방어**: alert 라벨에 `; rm -rf /` 같은 값이 들어와도 argv에
  반영되지 않는지 (§3 불변식의 회귀 테스트).

검증 명령은 AGENTS.md를 따른다.

```bash
uv sync --all-packages
uv run --all-packages ruff check .
uv run --all-packages pytest
```

---

## 7. 미해결 사항

- **stream-processor가 실제로 어느 EC2에 있는지 확정되지 않았다.**
  `deploy-monitoring.yml`은 `SPARK_EC2_HOST`를 별도로 쓰는데
  `deploy-stream-processor.yml`은 `EC2_HOST`를 쓴다. `services/ops-agent/README.md`가
  이미 지적한 문제이고 이 설계에서 해결하지 않는다. Project EC2 SSH 설정을 추가할
  때 두 호스트가 같은 인스턴스로 밝혀지면 설정을 합칠 수 있다.
- **`context/`에 ops-agent가 아예 없다.** `context/services.md`,
  `context/architecture.md`, `context/manifest.yaml` 어디에도 ops-agent 언급이 없다
  (#447이 서비스를 추가하면서 context를 갱신하지 않은 것으로 보인다). AGENTS.md는
  아키텍처가 바뀌면 `context/`를 함께 갱신하라고 정하므로, #546 / #547 구현 시점에
  ops-agent 항목을 신설하고 ADR-0013을 참조하게 한다. 이번 설계 단계에서는 다루지
  않는다.
