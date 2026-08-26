---
status: accepted
date: 2026-08-26
supersedes:
superseded_by:
---

# 0013. Ops Agent는 저위험 조치를 사람 승인 없이 즉시 실행한다

## 배경

Ops Agent(#447)는 Grafana alert를 받아 Prometheus로 재검증하고, allowlist된 조치만
실행한 뒤 Slack으로 결과를 알린다. 현재 자동 실행되는 조치는
`docker restart stream-processor` 하나뿐이다
(`services/ops-agent/src/ops_agent/remediation.py:19`).

이 조치를 넓히려 하니(#545) 먼저 정해야 할 것이 생겼다. **agent가 상태를 바꾸는
명령을 사람 확인 없이 실행해도 되는가.** 이 질문은 조치를 추가할 때마다 반복해서
제기되므로, 판정 기준을 코드가 아니라 결정으로 남긴다.

논의의 계기는 두 가지였다.

**첫째, agent의 동작이 밖에서 보이지 않는다.** Slack 알림은
`실행한 조치: restart_stream_processor (succeeded=True)` 한 줄이 전부라, 읽기만 한
명령과 상태를 바꾼 명령이 구분되지 않고 실행된 명령 원문도 알 수 없다. 무엇을 하는지
보이지 않는 자동화는 "일단 사람 확인을 거치자"는 요구를 자연히 부른다.

**둘째, Slack으로 예/아니오를 받는 구조가 직관적으로 안전해 보인다.** 그러나 검토
결과 이 저장소에서는 비용이 작지 않다. `ops-agent`는 포트를 publish하지 않아
(`infra/compose/monitoring.yaml`) 인터넷에서 도달할 수 없고, `terraform/`은 아직 빈
placeholder라 보안그룹이 수동 관리다. 더 중요한 것은 `orchestrator.handle()`이 현재
webhook 핸들러 안에서 진단 → 조치 → 복구 폴링까지 동기로 끝낸다는 점이다
(`services/ops-agent/src/ops_agent/orchestrator.py:78`). 승인을 넣으면 이 흐름이 두
단계로 쪼개지고 pending 상태 저장소가 필요해진다.

## 결정

**되돌릴 필요가 없고 blast radius가 컨테이너 하나인 조치는 사람 승인 없이 즉시
실행한다.** 그 기준을 만족하지 않는 조치는 자동화하지 않고 escalation만 한다. 즉
**"즉시 실행"과 "escalation" 사이에 사람 승인 단계를 두지 않는다.**

조치가 저위험인지는 아래 다섯 조건을 **모두** 만족하는지로 판정한다.

1. **되돌릴 필요가 없다.** 실행 후 프로세스가 스스로 정상 상태로 복귀하며, 별도의
   복구 조작이 필요하지 않다.
2. **데이터가 사라지지 않는다.** 유실될 상태가 없거나(stateless), checkpoint에서
   재개된다.
3. **blast radius가 컨테이너 하나다.** 다른 서비스의 가용성에 영향을 주지 않는다.
4. **진행 중인 작업을 중단시키지 않는다.** 중단되더라도 클라이언트 재시도로 흡수되는
   범위여야 한다.
5. **실행 argv가 코드에 고정돼 있다.** alert payload에서 온 값이 명령에 흘러드는
   경로가 없다(`services/ops-agent/src/ops_agent/ssh.py:1`).

이 기준으로 판정한 결과는 다음과 같다.

| 조치 | 판정 | 근거 |
| --- | --- | --- |
| `restart_stream_processor` | 즉시 실행 | Structured Streaming이 checkpoint에서 재개 |
| `restart_serving_api` | 즉시 실행 | 읽기 전용 stateless FastAPI, 끊긴 요청은 재시도로 흡수 |
| `restart_node_exporter` | 즉시 실행 | 관측 계층이라 파이프라인에 영향 없음 |
| Airflow Scheduler 재시작 | escalation | 조건 4 위반 — 실행 중인 task에 영향 |
| Kafka broker 재시작 | escalation | 조건 2·3·4 위반 |
| Kafka offset reset | escalation | 조건 1·2 위반 |
| EMR job 재실행 | escalation | 조건 3 위반 — 비용과 동시성에 영향 |
| 디스크 정리(`docker image prune` 등) | escalation | 조건 1 위반 — 지운 이미지를 되돌릴 수 없음 |
| DB 스키마 변경, 데이터·S3 삭제, 임의 쉘 실행 | escalation | 조건 1·2 위반 |

승인 게이트 대신 **사후 가시성**에 투자한다. agent가 어떤 호스트에서 어떤 명령을
실행했는지, 그중 무엇이 상태를 바꾼 명령인지를 Slack 알림만 보고 알 수 있게 하고,
실행 이력을 append-only로 남긴다(#546).

## 대안

| 대안 | 장점 | 단점 | 기각 이유 |
| --- | --- | --- | --- |
| **A. 저위험 조치 즉시 실행 + 사후 가시성** (채택) | 사람 없이 복구가 끝난다. 인바운드 경로·서명 검증·pending 상태가 필요 없어 공격면이 늘지 않는다 | 사전 판단 단계가 없다. 기준 판정을 잘못하면 그대로 실행된다 | — |
| **B. 모든 조치에 Slack 예/아니오 승인** | 모든 변경에 사람 확인이 들어간다. 상호작용이 눈에 보인다 | 새벽 장애에서 사람이 깰 때까지 복구가 지연된다. 공개 HTTPS 엔드포인트 또는 Socket Mode, 서명 검증, 동기 흐름 분리, 미응답·권한·중복·복구후클릭 정책이 모두 필요하다 | 승인이 막아주는 위험은 조치의 위험도에 비례하는데, 위 표의 즉시 실행 3종이 전부 저위험이라 막아주는 것이 거의 없다. 반면 무인 복구라는 핵심 가치는 사라진다 |
| **C. 위험도 등급을 나눠 저위험은 즉시 실행, 고위험은 승인** | 장기적으로 옳은 구조. 조치를 넓힐 때 자연스럽게 확장된다 | B의 비용을 그대로 지불해야 한다 | "승인 필요" 등급의 멤버가 하나도 없다. 멤버 없는 등급을 미리 만드는 것은 YAGNI다. 필요해지는 시점에 이 ADR을 대체하는 편이 낫다 |

## 결과

**감수하는 것.** 저위험 판정이 틀리면 사람 확인 없이 잘못된 조치가 실행된다. 이를
다섯 조건의 명시화, allowlist(`policy.py`), Prometheus 재검증, cooldown, 고정 argv로
방어하지만 사전 승인만큼 강하지는 않다.

**얻는 것.** 복구가 사람의 응답 시간에 묶이지 않는다. 조치를 추가할 때 논의가
"승인을 붙일까"가 아니라 "다섯 조건을 만족하나"로 좁혀진다.

**이 결정을 재검토해야 하는 시점.** 다섯 조건을 만족하지 않는 조치를 자동화하고
싶어질 때다. 그때는 이 ADR을 `superseded`로 바꾸고 승인 게이트를 도입하는 ADR을
새로 쓴다. 위 대안 B의 단점 칸에 필요한 구성요소를 적어 뒀으므로 다시 조사하지
않아도 된다. 구현 수준의 상세는
`docs/superpowers/specs/2026-08-26-ops-agent-visibility-and-safe-actions-design.md` §0에 있다.

**같이 정해진 것.** 조치가 늘어나도 `ops-agent`는 인바운드 요청을 받지 않는다.
Grafana webhook이 유일한 입구로 남고, Slack은 출력 전용이다. 이는 ADR-0010이
Monitoring EC2를 분리하며 세운 "알림 채널이 자동복구 시스템의 생사에 의존하지
않는다"는 원칙과 같은 방향이다.

## 영향 범위

- `services/ops-agent/src/ops_agent/policy.py` — 다섯 조건이 allowlist 판정의 근거가
  된다. `ESCALATION_ONLY_ACTIONS` 목록은 위 표의 escalation 행과 일치해야 한다.
- `services/ops-agent/README.md` — 자동 조치 가능 범위 절에 판정 기준을 반영한다.
- `infra/monitoring/grafana/provisioning/alerting/rules.yaml` — 자동 조치 대상 alert만
  `auto_remediate: "true"` 라벨을 갖는다. 이 라벨은 위 기준을 통과한 조치에만 붙인다.
- 인바운드 경로 없음이 유지되므로 `infra/compose/monitoring.yaml`의 ops-agent는 계속
  포트를 publish하지 않는다.

## 참고

- [ADR-0010. Project EC2와 Monitoring EC2를 분리한 Prometheus/Grafana 모니터링](0010-split-project-and-monitoring-ec2-for-prometheus-grafana.md)
- `docs/superpowers/specs/2026-08-26-ops-agent-visibility-and-safe-actions-design.md`
- #447 (Ops Agent MVP), #512 (closed — 조치 확대), #545 / #546 / #547
