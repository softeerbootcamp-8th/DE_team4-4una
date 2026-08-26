"""EMR Serverless driver 로그를 정규식 룰로 분류해 한국어 원인·조치를 돌려준다.

dags/notifications.py의 실패 알림이 이 모듈을 쓴다. 지금까지 알림에는 Airflow가 던지는
래퍼 예외("Job reached failure state FAILED")만 실려서, 원인을 알려면 S3 로그를 받아
압축을 풀고 Spark 스택트레이스를 읽어야 했다.

룰을 YAML이 아니라 코드 상수로 두는 이유는 ops_agent/policy.py의 allowlist와 같다 —
"무엇을 어떻게 진단할지"는 사람이 리뷰를 거쳐 등록해야 하는 정책이고, 정규식을 YAML에
넣으면 이스케이프가 값의 일부로 섞여 들어간다(weather_rules.yaml처럼 튜닝되는 임계값과는
성격이 다르다).

이 모듈은 순수 함수만 둔다 — Airflow/S3/Slack에 의존하지 않아 로그 문자열만으로 단위
테스트가 된다. 실제 로그를 읽어오고 Variable을 조회하는 일은 notifications.py가 맡는다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

UNKNOWN_ERROR = "UNKNOWN_ERROR"

# 오류 구간을 잘라낼 때 기준이 되는 키워드. Spark driver 로그는 대부분 INFO라
# 전체에 룰을 돌리면 무관한 줄에서 오탐이 난다 — 이 키워드가 나온 줄 주변만 본다.
_ERROR_KEYWORDS = re.compile(
    r"Traceback|Exception|ERROR|Caused by|ExitCode|exit code|SIGKILL|"
    r"OutOfMemory|MemoryError|No space left|FAILED",
    re.IGNORECASE,
)

# 정상 Job Run에도 매번 찍히면서 위 키워드에 걸리는 줄들. 실제 성공 로그로 확인했다 —
# 이걸 안 걸러내면 근거 로그가 전부 이 줄들로 채워져 진단이 무의미해진다.
_BENIGN_NOISE = re.compile(
    r"SLF4J:"  # 매 Job Run 시작 시 "Failed to load class ..." 3줄
    r"|(?:failed|running|waiting): HashSet\(\)"  # DAGScheduler 상태 덤프
    r"|No executor found for"  # executor 등록 전 정상 로그
)

_WINDOW_BEFORE = 30
_WINDOW_AFTER = 50

# 오류 구간이 지나치게 커지면(예: executor 수십 개가 연쇄로 죽어 ERROR가 도배된 로그)
# 분류에도 도움이 안 되고 메시지 조립만 무거워진다. 뒤쪽이 원인에 가까우므로 끝에서 자른다.
_MAX_WINDOW_LINES = 400

# 값을 몰라도 형태만으로 가릴 수 있는 비밀값. Airflow Variable 조회가 실패해도
# 최소한의 방어가 남도록 값 기반 마스킹(mask_values)과 병행한다.
_SECRET_ASSIGNMENT = re.compile(
    r"((?:PASSWORD|PASSWD|SECRET|TOKEN|ACCESS_KEY|PRIVATE_KEY)[A-Z_]*\s*[=:]\s*)(\S+)",
    re.IGNORECASE,
)

MASK = "***"

# 너무 짧은 값을 그대로 치환하면 로그 곳곳의 무관한 문자열까지 ***로 바뀌어
# 오히려 읽기 어려워진다.
_MIN_MASKABLE_LENGTH = 4

# exit 137(SIGKILL)만으로는 driver가 죽었는지 executor가 죽었는지 알 수 없는데,
# 이 저장소에서는 그 둘의 원인과 조치가 서로 다르다(#386 executor / #508 driver).
# 그래서 어느 쪽이 종료됐는지를 먼저 판별하고, 그 결과로 룰을 고른다.
_EXECUTOR_SIDE_MARKERS = re.compile(
    r"ExecutorLostFailure|Lost executor|Container killed|Container from a bad node|"
    r"Removing executor|executor .{0,40}exited|exceeded .{0,20}memory limits",
    re.IGNORECASE,
)
# "Shutdown hook called"/"SparkContext ... stop"은 정상 종료에도 매번 찍힌다 — 실제 성공
# Job Run 로그로 확인했다. 사망 신호로 쓰면 모든 로그가 driver 종료로 판별된다.
_DRIVER_SIDE_MARKERS = re.compile(
    r'Exception in thread "main"|driver .{0,40}exited|MemoryError|Py4JJavaError',
    re.IGNORECASE,
)

DRIVER = "driver"
EXECUTOR = "executor"


@dataclass(frozen=True, slots=True)
class FailureRule:
    """실패 로그 한 종류를 알아보는 규칙.

    `priority`가 작을수록 먼저 검사한다. `requires_side`가 있으면 종료된 쪽이
    그와 일치할 때만 적용한다.
    """

    error_type: str
    priority: int
    patterns: tuple[re.Pattern[str], ...]
    summary: str
    cause: str
    actions: tuple[str, ...]
    requires_side: str | None = None


def _compile(*patterns: str) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)


# 우선순위 근거: 로그 하나에 여러 패턴이 함께 찍힌다. executor가 메모리로 죽으면 Spark가
# 곧바로 executor를 다시 요청하다 ApplicationMaxCapacityExceededException을 반복하고
# 마지막에 Job Run이 FAILED로 끝난다. 단순히 먼저 나온 패턴을 고르면 "가장 나중 증상"인
# capacity 예외가 원인으로 잡혀 엉뚱한 곳을 보게 된다. 그래서 부팅 단계 실패(의존성) →
# 자원 고갈(디스크/메모리) → 그 결과로 나타나는 capacity 순으로 검사한다.
FAILURE_RULES: tuple[FailureRule, ...] = (
    FailureRule(
        error_type="PYTHON_DEPENDENCY",
        priority=10,
        patterns=_compile(
            r"ModuleNotFoundError",
            r"No module named",
            r"ImportError",
        ),
        summary="EMR 실행 환경에서 Python 모듈을 찾지 못했습니다.",
        cause=(
            "Job Run이 batch-jobs가 설치된 인터프리터가 아닌 다른 Python으로 부팅된 것으로 "
            "보입니다. base 이미지의 기본 python3는 3.9이고 batch-jobs는 python3.12 경로에만 "
            "설치됩니다."
        ),
        actions=(
            "sparkSubmitParameters의 PYSPARK_PYTHON / spark.pyspark.python 값 확인",
            "커스텀 이미지에 해당 패키지가 실제로 설치됐는지 확인",
            "entryPoint 경로가 커스텀 이미지 기준인지 확인",
        ),
    ),
    FailureRule(
        error_type="DISK_EXCEEDED",
        priority=20,
        patterns=_compile(
            r"No space left on device",
            r"Disk .{0,30}(exceeded|full)",
            r"DiskBlockObjectWriter.{0,60}(No space|IOException)",
        ),
        summary="EMR worker의 디스크 공간이 부족합니다.",
        cause=(
            "shuffle 데이터나 임시 파일이 worker에 할당된 디스크를 소진한 것으로 보입니다."
        ),
        actions=(
            "spark.emr-serverless.executor.disk / driver.disk 값 확인",
            "shuffle 데이터 크기와 파티션 수 확인",
            "불필요한 cache/persist 여부 확인",
        ),
    ),
    FailureRule(
        error_type="DRIVER_MEMORY_EXCEEDED",
        priority=30,
        requires_side=DRIVER,
        patterns=_compile(
            r'Exception in thread "main".{0,80}OutOfMemoryError',
            r"java\.lang\.OutOfMemoryError",
            r"MemoryError",
            r"(?:ExitCode|exit code)[:= ]+137",
            r"SIGKILL",
            r"memory usage exceeded",
        ),
        summary="EMR driver가 메모리 부족으로 종료되었습니다.",
        cause=(
            "driver 프로세스가 할당된 메모리를 초과해 종료된 것으로 보입니다. 이 저장소에서는 "
            "Great Expectations가 테이블 전량을 driver의 pandas로 올리는 audit 경로에서 주로 "
            "발생했습니다."
        ),
        actions=(
            "audit 프로파일의 spark.driver.memory / memoryOverhead 확인",
            "driver에서 테이블 전량을 적재하는 코드(SELECT *, collect, toPandas) 확인",
            "broadcast로 만드는 데이터 크기 확인",
        ),
    ),
    FailureRule(
        error_type="EXECUTOR_MEMORY_EXCEEDED",
        priority=40,
        requires_side=EXECUTOR,
        patterns=_compile(
            r"ExecutorLostFailure",
            r"Container killed.{0,60}memory",
            r"exceeded .{0,20}memory limits",
            r"java\.lang\.OutOfMemoryError",
            r"(?:ExitCode|exit code)[:= ]+137",
            r"SIGKILL",
            r"memory usage exceeded",
        ),
        summary="EMR executor가 메모리 부족으로 종료되었습니다.",
        cause=(
            "executor가 할당된 메모리를 초과해 OS에 강제 종료된 것으로 보입니다. 이 저장소에서는 "
            "mapInPandas가 파티션마다 Python worker에서 road_segment broadcast로 STRtree를 만드는 "
            "Map Matching 경로에서 주로 발생했습니다."
        ),
        actions=(
            "spark.executor.memoryOverhead 확인 (Python worker 메모리는 여기서 나온다)",
            "mapInPandas / UDF의 파티션당 처리 데이터 크기 확인",
            "spark.executor.cores 대비 동시 실행되는 Python worker 수 확인",
        ),
    ),
    FailureRule(
        error_type="EMR_CAPACITY_EXCEEDED",
        priority=50,
        patterns=_compile(
            r"ApplicationMaxCapacityExceededException",
            r"maximum capacity",
        ),
        summary="EMR Serverless Application의 최대 자원을 초과했습니다.",
        cause=(
            "요청한 executor가 Application의 maximumCapacity를 넘어선 것으로 보입니다. 원인이 "
            "둘이므로 아래를 모두 확인해야 합니다: dynamicAllocation 설정이 "
            "spark.executor.instances와 어긋난 경우, 그리고 같은 Application을 공유하는 다른 "
            "DAG의 Job Run이 동시에 실행된 경우."
        ),
        actions=(
            (
                "spark.dynamicAllocation의 min/max/initialExecutors가 "
                "spark.executor.instances와 일치하는지 확인"
            ),
            (
                "같은 Application에 동시 실행 중인 Job Run 확인 "
                "(emr_serverless pool로 직렬화되고 있는지)"
            ),
            "Application의 maximumCapacity 설정 확인",
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class Classification:
    """분류 결과. `error_type`이 UNKNOWN_ERROR면 rule은 None이다."""

    error_type: str
    rule: FailureRule | None
    evidence: tuple[str, ...]

    @property
    def is_known(self) -> bool:
        return self.rule is not None


def mask_values(text: str, values) -> str:
    """알고 있는 비밀값을 지우고, 값을 몰라도 형태로 알아볼 수 있는 것은 패턴으로 지운다.

    driver는 기동 시 자기 sparkSubmitParameters를 stderr에 그대로 찍는데, 이 저장소는
    POSTGRES_PASSWORD를 driverEnv로 넘기므로 로그에 평문으로 남는다. 이 로그 조각이
    Slack 채널과 S3 기록으로 나가기 때문에 반드시 먼저 지운다.
    """
    for value in values:
        if value and len(value) >= _MIN_MASKABLE_LENGTH:
            text = text.replace(value, MASK)
    return _SECRET_ASSIGNMENT.sub(rf"\1{MASK}", text)


def extract_error_window(log_text: str) -> list[str]:
    """오류 키워드가 나온 줄의 앞뒤만 남긴다. 키워드가 없으면 빈 리스트."""
    lines = log_text.splitlines()
    hit_indexes = [index for index, line in enumerate(lines) if _is_error_line(line)]
    if not hit_indexes:
        return []

    keep: set[int] = set()
    for index in hit_indexes:
        start = max(0, index - _WINDOW_BEFORE)
        end = min(len(lines), index + _WINDOW_AFTER + 1)
        keep.update(range(start, end))

    window = [lines[index] for index in sorted(keep)]
    # 원인은 보통 마지막 실패 지점에 가까우므로 앞쪽을 버린다.
    return window[-_MAX_WINDOW_LINES:]


def _is_error_line(line: str) -> bool:
    return bool(_ERROR_KEYWORDS.search(line)) and not _BENIGN_NOISE.search(line)


def _detect_terminated_side(window: list[str]) -> str | None:
    """exit 137처럼 양쪽에 공통인 신호를 어느 쪽 것으로 볼지 판별한다.

    executor 쪽 신호를 우선한다 — executor가 죽으면 driver도 뒤이어 종료 로그를 남기므로,
    driver 신호만 보고 driver 문제로 단정하면 오진한다. 어느 쪽도 확실하지 않으면 None을
    돌려주고, side를 요구하는 룰은 적용하지 않는다(추측하지 않는다).
    """
    if any(_EXECUTOR_SIDE_MARKERS.search(line) for line in window):
        return EXECUTOR
    if any(_DRIVER_SIDE_MARKERS.search(line) for line in window):
        return DRIVER
    return None


def classify(window: list[str], *, max_evidence: int = 5) -> Classification:
    """오류 구간을 룰에 대조해 error_type과 근거가 된 실제 로그 줄을 돌려준다."""
    if not window:
        return Classification(error_type=UNKNOWN_ERROR, rule=None, evidence=())

    side = _detect_terminated_side(window)

    for rule in sorted(FAILURE_RULES, key=lambda item: item.priority):
        if rule.requires_side is not None and rule.requires_side != side:
            continue
        evidence = _matching_lines(window, rule, max_evidence)
        if evidence:
            return Classification(error_type=rule.error_type, rule=rule, evidence=evidence)

    # 어느 룰에도 걸리지 않았다. 원인을 지어내지 않고, 사람이 볼 만한 줄만 골라 준다.
    return Classification(
        error_type=UNKNOWN_ERROR,
        rule=None,
        evidence=_keyword_lines(window, max_evidence),
    )


def _matching_lines(window: list[str], rule: FailureRule, limit: int) -> tuple[str, ...]:
    matched: list[str] = []
    for line in window:
        stripped = line.strip()
        if not stripped or stripped in matched:
            continue
        if any(pattern.search(line) for pattern in rule.patterns):
            matched.append(stripped)
            if len(matched) >= limit:
                break
    return tuple(matched)


def _keyword_lines(window: list[str], limit: int) -> tuple[str, ...]:
    # UNKNOWN일 때 보여줄 줄. 뒤에서부터 고른다 — 실제 실패 지점에 더 가깝다.
    matched: list[str] = []
    for line in reversed(window):
        stripped = line.strip()
        if not stripped or stripped in matched:
            continue
        if _is_error_line(line):
            matched.append(stripped)
            if len(matched) >= limit:
                break
    return tuple(reversed(matched))
