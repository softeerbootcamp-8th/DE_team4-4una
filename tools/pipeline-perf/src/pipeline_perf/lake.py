"""Bronze input partition file statistics (#462).

Job Run이 읽는 파티션이 작은 파일 수천 개인지 큰 파일 몇 개인지에 따라 같은 데이터량
이라도 태스크 수와 스케줄링 오버헤드가 달라진다. small-file 기여도를 판단하려면
실행 지표 옆에 입력 파일 분포가 함께 있어야 한다.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol


class Reader(Protocol):
    def list_objects(self, uri: str) -> list[Any]: ...


def bronze_partition_uri(root_uri: str, target_hour: datetime) -> str:
    """stream-processor bronze_sink / batch-jobs cleansing과 같은 파티션 규칙."""
    return f"{root_uri.rstrip('/')}/event_date={target_hour.date().isoformat()}/hour={target_hour.hour:02d}"


def describe_partition(reader: Reader, partition_uri: str) -> dict[str, Any]:
    """파티션 하나의 파일 개수·총 바이트·평균/최소/최대 크기."""
    objects = [
        metadata
        for metadata in reader.list_objects(partition_uri)
        if not metadata.uri.endswith("/") and not metadata.uri.endswith("_SUCCESS")
    ]
    sizes = sorted(metadata.size for metadata in objects)
    total = sum(sizes)
    return {
        "partition_uri": partition_uri,
        "file_count": len(sizes),
        "total_bytes": total,
        "avg_bytes": round(total / len(sizes)) if sizes else None,
        "min_bytes": sizes[0] if sizes else None,
        "max_bytes": sizes[-1] if sizes else None,
    }
