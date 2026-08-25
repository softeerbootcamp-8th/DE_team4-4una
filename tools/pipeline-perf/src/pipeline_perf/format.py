"""Formatting helpers shared by the report renderer and the compare table."""

from __future__ import annotations

_UNITS = ("B", "KiB", "MiB", "GiB", "TiB")


def duration(seconds: float | None) -> str:
    """초를 `m:ss` 또는 `h:mm:ss`로 쓴다. None은 `-`."""
    if seconds is None:
        return "-"
    total = round(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, second = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{second:02d}"
    return f"{minutes}:{second:02d}"


def milliseconds(value: int | None) -> str:
    """ms 값을 쓴다. 10초 미만은 `1.9s`처럼 소수 1자리로 — Spark task duration은
    초 단위로 반올림하면 분포가 통째로 0:00이 되어 버린다."""
    if value is None:
        return "-"
    if value < 10_000:
        return f"{value / 1000:.1f}s"
    return duration(value / 1000)


def size(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "-"
    value = float(num_bytes)
    for unit in _UNITS:
        if abs(value) < 1024 or unit == _UNITS[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} {_UNITS[-1]}"


def number(value: float | None, digits: int = 0) -> str:
    if value is None:
        return "-"
    if digits:
        return f"{value:,.{digits}f}"
    return f"{value:,.0f}"


def percent(ratio: float | None) -> str:
    if ratio is None:
        return "-"
    return f"{ratio * 100:.1f}%"


def truncate(text: str | None, limit: int = 70) -> str:
    if not text:
        return "-"
    single_line = " ".join(text.split())
    if len(single_line) <= limit:
        return single_line
    return single_line[: limit - 1] + "…"
