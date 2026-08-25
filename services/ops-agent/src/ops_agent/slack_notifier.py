# Airflow SlackHook에 의존하지 않고 slack_sdk로 직접 chat_postMessage를 호출하는 독립 알림기(#447).

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ops_agent.owners import ServiceOwner


class SlackClient(Protocol):
    """slack_sdk.WebClient가 만족하는 최소 인터페이스 — 테스트에서 fake로 대체한다."""

    def chat_postMessage(self, *, channel: str, text: str) -> object: ...


def build_client(bot_token: str) -> SlackClient:
    from slack_sdk import WebClient

    return WebClient(token=bot_token)


@dataclass(frozen=True, slots=True)
class SlackNotifier:
    channel: str
    client: SlackClient

    def post(self, text: str) -> None:
        self.client.chat_postMessage(channel=self.channel, text=text)


def mention_text(owner: ServiceOwner | None) -> str:
    # 실시간 email 조회(users.lookupByEmail)는 하지 않는다 — MVP 범위, notifications.py와 다른 점.
    if owner is None:
        return "(no owner configured)"
    if owner.slack_id:
        return f"<@{owner.slack_id}>"
    if owner.email:
        return f"{owner.name} ({owner.email})"
    return owner.name
