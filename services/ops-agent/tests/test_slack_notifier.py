from __future__ import annotations

from ops_agent.owners import ServiceOwner
from ops_agent.slack_notifier import SlackNotifier, mention_text
from ops_agent_test_support import FakeSlackClient


class TestSlackNotifier:
    def test_post_sends_to_the_configured_channel(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)

        notifier.post("hello")

        assert client.messages == [("#alerts", "hello")]

    def test_post_returns_the_message_timestamp_for_threading(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)

        ts = notifier.post("hello")

        assert ts == "1700000000.000001"

    def test_post_passes_blocks_through(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "hi"}}]

        notifier.post("fallback", blocks=blocks)

        assert client.calls[0]["blocks"] == blocks
        assert client.calls[0]["text"] == "fallback"

    def test_a_thread_reply_targets_the_parent_message(self):
        client = FakeSlackClient()
        notifier = SlackNotifier(channel="#alerts", client=client)

        notifier.post_thread_reply("1700000000.000001", "log tail")

        assert client.calls[0]["thread_ts"] == "1700000000.000001"
        assert client.calls[0]["text"] == "log tail"


class TestMentionText:
    def test_prefers_slack_id_when_present(self):
        owner = ServiceOwner(name="bob", email="bob@example.com", slack_id="U123", severity="high")

        assert mention_text(owner) == "<@U123>"

    def test_falls_back_to_email_when_no_slack_id(self):
        owner = ServiceOwner(name="alice", email="alice@example.com", slack_id=None, severity="high")

        assert mention_text(owner) == "alice (alice@example.com)"

    def test_falls_back_to_name_when_neither_is_present(self):
        owner = ServiceOwner(name="carol", email=None, slack_id=None, severity="low")

        assert mention_text(owner) == "carol"

    def test_no_owner_configured(self):
        assert mention_text(None) == "(no owner configured)"
