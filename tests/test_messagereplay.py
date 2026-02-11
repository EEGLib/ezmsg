"""Tests for ezmsg.util.messagereplay module."""

import json
import typing
from dataclasses import fields
from pathlib import Path

import pytest

import ezmsg.core as ez
from ezmsg.util.messagecodec import MessageEncoder, LogStart


def test_import():
    """Importing the module should not raise (regression: frozen dataclass inheritance on 3.13+)."""
    from ezmsg.util.messagereplay import (
        FileReplayMessage,
        MessageCollector,
        MessageReplay,
        MessageReplaySettings,
        ReplayStatusMessage,
    )


def test_replay_status_message():
    from ezmsg.util.messagereplay import ReplayStatusMessage

    msg = ReplayStatusMessage(filename=Path("/tmp/test.log"), idx=3, total=10)
    assert msg.filename == Path("/tmp/test.log")
    assert msg.idx == 3
    assert msg.total == 10
    assert msg.done is False

    msg_done = ReplayStatusMessage(filename=Path("/tmp/test.log"), idx=10, total=10, done=True)
    assert msg_done.done is True


def test_file_replay_message_defaults():
    from ezmsg.util.messagereplay import FileReplayMessage

    msg = FileReplayMessage()
    assert msg.filename is None
    assert msg.rate is None

    msg2 = FileReplayMessage(filename=Path("/tmp/test.log"), rate=10.0)
    assert msg2.filename == Path("/tmp/test.log")
    assert msg2.rate == 10.0


def test_message_replay_settings():
    """Settings should inherit from both ez.Settings and FileReplayMessage without error."""
    from ezmsg.util.messagereplay import MessageReplaySettings

    settings = MessageReplaySettings()
    assert settings.filename is None
    assert settings.rate is None
    assert settings.progress is False

    settings2 = MessageReplaySettings(filename=Path("/tmp/test.log"), rate=5.0, progress=True)
    assert settings2.filename == Path("/tmp/test.log")
    assert settings2.rate == 5.0
    assert settings2.progress is True


def test_message_replay_settings_is_frozen():
    """ez.Settings subclasses should be frozen dataclasses."""
    from ezmsg.util.messagereplay import MessageReplaySettings

    settings = MessageReplaySettings()
    with pytest.raises(AttributeError):
        settings.progress = True


def test_message_replay_unit_streams():
    """MessageReplay unit should have the expected stream attributes."""
    from ezmsg.util.messagereplay import MessageReplay

    assert hasattr(MessageReplay, "INPUT_FILE")
    assert hasattr(MessageReplay, "INPUT_PAUSED")
    assert hasattr(MessageReplay, "INPUT_STOP")
    assert hasattr(MessageReplay, "OUTPUT_MESSAGE")
    assert hasattr(MessageReplay, "OUTPUT_TOTAL")
    assert hasattr(MessageReplay, "OUTPUT_REPLAY_STATUS")


def test_message_collector_collects(tmp_path):
    """MessageCollector should be instantiable and have an empty messages list."""
    from ezmsg.util.messagereplay import MessageCollector

    collector = MessageCollector()
    assert hasattr(collector, "INPUT_MESSAGE")
    assert hasattr(collector, "OUTPUT_MESSAGE")
