"""Regression tests for clean shutdown behavior.

Ensures no "Task was destroyed but it is pending!" warnings or
"RuntimeError: Event loop is closed" errors during shutdown.
"""

import asyncio
import threading
import warnings
from unittest.mock import AsyncMock, MagicMock

import pytest

from ezmsg.core.netprotocol import close_stream_writer
from ezmsg.core.backendprocess import new_threaded_event_loop


# -- close_stream_writer tests ------------------------------------------------


@pytest.mark.asyncio
async def test_close_stream_writer_handles_runtime_error():
    """writer.close() raising RuntimeError (event loop closed) must not propagate."""
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.close.side_effect = RuntimeError("Event loop is closed")
    # Should return silently without calling wait_closed
    await close_stream_writer(writer)
    writer.close.assert_called_once()
    writer.wait_closed.assert_not_called()


@pytest.mark.asyncio
async def test_close_stream_writer_normal():
    """Normal close path still works: close() + wait_closed()."""
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.wait_closed = AsyncMock()
    await close_stream_writer(writer)
    writer.close.assert_called_once()
    writer.wait_closed.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_stream_writer_connection_reset():
    """ConnectionResetError from wait_closed is suppressed."""
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.wait_closed = AsyncMock(side_effect=ConnectionResetError)
    await close_stream_writer(writer)
    writer.close.assert_called_once()
    writer.wait_closed.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_stream_writer_broken_pipe():
    """BrokenPipeError from wait_closed is suppressed."""
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.wait_closed = AsyncMock(side_effect=BrokenPipeError)
    await close_stream_writer(writer)
    writer.close.assert_called_once()
    writer.wait_closed.assert_awaited_once()


# -- new_threaded_event_loop shutdown tests ------------------------------------


def test_threaded_loop_cancels_pending_tasks_on_exit():
    """Pending tasks in the loop must be cancelled before the loop closes,
    so Python does not emit 'Task was destroyed but it is pending!' warnings."""
    task_was_cancelled = threading.Event()

    async def long_running():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            task_was_cancelled.set()
            raise

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        with new_threaded_event_loop() as loop:
            # Schedule a task that will never complete on its own
            asyncio.run_coroutine_threadsafe(long_running(), loop)
            # Give the task time to start
            asyncio.run_coroutine_threadsafe(asyncio.sleep(0.05), loop).result(
                timeout=2.0
            )
        # Exiting the context manager should cancel and await the task

    task_was_cancelled.wait(timeout=5.0)
    assert task_was_cancelled.is_set(), "Pending task was not cancelled during shutdown"

    destroyed_warnings = [
        w for w in caught if "was destroyed but it is pending" in str(w.message)
    ]
    assert destroyed_warnings == [], (
        f"Got 'Task was destroyed' warnings: {destroyed_warnings}"
    )


def test_threaded_loop_clean_exit_no_tasks():
    """Loop with no remaining tasks shuts down cleanly."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        with new_threaded_event_loop() as loop:
            fut = asyncio.run_coroutine_threadsafe(asyncio.sleep(0), loop)
            fut.result(timeout=2.0)

    destroyed_warnings = [
        w for w in caught if "was destroyed but it is pending" in str(w.message)
    ]
    assert destroyed_warnings == [], (
        f"Got 'Task was destroyed' warnings: {destroyed_warnings}"
    )


def test_threaded_loop_multiple_pending_tasks_cancelled():
    """All pending tasks (not just one) are cancelled on shutdown."""
    cancel_count = 0
    lock = threading.Lock()

    async def sleeper():
        nonlocal cancel_count
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            with lock:
                cancel_count += 1
            raise

    with new_threaded_event_loop() as loop:
        for _ in range(5):
            asyncio.run_coroutine_threadsafe(sleeper(), loop)
        # Let them all start
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0.05), loop).result(timeout=2.0)

    # Brief wait for cancel callbacks
    import time

    time.sleep(0.2)
    assert cancel_count == 5, f"Expected 5 cancellations, got {cancel_count}"
