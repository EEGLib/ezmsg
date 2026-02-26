from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import TimeoutError as FutureTimeoutError
from copy import deepcopy
from typing import Any, Callable, Iterable

from .backendprocess import new_threaded_event_loop
from .graphcontext import GraphContext
from .messagecache import CacheMiss
from .netprotocol import AddressType
from .pubclient import Publisher
from .subclient import Subscriber

logger = logging.getLogger("ezmsg")


def _future_result(future, timeout: float | None):
    try:
        return future.result(timeout=timeout)
    except FutureTimeoutError as exc:
        future.cancel()
        raise TimeoutError("Timed out waiting for async operation") from exc


class SyncPublisher:
    """Synchronous wrapper around ezmsg Publisher."""

    def __init__(self, pub: Publisher, loop: asyncio.AbstractEventLoop) -> None:
        self._pub = pub
        self._loop = loop

    def publish(self, obj: Any, timeout: float | None = None) -> None:
        fut = asyncio.run_coroutine_threadsafe(self._pub.broadcast(obj), self._loop)
        _future_result(fut, timeout)

    def broadcast(self, obj: Any, timeout: float | None = None) -> None:
        self.publish(obj, timeout=timeout)

    def sync(self, timeout: float | None = None) -> None:
        fut = asyncio.run_coroutine_threadsafe(self._pub.sync(), self._loop)
        _future_result(fut, timeout)

    def pause(self) -> None:
        self._loop.call_soon_threadsafe(self._pub.pause)

    def resume(self) -> None:
        self._loop.call_soon_threadsafe(self._pub.resume)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._pub.close)

    def wait_closed(self, timeout: float | None = None) -> None:
        fut = asyncio.run_coroutine_threadsafe(self._pub.wait_closed(), self._loop)
        _future_result(fut, timeout)


class _SyncZeroCopy:
    def __init__(
        self,
        sub: Subscriber,
        loop: asyncio.AbstractEventLoop,
        timeout: float | None,
    ) -> None:
        self._sub = sub
        self._loop = loop
        self._timeout = timeout
        self._cm = None

    def __enter__(self) -> Any:
        self._cm = self._sub.recv_zero_copy()
        fut = asyncio.run_coroutine_threadsafe(self._cm.__aenter__(), self._loop)
        return _future_result(fut, self._timeout)

    def __exit__(self, exc_type, exc, tb) -> bool:
        assert self._cm is not None
        fut = asyncio.run_coroutine_threadsafe(
            self._cm.__aexit__(exc_type, exc, tb), self._loop
        )
        return bool(_future_result(fut, self._timeout))


class SyncSubscriber:
    """Synchronous wrapper around ezmsg Subscriber."""

    def __init__(
        self,
        sub: Subscriber,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._sub = sub
        self._loop = loop

    def recv(self, timeout: float | None = None) -> Any:
        fut = asyncio.run_coroutine_threadsafe(self._sub.recv(), self._loop)
        return _future_result(fut, timeout)

    def recv_zero_copy(self, timeout: float | None = None) -> _SyncZeroCopy:
        return _SyncZeroCopy(self._sub, self._loop, timeout)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._sub.close)

    def wait_closed(self, timeout: float | None = None) -> None:
        fut = asyncio.run_coroutine_threadsafe(self._sub.wait_closed(), self._loop)
        _future_result(fut, timeout)


class SyncContext:
    """Synchronous interface for ezmsg's low-level pub/sub API."""

    def __init__(
        self, graph_address: AddressType | None = None, auto_start: bool | None = None
    ) -> None:
        self._graph_address = graph_address
        self._auto_start = auto_start
        self._graph_context: GraphContext | None = None
        self._loop_cm = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._shutdown_requested = threading.Event()
        self._spinning = False
        self._closed = False
        self._spin_future = None
        self._callback_subscriptions: list[
            tuple[SyncSubscriber, Callable[[Any], None], bool]
        ] = []

    @property
    def graph_address(self) -> AddressType | None:
        if self._graph_context is None:
            return self._graph_address
        return self._graph_context.graph_address

    def __enter__(self) -> "SyncContext":

        # SyncContext instances are single-use: they cannot be re-entered after shutdown.
        if self._closed:
            raise RuntimeError(
                "SyncContext instances cannot be reused after shutdown; "
                "create a new SyncContext instead."
            )
        
        if self._loop_cm is not None:
            return self

        self._loop_cm = new_threaded_event_loop()
        self._loop = self._loop_cm.__enter__()

        graph_context = GraphContext(self._graph_address, auto_start=self._auto_start)

        async def _enter() -> GraphContext:
            return await graph_context.__aenter__()

        self._graph_context = _future_result(
            asyncio.run_coroutine_threadsafe(_enter(), self._loop), None
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.shutdown()
        return False

    def publisher(self, topic: str, **kwargs) -> SyncPublisher:
        return self.create_publisher(topic, **kwargs)

    def create_publisher(self, topic: str, **kwargs) -> SyncPublisher:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(
            self._graph_context.publisher(topic, **kwargs), self._loop
        )
        pub = _future_result(fut, None)
        return SyncPublisher(pub, self._loop)

    def subscriber(self, topic: str, **kwargs) -> SyncSubscriber:
        return self.create_subscription(topic, **kwargs)

    def create_subscription(
        self,
        topic: str,
        callback: Callable[[Any], None] | None = None,
        *,
        zero_copy: bool = False,
        **kwargs,
    ) -> SyncSubscriber:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(
            self._graph_context.subscriber(topic, **kwargs), self._loop
        )
        sub = _future_result(fut, None)
        sync_sub = SyncSubscriber(sub, self._loop)
        if callback is not None:
            self._callback_subscriptions.append((sync_sub, callback, zero_copy))
        return sync_sub

    def connect(self, from_topic: str, to_topic: str, timeout: float | None = None) -> None:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(
            self._graph_context.connect(from_topic, to_topic), self._loop
        )
        _future_result(fut, timeout)

    def disconnect(
        self, from_topic: str, to_topic: str, timeout: float | None = None
    ) -> None:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(
            self._graph_context.disconnect(from_topic, to_topic), self._loop
        )
        _future_result(fut, timeout)

    def sync(self, timeout: float | None = None) -> None:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(self._graph_context.sync(), self._loop)
        _future_result(fut, timeout)

    def pause(self, timeout: float | None = None) -> None:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(self._graph_context.pause(), self._loop)
        _future_result(fut, timeout)

    def resume(self, timeout: float | None = None) -> None:
        self._ensure_started()
        assert self._graph_context is not None
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(self._graph_context.resume(), self._loop)
        _future_result(fut, timeout)

    def spin(self, poll_interval: float = 0.1) -> None:
        self._ensure_started()
        if self._shutdown_requested.is_set():
            return

        self._spinning = True
        try:
            while not self._shutdown_requested.is_set():
                self.spin_once(timeout=poll_interval)
        except KeyboardInterrupt:
            self.shutdown()
        finally:
            self._spinning = False
            if self._shutdown_requested.is_set() and not self._closed:
                self._finalize_shutdown()

    def spin_once(self, timeout: float | None = 0.0) -> bool:
        """
        Process at most one subscription callback.

        :param timeout: Seconds to wait for a callback. Use None to block forever.
        :return: True if a callback was processed, False otherwise.
        """
        self._ensure_started()
        if self._shutdown_requested.is_set():
            return False

        if not self._callback_subscriptions:
            if timeout is None:
                self._shutdown_requested.wait()
            elif timeout > 0:
                self._shutdown_requested.wait(timeout)
            return False

        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(
            _recv_any(self._callback_subscriptions, timeout), self._loop
        )
        self._spin_future = fut
        keep_future = False
        try:
            result = _future_result(fut, None)
        except KeyboardInterrupt:
            if not fut.done():
                fut.cancel()
            keep_future = True
            raise
        finally:
            if not keep_future:
                self._spin_future = None
        if result is None:
            return False

        entry, cm, msg = result
        _, callback, zero_copy = entry

        try:
            if not zero_copy:
                msg = deepcopy(msg)
            callback(msg)
        except Exception:
            logger.exception("Unhandled exception in subscription callback")
        finally:
            exit_fut = asyncio.run_coroutine_threadsafe(
                cm.__aexit__(None, None, None), self._loop
            )
            try:
                _future_result(exit_fut, None)
            except CacheMiss:
                logger.warning(
                    "Cache miss while releasing message; publisher likely exited."
                )
            except Exception:
                logger.exception("Failed while releasing message backpressure")
        return True

    def shutdown(self) -> None:
        self._shutdown_requested.set()
        if self._spinning:
            return
        self._finalize_shutdown()

    def _finalize_shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._spin_future is not None and not self._spin_future.done():
            self._spin_future.cancel()
            try:
                self._spin_future.result(timeout=1.0)
            except Exception:
                pass
        self._spin_future = None

        if self._loop is not None:

            async def _cleanup() -> None:
                if self._graph_context is not None:
                    await self._graph_context.__aexit__(None, None, None)

            fut = asyncio.run_coroutine_threadsafe(_cleanup(), self._loop)
            _future_result(fut, None)

        if self._loop_cm is not None:
            self._loop_cm.__exit__(None, None, None)

        self._loop_cm = None
        self._loop = None
        self._graph_context = None

    def _ensure_started(self) -> None:
        if self._loop is None or self._graph_context is None:
            raise RuntimeError("SyncContext must be entered with 'with' before use")


def init(
    graph_address: AddressType | None = None, auto_start: bool | None = None
) -> SyncContext:
    """Create a SyncContext for low-level synchronous usage."""
    return SyncContext(graph_address=graph_address, auto_start=auto_start)


def spin(context: SyncContext, poll_interval: float = 0.1) -> None:
    """Spin a SyncContext, dispatching subscription callbacks."""
    context.spin(poll_interval=poll_interval)


def spin_once(context: SyncContext, timeout: float | None = 0.0) -> bool:
    """Process at most one subscription callback."""
    return context.spin_once(timeout=timeout)


async def _recv_any(
    entries: Iterable[tuple[SyncSubscriber, Callable[[Any], None], bool]],
    timeout: float | None,
) -> tuple[tuple[SyncSubscriber, Callable[[Any], None], bool], Any, Any] | None:
    async def _cleanup_result(result: Any) -> None:
        if isinstance(result, BaseException):
            return
        try:
            _, cm, _ = result
        except Exception:
            return
        try:
            await cm.__aexit__(None, None, None)
        except CacheMiss:
            logger.warning(
                "Cache miss while releasing message; publisher likely exited."
            )
        except Exception:
            logger.exception("Failed while releasing message backpressure")

    async def _recv_entry(
        entry: tuple[SyncSubscriber, Callable[[Any], None], bool]
    ) -> tuple[tuple[SyncSubscriber, Callable[[Any], None], bool], Any, Any]:
        sub, _, _ = entry
        cm = sub._sub.recv_zero_copy()
        msg = await cm.__aenter__()
        return entry, cm, msg

    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout

    while True:
        remaining = None if deadline is None else max(0.0, deadline - loop.time())
        tasks = [asyncio.create_task(_recv_entry(entry)) for entry in entries]
        try:
            done, pending = await asyncio.wait(
                tasks, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                for task in pending:
                    try:
                        task.cancel()
                    except RuntimeError:
                        pass
                pending_results = await asyncio.gather(
                    *pending, return_exceptions=True
                )
                for result in pending_results:
                    await _cleanup_result(result)
                return None

            winner_result = None
            for task in done:
                try:
                    result = task.result()
                except CacheMiss:
                    # Likely stale notification after publisher exit; keep waiting.
                    continue
                except asyncio.CancelledError:
                    continue
                except Exception:
                    logger.exception("Sync subscription receive failed")
                    continue
                if winner_result is None:
                    winner_result = result
                else:
                    await _cleanup_result(result)

            for task in pending:
                try:
                    task.cancel()
                except RuntimeError:
                    pass
            pending_results = await asyncio.gather(
                *pending, return_exceptions=True
            )
            for result in pending_results:
                await _cleanup_result(result)

            if winner_result is not None:
                return winner_result

            # Only CacheMiss/cancelled/error occurred; continue within timeout window.
            if deadline is not None and loop.time() >= deadline:
                return None
        finally:
            for task in tasks:
                if not task.done():
                    try:
                        task.cancel()
                    except RuntimeError:
                        pass
