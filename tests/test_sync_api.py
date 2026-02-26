import asyncio
import socket
import threading
import time
from contextlib import asynccontextmanager

import pytest

import ezmsg.core as ez
from ezmsg.core import sync as sync_mod
from ezmsg.core.messagecache import CacheMiss


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_sync_autostart_and_spin_once_no_messages():
    host = "127.0.0.1"
    port = _free_port()

    with ez.sync.init((host, port), auto_start=True) as ctx:
        ctx.create_subscription("/TEST", callback=lambda _: None)
        assert ctx.spin_once(timeout=0.01) is False


def test_sync_backpressure_blocks_publish():
    host = "127.0.0.1"
    port = _free_port()

    with ez.sync.init((host, port), auto_start=True) as ctx:
        received = []
        done = threading.Event()

        def on_msg(msg: str) -> None:
            time.sleep(0.2)
            received.append(msg)
            if len(received) >= 2:
                done.set()

        ctx.create_subscription("/TEST", callback=on_msg, zero_copy=True)
        pub = ctx.create_publisher("/TEST", num_buffers=1, force_tcp=True)

        spin_thread = threading.Thread(
            target=ctx.spin,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        spin_thread.start()

        time.sleep(0.05)
        pub.publish("one")
        t1 = time.monotonic()
        pub.publish("two")
        t2 = time.monotonic()

        assert t2 - t1 >= 0.15
        assert done.wait(2.0), "Timed out waiting for messages to be received"
        assert received == ["one", "two"]

        ctx.shutdown()
        spin_thread.join(timeout=1.0)


def test_spin_once_processes_all_ready_callbacks():
    host = "127.0.0.1"
    port = _free_port()

    with ez.sync.init((host, port), auto_start=True) as ctx:
        received: list[tuple[str, str]] = []

        def on_a(msg: str) -> None:
            received.append(("A", msg))

        def on_b(msg: str) -> None:
            received.append(("B", msg))

        ctx.create_subscription("/A", callback=on_a, zero_copy=True)
        ctx.create_subscription("/B", callback=on_b, zero_copy=True)

        pub_a = ctx.create_publisher("/A", num_buffers=1, force_tcp=True)
        pub_b = ctx.create_publisher("/B", num_buffers=1, force_tcp=True)

        time.sleep(0.05)
        pub_a.publish("one")
        pub_b.publish("two")
        time.sleep(0.05)

        assert ctx.spin_once(timeout=0.2) is True
        assert set(received) == {("A", "one"), ("B", "two")}


@pytest.mark.asyncio
async def test_recv_any_cachemiss_does_not_raise():
    class DummySub:
        @asynccontextmanager
        async def recv_zero_copy(self):
            raise CacheMiss
            yield

    class DummySyncSub:
        def __init__(self) -> None:
            self._sub = DummySub()

    entry = (DummySyncSub(), lambda _: None, True)
    result = await sync_mod._recv_any([entry], timeout=0.01)
    assert result == []


@pytest.mark.asyncio
async def test_recv_any_returns_all_ready_contexts():
    class DummyCM:
        def __init__(self, name: str, gate: asyncio.Event | None) -> None:
            self._name = name
            self._gate = gate
            self.exited = asyncio.Event()

        async def __aenter__(self):
            if self._gate is not None:
                try:
                    await self._gate.wait()
                except asyncio.CancelledError:
                    await self._gate.wait()
            return f"msg-{self._name}"

        async def __aexit__(self, exc_type, exc, tb):
            self.exited.set()

    class DummySub:
        def __init__(self, cm: DummyCM) -> None:
            self._cm = cm

        def recv_zero_copy(self):
            return self._cm

    class DummySyncSub:
        def __init__(self, cm: DummyCM) -> None:
            self._sub = DummySub(cm)

    release = asyncio.Event()
    cms: list[DummyCM] = []
    entries = []

    def add_entry(name: str, gate: asyncio.Event | None) -> None:
        cm = DummyCM(name, gate)
        cms.append(cm)
        entries.append((DummySyncSub(cm), lambda _: None, True))

    add_entry("fast", None)
    add_entry("slow1", release)
    add_entry("slow2", release)

    async def _release_pending() -> None:
        await asyncio.sleep(0.01)
        release.set()

    asyncio.create_task(_release_pending())

    result = await sync_mod._recv_any(entries, timeout=0.2)
    assert len(result) == 3

    returned_cms = {cm for _, cm, _ in result}
    assert returned_cms == set(cms)

    for _, cm, _ in result:
        await cm.__aexit__(None, None, None)

    for cm in cms:
        assert cm.exited.is_set()
