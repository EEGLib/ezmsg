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

    with ez.init((host, port), auto_start=True) as ctx:
        ctx.create_subscription("/TEST", callback=lambda _: None)
        assert ctx.spin_once(timeout=0.01) is False


def test_sync_backpressure_blocks_publish():
    host = "127.0.0.1"
    port = _free_port()

    with ez.init((host, port), auto_start=True) as ctx:
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
        done.wait(2.0)

        ctx.shutdown()
        spin_thread.join(timeout=1.0)


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
    assert result is None
