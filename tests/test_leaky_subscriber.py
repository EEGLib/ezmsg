"""Tests for leaky subscriber functionality."""

import asyncio
from uuid import uuid4

import pytest
from ezmsg.core.stream import InputStream


def test_input_stream_default_not_leaky():
    """InputStream defaults to non-leaky with no queue limit."""
    stream = InputStream(float)
    assert stream.leaky is False
    assert stream.max_queue is None


def test_input_stream_leaky_configuration():
    """InputStream accepts leaky and max_queue parameters."""
    stream = InputStream(float, leaky=True, max_queue=5)
    assert stream.leaky is True
    assert stream.max_queue == 5


def test_input_stream_leaky_requires_max_queue():
    """Leaky mode requires max_queue to be set."""
    with pytest.raises(ValueError, match="max_queue must be set"):
        InputStream(float, leaky=True)


def test_input_stream_max_queue_must_be_positive():
    """max_queue must be a positive integer."""
    with pytest.raises(ValueError, match="must be positive"):
        InputStream(float, leaky=True, max_queue=0)
    with pytest.raises(ValueError, match="must be positive"):
        InputStream(float, leaky=True, max_queue=-1)


@pytest.mark.asyncio
async def test_subscriber_leaky_queue_drops_oldest():
    """Leaky subscriber drops oldest messages when queue is full."""
    # Test the leaky queue helper class directly
    from ezmsg.core.messagechannel import LeakyQueue

    queue = LeakyQueue(2)

    # Fill the queue
    await queue.put(("pub1", 1))
    await queue.put(("pub1", 2))
    assert queue.qsize() == 2

    # Adding third item should drop the oldest
    await queue.put(("pub1", 3))
    assert queue.qsize() == 2

    # Should get items 2 and 3 (1 was dropped)
    item1 = await queue.get()
    item2 = await queue.get()
    assert item1 == ("pub1", 2)
    assert item2 == ("pub1", 3)


def test_subscriber_init_creates_leaky_queue():
    """Subscriber creates LeakyQueue when leaky=True."""
    from ezmsg.core.subclient import Subscriber
    from ezmsg.core.messagechannel import LeakyQueue

    # Access internal constructor with guard bypass for testing
    sub = Subscriber(
        id=uuid4(),
        topic="test",
        graph_address=None,
        leaky=True,
        max_queue=5,
        _guard=Subscriber._SENTINEL,
    )

    assert isinstance(sub._incoming, LeakyQueue)
    assert sub._incoming.maxsize == 5


def test_subscriber_init_default_unbounded_queue():
    """Subscriber creates standard asyncio.Queue by default (not LeakyQueue)."""
    from ezmsg.core.subclient import Subscriber
    from ezmsg.core.messagechannel import LeakyQueue

    sub = Subscriber(
        id=uuid4(),
        topic="test",
        graph_address=None,
        _guard=Subscriber._SENTINEL,
    )

    # Default should be a standard asyncio.Queue, not LeakyQueue
    assert isinstance(sub._incoming, asyncio.Queue)
    assert not isinstance(sub._incoming, LeakyQueue)
    assert sub._incoming.maxsize == 0  # 0 means unlimited in asyncio.Queue


@pytest.mark.asyncio
async def test_graphcontext_subscriber_passes_leaky_params():
    """GraphContext.subscriber passes leaky and max_queue to Subscriber.create."""
    from unittest.mock import patch, AsyncMock
    from ezmsg.core.graphcontext import GraphContext

    ctx = GraphContext(graph_address=None)

    with patch('ezmsg.core.graphcontext.Subscriber.create', new_callable=AsyncMock) as mock_create:
        mock_create.return_value = AsyncMock()

        await ctx.subscriber("test_topic", leaky=True, max_queue=3)

        mock_create.assert_called_once_with(
            "test_topic",
            None,  # graph_address
            leaky=True,
            max_queue=3
        )


def test_subscriber_decorator_extracts_leaky_from_stream():
    """@subscriber decorator extracts leaky config from InputStream."""
    from ezmsg.core.unit import subscriber, LEAKY_ATTR, MAX_QUEUE_ATTR

    INPUT = InputStream(float, leaky=True, max_queue=10)

    @subscriber(INPUT)
    async def process(self, msg: float):
        pass

    assert getattr(process, LEAKY_ATTR, False) is True
    assert getattr(process, MAX_QUEUE_ATTR, None) == 10


def test_subscriber_decorator_default_not_leaky():
    """@subscriber decorator defaults to non-leaky."""
    from ezmsg.core.unit import subscriber, LEAKY_ATTR, MAX_QUEUE_ATTR

    INPUT = InputStream(float)

    @subscriber(INPUT)
    async def process(self, msg: float):
        pass

    assert getattr(process, LEAKY_ATTR, False) is False
    assert getattr(process, MAX_QUEUE_ATTR, None) is None


@pytest.mark.asyncio
async def test_leaky_queue_under_load():
    """Test LeakyQueue behavior under simulated load."""
    from ezmsg.core.messagechannel import LeakyQueue

    queue = LeakyQueue(3)

    # Simulate fast producer
    for i in range(100):
        await queue.put(("pub", i))

    # Queue should only have last 3 items
    assert queue.qsize() == 3

    items = []
    while not queue.empty():
        items.append(await queue.get())

    # Should have the most recent items
    assert items == [("pub", 97), ("pub", 98), ("pub", 99)]


@pytest.mark.asyncio
async def test_leaky_queue_on_drop_callback():
    """LeakyQueue calls on_drop callback when dropping items."""
    from ezmsg.core.messagechannel import LeakyQueue

    dropped_items = []

    def on_drop(item):
        dropped_items.append(item)

    queue = LeakyQueue(2, on_drop)

    # Fill the queue
    await queue.put(("pub", 1))
    await queue.put(("pub", 2))
    assert len(dropped_items) == 0

    # This should trigger drop of item 1
    await queue.put(("pub", 3))
    assert len(dropped_items) == 1
    assert dropped_items[0] == ("pub", 1)

    # This should trigger drop of item 2
    await queue.put(("pub", 4))
    assert len(dropped_items) == 2
    assert dropped_items[1] == ("pub", 2)


@pytest.mark.asyncio
async def test_leaky_queue_on_drop_callback_put_nowait():
    """LeakyQueue calls on_drop callback when using put_nowait."""
    from ezmsg.core.messagechannel import LeakyQueue

    dropped_items = []

    def on_drop(item):
        dropped_items.append(item)

    queue = LeakyQueue(2, on_drop)

    # Fill the queue
    queue.put_nowait(("pub", 1))
    queue.put_nowait(("pub", 2))
    assert len(dropped_items) == 0

    # This should trigger drop of item 1
    queue.put_nowait(("pub", 3))
    assert len(dropped_items) == 1
    assert dropped_items[0] == ("pub", 1)


def test_channel_release_without_get():
    """Channel.release_without_get frees backpressure without yielding message."""
    from unittest.mock import MagicMock
    from ezmsg.core.messagechannel import Channel

    pub_id = uuid4()
    client_id = uuid4()
    num_buffers = 4

    # Create channel with guard bypass
    chan = Channel(
        id=uuid4(),
        pub_id=pub_id,
        num_buffers=num_buffers,
        shm=None,
        graph_address=None,
        _guard=Channel._SENTINEL,
    )

    # Register a client
    chan.register_client(client_id, queue=MagicMock())

    # Simulate a leased buffer (as if notify_clients was called)
    msg_id = 100
    buf_idx = msg_id % num_buffers
    chan.backpressure.lease(client_id, buf_idx)

    # Put a message in the cache (required for release to work)
    chan.cache.put_local("test_message", msg_id)

    # Verify lease is held
    assert not chan.backpressure.available(buf_idx)

    # Mock _acknowledge to verify it gets called
    chan._acknowledge = MagicMock()

    # Release without getting the message
    chan.release_without_get(msg_id, client_id)

    # Verify backpressure is freed
    assert chan.backpressure.available(buf_idx)

    # Verify acknowledge was called (since no local backpressure)
    chan._acknowledge.assert_called_once_with(msg_id)


def test_subscriber_leaky_queue_has_on_drop_callback():
    """Leaky Subscriber's queue has on_drop callback configured."""
    from ezmsg.core.subclient import Subscriber
    from ezmsg.core.messagechannel import LeakyQueue

    sub = Subscriber(
        id=uuid4(),
        topic="test",
        graph_address=None,
        leaky=True,
        max_queue=5,
        _guard=Subscriber._SENTINEL,
    )

    assert isinstance(sub._incoming, LeakyQueue)
    assert sub._incoming._on_drop is not None
    # The callback should be the subscriber's _handle_dropped_notification method
    assert sub._incoming._on_drop == sub._handle_dropped_notification


def test_subscriber_handle_dropped_notification_releases_backpressure():
    """Subscriber._handle_dropped_notification calls channel.release_without_get."""
    from unittest.mock import MagicMock
    from ezmsg.core.subclient import Subscriber
    from ezmsg.core.messagechannel import Channel

    pub_id = uuid4()
    sub_id = uuid4()
    num_buffers = 4

    # Create subscriber with leaky queue
    sub = Subscriber(
        id=sub_id,
        topic="test",
        graph_address=None,
        leaky=True,
        max_queue=2,
        _guard=Subscriber._SENTINEL,
    )

    # Create a mock channel
    mock_channel = MagicMock(spec=Channel)
    mock_channel.num_buffers = num_buffers

    # Register the channel with the subscriber
    sub._channels[pub_id] = mock_channel

    # Simulate a dropped notification
    msg_id = 100
    sub._handle_dropped_notification((pub_id, msg_id))

    # Verify release_without_get was called on the channel
    mock_channel.release_without_get.assert_called_once_with(msg_id, sub_id)


def test_leaky_subscriber_backpressure_integration():
    """
    Integration test: leaky subscriber releases backpressure when dropping.

    Simulates the full flow:
    1. Channel notifies subscriber (taking backpressure lease)
    2. Queue fills up
    3. New notification causes old one to be dropped
    4. Dropped notification triggers backpressure release
    """
    from unittest.mock import MagicMock
    from ezmsg.core.subclient import Subscriber
    from ezmsg.core.messagechannel import Channel

    pub_id = uuid4()
    sub_id = uuid4()
    num_buffers = 4

    # Create a real channel
    chan = Channel(
        id=uuid4(),
        pub_id=pub_id,
        num_buffers=num_buffers,
        shm=None,
        graph_address=None,
        _guard=Channel._SENTINEL,
    )
    chan._acknowledge = MagicMock()  # Mock TCP acknowledgment

    # Create subscriber with leaky queue (max_queue=2)
    sub = Subscriber(
        id=sub_id,
        topic="test",
        graph_address=None,
        leaky=True,
        max_queue=2,
        _guard=Subscriber._SENTINEL,
    )

    # Wire the channel to the subscriber
    sub._channels[pub_id] = chan

    # Register subscriber with channel (simulating CHANNELS.register)
    chan.register_client(sub_id, sub._incoming)

    # Simulate channel notifying subscriber for msg_id=0, 1, 2
    # This is what _notify_clients does
    for msg_id in range(3):
        buf_idx = msg_id % num_buffers
        # Put message in cache (simulating what _publisher_connection does)
        chan.cache.put_local(f"message_{msg_id}", msg_id)
        chan.backpressure.lease(sub_id, buf_idx)
        sub._incoming.put_nowait((pub_id, msg_id))

    # Queue should have only 2 items (msg_id 1 and 2)
    # msg_id 0 should have been dropped and its backpressure released
    assert sub._incoming.qsize() == 2

    # Verify backpressure for buf_idx=0 (msg_id=0) was released
    assert chan.backpressure.available(0), "Backpressure for dropped msg should be released"

    # Verify backpressure for buf_idx=1,2 (msg_id=1,2) is still held
    assert not chan.backpressure.available(1), "Backpressure for queued msg should be held"
    assert not chan.backpressure.available(2), "Backpressure for queued msg should be held"

    # Verify the acknowledge was called for the dropped message
    chan._acknowledge.assert_called_once_with(0)
