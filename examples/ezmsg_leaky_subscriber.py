# Leaky Subscriber Example
#
# This example demonstrates the "leaky subscriber" feature, which allows
# slow consumers to drop old messages rather than blocking fast producers.
#
# Scenario:
# - A fast publisher produces messages at ~10 Hz (every 100ms)
# - A slow subscriber processes messages at ~1 Hz (1000ms per message)
# - Without leaky mode: the publisher would be blocked by backpressure
# - With leaky mode: old messages are dropped, subscriber always gets recent data
#
# This is useful for real-time applications where you want the latest data
# rather than processing a growing backlog of stale messages.

import asyncio
import typing

from dataclasses import dataclass, field

import ezmsg.core as ez


@dataclass
class TimestampedMessage:
    """A message with sequence number and timestamp for tracking latency."""

    seq: int
    created_at: float = field(default_factory=lambda: asyncio.get_event_loop().time())


class FastPublisherSettings(ez.Settings):
    num_messages: int = 20
    publish_interval_sec: float = 0.1  # 10 Hz


class FastPublisher(ez.Unit):
    """Publishes messages at ~10 Hz."""

    SETTINGS = FastPublisherSettings

    OUTPUT = ez.OutputStream(TimestampedMessage, num_buffers=32)

    @ez.publisher(OUTPUT)
    async def publish(self) -> typing.AsyncGenerator:

        for seq in range(self.SETTINGS.num_messages):
            msg = TimestampedMessage(seq=seq)
            print(f"[Publisher] Sending seq={seq}", flush=True)
            yield (self.OUTPUT, msg)
            await asyncio.sleep(self.SETTINGS.publish_interval_sec)

        print("[Publisher] Done sending all messages", flush=True)
        raise ez.Complete


class SlowSubscriberSettings(ez.Settings):
    process_time_sec: float = 1.0  # Simulates slow processing at ~1 Hz
    expected_messages: int = 20


class SlowSubscriberState(ez.State):
    received_seqs: list
    received_count: int = 0
    total_latency: float = 0.0


class SlowSubscriber(ez.Unit):
    """
    A slow subscriber that takes 1 second to process each message.

    Uses a leaky InputStream to drop old messages when it can't keep up,
    ensuring it always processes relatively recent data.
    """

    SETTINGS = SlowSubscriberSettings
    STATE = SlowSubscriberState

    # Leaky input stream; oldest messages are dropped
    INPUT = ez.InputStream(TimestampedMessage, leaky=True)

    async def initialize(self) -> None:
        self.STATE.received_seqs = []

    @ez.subscriber(INPUT)
    async def on_message(self, msg: TimestampedMessage) -> None:
        now = asyncio.get_event_loop().time()
        latency_ms = (now - msg.created_at) * 1000

        self.STATE.received_count += 1
        self.STATE.total_latency += latency_ms
        self.STATE.received_seqs.append(msg.seq)

        print(
            f"[Subscriber] Processing seq={msg.seq:3d}, latency={latency_ms:6.0f}ms",
            flush=True,
        )

        # Simulate slow processing
        await asyncio.sleep(self.SETTINGS.process_time_sec)

        # Terminate after receiving the last message
        if msg.seq == self.SETTINGS.expected_messages - 1:
            raise ez.NormalTermination

    async def shutdown(self) -> None:
        dropped = self.SETTINGS.expected_messages - self.STATE.received_count
        avg_latency = (
            self.STATE.total_latency / self.STATE.received_count
            if self.STATE.received_count > 0
            else 0
        )

        print("\n" + "=" * 60, flush=True)
        print("LEAKY SUBSCRIBER SUMMARY", flush=True)
        print("=" * 60, flush=True)
        print(f"  Messages published:    {self.SETTINGS.expected_messages}", flush=True)
        print(f"  Messages received:     {self.STATE.received_count}", flush=True)
        print(f"  Messages dropped:      {dropped}", flush=True)
        print(f"  Sequences received:    {self.STATE.received_seqs}", flush=True)
        print(f"  Average latency:       {avg_latency:.0f}ms", flush=True)
        print("=" * 60, flush=True)
        print(
            "\nNote: With leaky=True, the subscriber drops old messages to stay\n"
            "      current. Without it, backpressure would slow the publisher.",
            flush=True,
        )


class LeakyDemo(ez.Collection):
    """Demo system with a fast publisher and slow leaky subscriber."""

    SETTINGS = FastPublisherSettings

    PUB = FastPublisher()
    SUB = SlowSubscriber()

    def configure(self) -> None:
        num_msgs = self.SETTINGS.num_messages
        self.PUB.apply_settings(
            FastPublisherSettings(
                num_messages=num_msgs,
                publish_interval_sec=self.SETTINGS.publish_interval_sec,
            )
        )
        self.SUB.apply_settings(
            SlowSubscriberSettings(process_time_sec=1.0, expected_messages=num_msgs)
        )

    def network(self) -> ez.NetworkDefinition:
        return ((self.PUB.OUTPUT, self.SUB.INPUT),)


if __name__ == "__main__":
    print("Leaky Subscriber Demo", flush=True)
    print("=" * 60, flush=True)
    print("Publisher:  20 messages at 10 Hz (100ms intervals)", flush=True)
    print("Subscriber: Processes at 1 Hz (1000ms per message)", flush=True)
    print("Queue:      max_queue=3, leaky=True", flush=True)
    print("=" * 60, flush=True)
    print("\nExpected behavior:", flush=True)
    print("- Publisher sends 20 messages over ~2 seconds", flush=True)
    print("- Subscriber can only process ~1 message per second", flush=True)
    print("- Many messages will be dropped to keep subscriber current", flush=True)
    print("=" * 60 + "\n", flush=True)

    settings = FastPublisherSettings(num_messages=20, publish_interval_sec=0.1)
    system = LeakyDemo(settings)
    ez.run(DEMO=system)
