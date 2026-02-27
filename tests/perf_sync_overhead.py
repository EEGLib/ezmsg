import argparse
import asyncio
import statistics
import time

import ezmsg.core as ez


TOPIC = "/PERF_SYNC_OVERHEAD"


def _format_result(label: str, seconds: float, count: int) -> str:
    per_msg_us = (seconds / count) * 1e6
    rate = count / seconds if seconds > 0 else float("inf")
    return f"{label}: {seconds:.4f}s total, {per_msg_us:.2f} us/msg, {rate:,.0f} msg/s"


async def _async_roundtrip(
    count: int,
    warmup: int,
    host: str,
    port: int,
    force_tcp: bool,
) -> float:
    async with ez.GraphContext((host, port), auto_start=True) as ctx:
        pub = await ctx.publisher(TOPIC, host=host, force_tcp=force_tcp)
        sub = await ctx.subscriber(TOPIC)

        for i in range(warmup):
            await pub.broadcast(i)
            await sub.recv()

        start = time.perf_counter()
        for i in range(count):
            await pub.broadcast(i)
            await sub.recv()
        return time.perf_counter() - start


def _sync_roundtrip(
    count: int,
    warmup: int,
    host: str,
    port: int,
    force_tcp: bool,
) -> float:
    with ez.sync.init((host, port), auto_start=True) as ctx:
        pub = ctx.create_publisher(TOPIC, host=host, force_tcp=force_tcp)
        sub = ctx.create_subscription(TOPIC)

        for i in range(warmup):
            pub.publish(i)
            sub.recv()

        start = time.perf_counter()
        for i in range(count):
            pub.publish(i)
            sub.recv()
        return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure sync wrapper overhead vs async API."
    )
    parser.add_argument("--count", type=int, default=10_000, help="messages per run")
    parser.add_argument("--warmup", type=int, default=1_000, help="warmup messages")
    parser.add_argument("--repeats", type=int, default=3, help="number of repeats")
    parser.add_argument("--host", default="127.0.0.1", help="graphserver host")
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="graphserver port (0 uses ephemeral port)",
    )
    parser.add_argument(
        "--force-tcp",
        action="store_true",
        help="force TCP transport (disable SHM)",
    )
    args = parser.parse_args()

    async_times = []
    sync_times = []

    for _ in range(args.repeats):
        async_times.append(
            asyncio.run(
                _async_roundtrip(
                    args.count,
                    args.warmup,
                    args.host,
                    args.port,
                    args.force_tcp,
                )
            )
        )
        sync_times.append(
            _sync_roundtrip(
                args.count,
                args.warmup,
                args.host,
                args.port,
                args.force_tcp,
            )
        )

    async_med = statistics.median(async_times)
    sync_med = statistics.median(sync_times)

    print(_format_result("async", async_med, args.count))
    print(_format_result("sync ", sync_med, args.count))
    if async_med > 0:
        overhead = (sync_med / async_med - 1.0) * 100.0
        print(f"overhead: {overhead:.1f}%")


if __name__ == "__main__":
    main()
