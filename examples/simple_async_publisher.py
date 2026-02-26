import asyncio

import ezmsg.core as ez

TOPIC = "/TEST"


async def main(host: str = "127.0.0.1", port: int = 12345) -> None:
    async with ez.GraphContext((host, port), auto_start=True) as ctx:
        pub = await ctx.publisher(TOPIC)
        try:
            print("Publisher Task Launched")
            count = 0
            while True:
                await pub.broadcast(f"{count=}")
                await asyncio.sleep(0.1)
                count += 1
        except asyncio.CancelledError:
            pass
        finally:
            print("Publisher Task Concluded")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="hostname for graphserver")
    parser.add_argument("--port", default=12345, type=int, help="port for graphserver")

    args = parser.parse_args()

    asyncio.run(main(host=args.host, port=args.port))
