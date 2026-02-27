import asyncio

import ezmsg.core as ez

TOPIC = "/TEST"


async def main(host: str = "127.0.0.1", port: int = 12345) -> None:
    async with ez.GraphContext((host, port), auto_start=True) as ctx:
        sub = await ctx.subscriber(TOPIC)
        try:
            print("Subscriber Task Launched")
            while True:
                async with sub.recv_zero_copy() as msg:
                    # Uncomment if you want to witness backpressure!
                    # await asyncio.sleep(1.0)
                    print(msg)
        except asyncio.CancelledError:
            pass
        finally:
            print("Subscriber Task Concluded")
            print("Detached")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="hostname for graphserver")
    parser.add_argument("--port", default=12345, type=int, help="port for graphserver")

    args = parser.parse_args()

    asyncio.run(main(host=args.host, port=args.port))
