import time
import ezmsg.core as ez

TOPIC = "/TEST"


def main(host: str = "127.0.0.1", port: int = 12345) -> None:
    with ez.init((host, port), auto_start=True) as ctx:
        print("Subscriber Task Launched")

        def on_message(msg: str) -> None:
            # Uncomment if you want to witness backpressure!
            time.sleep(1.0)
            print(msg)

        ctx.create_subscription(TOPIC, callback=on_message)
        ez.spin(ctx)

    print("Subscriber Task Concluded")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="hostname for graphserver")
    parser.add_argument("--port", default=12345, type=int, help="port for graphserver")
    args = parser.parse_args()

    main(host=args.host, port=args.port)
