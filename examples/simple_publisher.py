import time

import ezmsg.core as ez

TOPIC = "/TEST"

def main(host: str = "127.0.0.1", port: int = 12345) -> None:
    with ez.init((host, port), auto_start=True) as ctx:
        pub = ctx.create_publisher(TOPIC, force_tcp=True)

        print("Publisher Task Launched")
        count = 0
        try:
            while True:
                output = f"{count=}"
                pub.publish(output)
                print(output)
                time.sleep(0.1)
                count += 1
        except KeyboardInterrupt:
            pass
        print("Publisher Task Concluded")

    print("Done")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1", help="hostname for graphserver")
    parser.add_argument("--port", default=12345, type=int, help="port for graphserver")
    args = parser.parse_args()

    main(host=args.host, port=args.port)
