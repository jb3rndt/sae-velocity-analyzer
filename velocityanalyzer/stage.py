import asyncio
import json
import logging
import signal
import threading
import time
from queue import Queue

from prometheus_client import Counter, Histogram, start_http_server
from visionlib.pipeline.consumer import RedisConsumer
from visionlib.pipeline.publisher import RedisPublisher
from websockets.server import serve

from .analyzer import Analyzer
from .config import AnalyzerConfig

logger = logging.getLogger(__name__)

REDIS_PUBLISH_DURATION = Histogram(
    "velocity_analyzer_redis_publish_duration",
    "The time it takes to push a message onto the Redis stream",
    buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25),
)
FRAME_COUNTER = Counter(
    "velocity_analyzer_frame_counter",
    "How many frames have been consumed from the Redis input stream",
)

updates = Queue()


def run_stage():

    stop_event = threading.Event()

    # Register signal handlers
    def sig_handler(signum, _):
        signame = signal.Signals(signum).name
        print(f"Caught signal {signame} ({signum}). Exiting...")
        stop_event.set()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    # Load config from settings.yaml / env vars
    CONFIG = AnalyzerConfig()

    logger.setLevel(CONFIG.log_level.value)

    logger.info(
        f"Starting prometheus metrics endpoint on port {CONFIG.prometheus_port}"
    )

    start_http_server(CONFIG.prometheus_port)

    logger.info(
        f"Starting velocity analyzer stage. Config: {CONFIG.model_dump_json(indent=2)}"
    )

    analyzer = Analyzer(CONFIG, lambda update: updates.put(update))

    consume = RedisConsumer(
        CONFIG.redis.host,
        CONFIG.redis.port,
        stream_keys=[f"{CONFIG.redis.input_stream_prefix}:{CONFIG.redis.stream_id}"],
    )
    publish = RedisPublisher(CONFIG.redis.host, CONFIG.redis.port)

    websocket_thread = threading.Thread(target=start_server)
    websocket_thread.daemon = True
    websocket_thread.start()

    with consume, publish:
        for stream_key, proto_data in consume():
            if stop_event.is_set():
                break

            if stream_key is None:
                continue

            stream_id = stream_key.split(":")[1]

            FRAME_COUNTER.inc()

            output_proto_data = analyzer.get(proto_data)

            if output_proto_data is None:
                continue

            with REDIS_PUBLISH_DURATION.time():
                publish(
                    f"{CONFIG.redis.output_stream_prefix}:{stream_id}",
                    output_proto_data,
                )


async def update_clients(websocket):
    while True:
        if updates:
            update = updates.get()
            print(f"Sending update: {update}")
            await websocket.send(json.dumps(update))
        time.sleep(0.2)


async def main():
    async with serve(update_clients, "localhost", 8765):
        await asyncio.Future()  # run forever


# Function to start the WebSocket server
def start_server():
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
