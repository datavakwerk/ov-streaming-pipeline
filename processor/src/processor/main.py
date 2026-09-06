"""Processor service entrypoint."""

from __future__ import annotations

import signal
import threading

from confluent_kafka import Consumer

from common.config import env, env_float, env_int, postgres_dsn
from common.logging import setup_logging
from common.metrics import start_metrics_server
from processor.consumer import ProcessorLoop, consumer_config
from processor.dlq import DeadLetterPublisher
from processor.pipeline import Pipeline
from processor.sink import PostgresSink
from processor.windows import TumblingWindower

log = setup_logging("processor")


def main() -> None:
    start_metrics_server(env_int("METRICS_PORT", 9102))
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    sink = PostgresSink.connect(postgres_dsn())
    windower = TumblingWindower(
        window_s=env_int("WINDOW_SECONDS", 300),
        allowed_lateness_s=env_int("ALLOWED_LATENESS_SECONDS", 120),
    )
    newest = sink.newest_window_end()
    if newest is not None:
        windower.seed_watermark(newest)
        log.info("watermark seeded from postgres: %s", newest.isoformat())
    pipeline = Pipeline(windower, sink)
    loop = ProcessorLoop(
        Consumer(consumer_config(bootstrap, env("CONSUMER_GROUP", "processor"))),
        pipeline,
        DeadLetterPublisher.connect(bootstrap),
        idle_finalize_s=env_float("IDLE_FINALIZE_SECONDS", 90),
        state_flush_s=env_float("STATE_FLUSH_SECONDS", 5),
    )
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    loop.run(stop)
    sink.close()


if __name__ == "__main__":
    main()
