"""ATLAS orchestration worker — hosts workflows + activities on ``Settings.temporal_task_queue``.

Run with ``atlas-worker`` (console script) or ``python -m atlas.worker.main``. Shuts down
gracefully on SIGTERM/SIGINT: in-flight activities finish (or heartbeat their cancellation),
workflow tasks are handed back to Temporal and resumed by another worker — durable execution.
"""

from __future__ import annotations

import asyncio
import signal
import sys

from prometheus_client import start_http_server
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from atlas.activities import ALL_ACTIVITIES
from atlas.core.config import Settings, get_settings
from atlas.observability.logging import configure_logging, get_logger
from atlas.observability.metrics import get_registry
from atlas.observability.tracing import configure_tracing, temporal_interceptors
from atlas.workflows import ALL_WORKFLOWS

WORKER_METRICS_PORT = 9090
MAX_CONCURRENT_ACTIVITIES = 20


async def build_worker(client: Client, settings: Settings) -> Worker:
    return Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        max_concurrent_activities=MAX_CONCURRENT_ACTIVITIES,
        interceptors=temporal_interceptors(),
    )


async def main(settings: Settings | None = None, *, metrics_port: int | None = WORKER_METRICS_PORT) -> None:
    settings = settings or get_settings()
    configure_logging(settings)
    configure_tracing(settings)
    log = get_logger(component="worker")

    if settings.metrics_enabled and metrics_port:
        try:
            start_http_server(metrics_port, registry=get_registry())
            log.info("metrics_server_started", port=metrics_port)
        except OSError as exc:  # port in use (e.g. second local worker) — not fatal
            log.warning("metrics_server_unavailable", port=metrics_port, error=str(exc))

    client = await Client.connect(
        settings.temporal_host,
        namespace=settings.temporal_namespace,
        data_converter=pydantic_data_converter,
        tls=settings.temporal_tls,
    )
    worker = await build_worker(client, settings)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _request_stop(sig: signal.Signals) -> None:
        log.info("shutdown_signal", signal=sig.name)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop, sig)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows / non-main thread
            signal.signal(sig, lambda s, _f: _request_stop(signal.Signals(s)))

    log.info(
        "worker_started",
        task_queue=settings.temporal_task_queue,
        temporal_host=settings.temporal_host,
        namespace=settings.temporal_namespace,
        workflows=[w.__name__ for w in ALL_WORKFLOWS],
        activities=len(ALL_ACTIVITIES),
    )
    async with worker:
        await stop.wait()
    log.info("worker_stopped")


def run() -> None:
    """Console-script entry point."""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover
        sys.exit(130)


if __name__ == "__main__":  # pragma: no cover
    run()
