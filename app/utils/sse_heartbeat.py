"""Keep server-sent event streams alive during long computations.

CloudFront and the load balancer close a connection that sends no bytes for
60 seconds. Long steps (burden calculation, risk analysis) can run longer
than that without producing an event, so the browser never receives the
result. ``with_heartbeat`` runs the event generator in a worker thread and
yields ``None`` whenever no event has arrived for ``interval`` seconds; the
caller sends an SSE comment line in its place, which clients ignore.
"""

from __future__ import annotations

import queue
import threading
from typing import Callable, Iterable, Iterator, Optional, TypeVar

from flask import copy_current_request_context, has_request_context

T = TypeVar("T")

HEARTBEAT_INTERVAL_SECONDS = 15
SSE_HEARTBEAT = ": keepalive\n\n"

_EVENT, _ERROR, _DONE = "event", "error", "done"


def with_heartbeat(
    make_events: Callable[[], Iterable[T]],
    interval: Optional[float] = None,
) -> Iterator[Optional[T]]:
    """Yield events from ``make_events()``, or ``None`` after ``interval`` seconds of silence.

    The worker thread gets a copy of the current request context (same Flask
    session), so code that reads or writes the session behaves as before.
    Exceptions raised by the generator are re-raised in the caller.
    """
    wait = HEARTBEAT_INTERVAL_SECONDS if interval is None else interval
    items: "queue.Queue[tuple[str, object]]" = queue.Queue()

    def produce() -> None:
        try:
            for event in make_events():
                items.put((_EVENT, event))
        except BaseException as exc:  # re-raised in the consumer
            items.put((_ERROR, exc))
        finally:
            items.put((_DONE, None))

    target = copy_current_request_context(produce) if has_request_context() else produce
    threading.Thread(target=target, name="sse-producer", daemon=True).start()

    while True:
        try:
            kind, value = items.get(timeout=wait)
        except queue.Empty:
            yield None
            continue
        if kind == _EVENT:
            yield value  # type: ignore[misc]
        elif kind == _ERROR:
            raise value  # type: ignore[misc]
        else:
            return
