from __future__ import annotations

import asyncio
from collections import deque
from typing import Final

import pytest
from starlette.types import Message
from transport_scenarios import Scenario, ScenarioResponse, Timing, Trace


class Connection:
    def __init__(self) -> None:
        self.sent: Final[deque[Message]] = deque()
        self.inbound: Final[asyncio.Queue[Message]] = asyncio.Queue()
        self.headers: Final = asyncio.Event()

    async def send(self, message: Message) -> None:
        self.sent.append(message)
        if message["type"] == "http.response.start":
            self.headers.set()

    async def receive(self) -> Message:
        return await self.inbound.get()

    def body(self) -> bytes:
        return b"".join(message.get("body", b"") for message in self.sent)

    async def close(self) -> None:
        await self.inbound.put({"type": "http.disconnect"})


def response(case: Scenario, timing: Timing) -> tuple[Trace, ScenarioResponse]:
    trace: Final = Trace(case, "transport-test", timing)
    return trace, ScenarioResponse(trace, "synthetic-anthropic-model")


@pytest.mark.asyncio
async def test_headers_are_withheld_until_the_configured_delay() -> None:
    connection: Final = Connection()
    trace, app = response("headers-delay", Timing(delay_seconds=0.15))
    task: Final = asyncio.create_task(app({}, connection.receive, connection.send))
    try:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(connection.headers.wait(), timeout=0.03)
        await asyncio.wait_for(task, timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert trace.end_reason == "completed"
    assert b"event: message_stop" in connection.body()
    assert next(event.elapsed_ms for event in trace.events if event.kind == "headers_write_started") >= 140


@pytest.mark.asyncio
async def test_heartbeats_precede_the_delayed_text_and_normal_terminal_event() -> None:
    connection: Final = Connection()
    trace, app = response("heartbeat", Timing(delay_seconds=0.21, heartbeat_seconds=0.05))
    await asyncio.wait_for(app({}, connection.receive, connection.send), timeout=1)
    body: Final = connection.body()
    pings: Final = tuple(event for event in trace.events if event.kind == "ping_write_returned")
    text: Final = next(event for event in trace.events if event.kind == "content_block_delta_write_returned")
    assert len(pings) == 4
    assert all(event.elapsed_ms < text.elapsed_ms for event in pings)
    assert text.elapsed_ms >= 200
    assert body.index(b"event: message_start") < body.index(b"event: ping") < body.index(b"event: content_block_delta")
    assert body.endswith(b'event: message_stop\ndata: {"type":"message_stop"}\n\n')
    assert trace.end_reason == "completed"


@pytest.mark.asyncio
async def test_injected_disconnect_omits_both_protocol_and_http_completion() -> None:
    connection: Final = Connection()
    trace, app = response("disconnect", Timing(disconnect_seconds=0.01))
    await asyncio.wait_for(app({}, connection.receive, connection.send), timeout=1)
    assert b"event: content_block_delta" in connection.body()
    assert b"event: message_stop" not in connection.body()
    assert connection.sent[-1]["more_body"] is True
    assert trace.end_reason == "injected_disconnect"


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("headers-delay", "client-cancel"))
async def test_peer_cancellation_stops_pending_generation(case: Scenario) -> None:
    connection: Final = Connection()
    trace, app = response(case, Timing(delay_seconds=0.2, heartbeat_seconds=0.05))
    task: Final = asyncio.create_task(app({}, connection.receive, connection.send))
    if case == "client-cancel":
        await asyncio.wait_for(connection.headers.wait(), timeout=1)
    await connection.close()
    await asyncio.wait_for(task, timeout=0.1)
    count: Final = len(connection.sent)
    await asyncio.sleep(0.25)
    assert len(connection.sent) == count
    assert trace.end_reason == "peer_disconnected"
    assert b"event: content_block_delta" not in connection.body()
    assert b"event: message_stop" not in connection.body()
    if case == "headers-delay":
        assert not connection.headers.is_set()
