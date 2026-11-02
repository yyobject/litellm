from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Annotated, Final, Literal

from fastapi import FastAPI, Path
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field
from reply_anthropic import messages_events
from reply_common import plan_reply, sse
from starlette.types import Receive, Scope, Send

Scenario = Literal["headers-delay", "heartbeat", "disconnect", "client-cancel"]
RunId = Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,64}$")]
LOGGER: Final = logging.getLogger("mock_transport")


class Timing(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    delay_seconds: float = Field(default=300, ge=0, le=600)
    heartbeat_seconds: float = Field(default=30, ge=0.05, le=60)
    disconnect_seconds: float = Field(default=2, ge=0, le=60)


class MessagesRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: str = Field(max_length=256)
    stream: Literal[True]


class Event(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: str
    elapsed_ms: float
    size: int = 0


class TraceSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    request_id: str
    message_id: str
    run_id: str
    scenario: Scenario
    started_at: str
    timing: Timing
    end_reason: str | None
    event_count: int
    events: tuple[Event, ...]


class Trace:
    def __init__(self, scenario: Scenario, run_id: str, timing: Timing) -> None:
        self.request_id: Final = "mock-" + uuid.uuid4().hex
        self.message_id: Final = "msg_" + uuid.uuid4().hex
        self.run_id: Final = run_id
        self.scenario: Final = scenario
        self.timing: Final = timing
        self.started_at: Final = datetime.now(timezone.utc).isoformat()
        self.started: Final = time.monotonic()
        self.events: Final[deque[Event]] = deque(maxlen=64)
        self.event_count = 0
        self.end_reason: str | None = None
        self.record("request_received")

    def record(self, kind: str, size: int = 0) -> None:
        self.event_count += 1
        self.events.append(Event(kind=kind, elapsed_ms=round((time.monotonic() - self.started) * 1000, 3), size=size))

    def finish(self, reason: str) -> None:
        if self.end_reason is None:
            self.end_reason = reason
            self.record(reason)

    def snapshot(self) -> TraceSnapshot:
        return TraceSnapshot(
            request_id=self.request_id,
            message_id=self.message_id,
            run_id=self.run_id,
            scenario=self.scenario,
            started_at=self.started_at,
            timing=self.timing,
            end_reason=self.end_reason,
            event_count=self.event_count,
            events=tuple(self.events),
        )


class ScenarioStore:
    def __init__(self) -> None:
        self.timing = Timing()
        self.traces: Final[deque[Trace]] = deque(maxlen=256)

    def begin(self, scenario: Scenario, run_id: str) -> Trace:
        trace: Final = Trace(scenario, run_id, self.timing)
        self.traces.append(trace)
        return trace


class ScenarioResponse(Response):
    def __init__(self, trace: Trace, model: str) -> None:
        super().__init__(media_type="text/event-stream")
        self.trace: Final = trace
        self.model: Final = model
        self.raw_headers = [
            (b"content-type", b"text/event-stream"),
            (b"cache-control", b"no-cache, no-store"),
            (b"x-accel-buffering", b"no"),
            (b"request-id", trace.request_id.encode()),
            (b"x-request-id", trace.request_id.encode()),
        ]

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        emitter: Final = asyncio.create_task(self.emit(send))
        disconnect: Final = asyncio.create_task(self.watch_disconnect(receive))
        try:
            done, pending = await asyncio.wait((emitter, disconnect), return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
        except asyncio.CancelledError:
            self.trace.finish("handler_cancelled")
            raise
        except OSError:
            self.trace.finish("downstream_write_failed")
        finally:
            emitter.cancel()
            disconnect.cancel()
            await asyncio.gather(emitter, disconnect, return_exceptions=True)
            LOGGER.info(json.dumps({"event": "mock_transport_summary", **self.trace.snapshot().model_dump()}))

    async def watch_disconnect(self, receive: Receive) -> None:
        while True:
            if (await receive())["type"] == "http.disconnect":
                self.trace.finish("peer_disconnected")
                return

    async def write(self, send: Send, event: dict[str, object]) -> None:
        kind: Final = str(event["type"])
        body: Final = sse(event, kind)
        self.trace.record(kind + "_write_started", len(body))
        await send({"type": "http.response.body", "body": body, "more_body": True})
        self.trace.record(kind + "_write_returned", len(body))

    async def wait_until(self, elapsed: float) -> None:
        await asyncio.sleep(max(0, self.trace.started + elapsed - time.monotonic()))

    async def emit(self, send: Send) -> None:
        timing: Final = self.trace.timing
        reply: Final = plan_reply(
            provider="anthropic",
            seed=self.trace.request_id,
            model=self.model,
            prompt_tokens=8,
            max_tokens=64,
            tool_names=(),
        )
        events: Final = messages_events(reply)
        opening: Final[dict[str, object]] = {
            "type": "message_start",
            "message": {
                "id": self.trace.message_id,
                "type": "message",
                "role": "assistant",
                "model": self.model,
                "content": [],
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": 8, "output_tokens": 0},
            },
        }
        if self.trace.scenario == "headers-delay":
            await self.wait_until(timing.delay_seconds)
        self.trace.record("headers_write_started")
        await send({"type": "http.response.start", "status": 200, "headers": self.raw_headers})
        self.trace.record("headers_write_returned")
        await self.write(send, opening)
        if self.trace.scenario in ("heartbeat", "client-cancel"):
            for target in (
                number * timing.heartbeat_seconds
                for number in range(1, int(timing.delay_seconds / timing.heartbeat_seconds) + 1)
            ):
                if target >= timing.delay_seconds:
                    break
                await self.wait_until(target)
                await self.write(send, {"type": "ping"})
            await self.wait_until(timing.delay_seconds)
        for event in events[2:]:
            await self.write(send, event)
            if self.trace.scenario == "disconnect" and event["type"] == "content_block_delta":
                await asyncio.sleep(timing.disconnect_seconds)
                self.trace.finish("injected_disconnect")
                return
        await send({"type": "http.response.body", "body": b"", "more_body": False})
        self.trace.finish("completed")


def mount_transport_scenarios(app: FastAPI) -> None:
    store: Final = ScenarioStore()
    if not LOGGER.handlers:
        LOGGER.addHandler(logging.StreamHandler())
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False

    async def configure(timing: Timing) -> Timing:
        store.timing = timing
        return store.timing

    async def traces(run_id: str | None = None) -> tuple[TraceSnapshot, ...]:
        return tuple(trace.snapshot() for trace in store.traces if run_id is None or trace.run_id == run_id)

    async def messages(scenario: Scenario, run_id: RunId, body: MessagesRequest) -> Response:
        return ScenarioResponse(store.begin(scenario, run_id), body.model)

    app.add_api_route("/mock/scenarios/timing", configure, methods=["POST"])
    app.add_api_route("/mock/scenarios/traces", traces, methods=["GET"])
    app.add_api_route("/scenarios/{scenario}/{run_id}/v1/messages", messages, methods=["POST"])
