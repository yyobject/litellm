# Anthropic transport scenarios

Start the existing mock with Python 3.12 and FastAPI, Pydantic 2 and Uvicorn installed

```sh
python tests/e2e/perf/setup/mocks/server.py anthropic --host 127.0.0.1 --port 8090
```

The existing `/v1/messages` and `/mock/latency` behavior is unchanged. Transport scenarios use a separate path, which can be set as an Anthropic deployment's `api_base`. No caller headers need to survive an intermediary

```yaml
model_list:
  - model_name: mock-anthropic-heartbeat
    litellm_params:
      model: anthropic/claude-sonnet-5-5
      api_base: http://127.0.0.1:8090/scenarios/heartbeat/example-run
      api_key: synthetic-mock-key
      timeout: 600
      stream_timeout: 600
```

Each path ends in `/scenarios/{scenario}/{run_id}/v1/messages`. Use a distinct run ID when comparing paths. Valid scenarios are `headers-delay`, `heartbeat`, `disconnect` and `client-cancel`. Requests must set `stream: true`

`headers-delay` waits 300 seconds before sending any HTTP response headers or body. `heartbeat` sends headers and `message_start` immediately, sends native Anthropic `ping` events every 30 seconds, then sends text and `message_stop` at 300 seconds. No heartbeat is generated at the same deadline as the text

`disconnect` sends headers, `message_start` and the first text delta, then ends the ASGI response without the final HTTP body marker after two seconds. Uvicorn closes the incomplete HTTP response and may log one expected `ASGI callable returned without completing response` error. The stream has no `message_stop`. This injects a premature HTTP close, not a guaranteed TCP RST

`client-cancel` behaves like `heartbeat` until the caller closes the connection. Test cancellation before headers with `headers-delay` and cancellation while streaming with `client-cancel`. The mock listens for its immediate peer's disconnect in both phases and cancels pending generation. A completed mock request after the original caller cancelled is evidence that cancellation did not reach the mock in time, not proof of which intermediary retained it

Set short timings for a smoke test, then restore the full durations before integration testing. Timing updates apply to future requests only

```sh
curl -sS http://127.0.0.1:8090/mock/scenarios/timing \
  -H 'Content-Type: application/json' \
  -d '{"delay_seconds":300,"heartbeat_seconds":30,"disconnect_seconds":2}'

curl -N --max-time 380 http://127.0.0.1:8090/scenarios/heartbeat/example-run/v1/messages \
  -H 'Content-Type: application/json' \
  -d '{"model":"claude-sonnet-5-5","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"synthetic transport check"}]}'

curl -sS 'http://127.0.0.1:8090/mock/scenarios/traces?run_id=example-run'
```

Traces contain generated request/message IDs, UTC start time, monotonic event offsets, write sizes and the termination reason. They never contain request headers or prompt bodies. A write returning means the ASGI server accepted it, not that the final caller received it. Compare mock timestamps with client observations and gateway logs

Only the latest 256 requests and the latest 64 events per request remain in memory. `event_count` exposes truncation. One bounded summary is logged when a request ends. Traces are lost on restart, and each worker has independent state. Run a single mock worker and collect traces before restarting it

The control endpoints have no authentication. Keep this synthetic service on a test-only private network. Use zero prices and dedicated model-scoped credentials when registering it in a gateway. Disable response-cache reuse for probes or vary the synthetic input, and record retries as separate upstream attempts. Do not change global production timeouts or heartbeat settings to make a test pass
