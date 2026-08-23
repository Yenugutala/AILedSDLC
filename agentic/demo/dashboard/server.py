from __future__ import annotations
"""
demo/dashboard/server.py
FastAPI + Server-Sent Events dashboard.
Starts in a background thread, auto-opens the browser, streams
KPI events to the live HTML page as the demo beats run.
"""

import asyncio
import json
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

_STATIC_DIR = Path(__file__).parent / "static"
_PORT = 8765

app = FastAPI(docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

# In-memory event queue — one queue per connected SSE client
_subscribers: list[asyncio.Queue] = []
_loop: asyncio.AbstractEventLoop | None = None
_all_events: list[dict] = []  # replay buffer for late-joining browsers


@app.get("/", response_class=HTMLResponse)
async def index():
    return (_STATIC_DIR / "index.html").read_text()


@app.get("/events")
async def sse_stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    _subscribers.append(queue)

    # Replay missed events so a newly-opened tab sees full history
    for event in _all_events:
        await queue.put(event)

    async def generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"event: {event['type']}\ndata: {json.dumps(event['payload'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            _subscribers.remove(queue)

    return StreamingResponse(generator(), media_type="text/event-stream")


def emit(event_type: str, payload: Any):
    """Thread-safe: push an event to all connected browsers."""
    event = {"type": event_type, "payload": payload}
    _all_events.append(event)

    if _loop and not _loop.is_closed():
        for q in list(_subscribers):
            asyncio.run_coroutine_threadsafe(q.put(event), _loop)


def start(open_browser: bool = True):
    """Launch dashboard server in a background daemon thread."""
    global _loop

    ready = threading.Event()

    def _run():
        global _loop
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)

        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=_PORT,
            loop="none",
            log_level="error",
        )
        server = uvicorn.Server(config)

        async def _serve():
            await server.serve()

        ready.set()
        _loop.run_until_complete(_serve())

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    ready.wait(timeout=5)
    time.sleep(0.5)  # let uvicorn bind the port

    if open_browser:
        webbrowser.open(f"http://localhost:{_PORT}")

    return f"http://localhost:{_PORT}"
