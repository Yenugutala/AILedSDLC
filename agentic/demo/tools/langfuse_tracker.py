from __future__ import annotations
"""
demo/tools/langfuse_tracker.py

Thin Langfuse wrapper for AI-DLC demo observability.

Gracefully does nothing if:
  • LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY are not set in .env
  • langfuse package is not installed

Usage:
  tracker = LangfuseTracker(public_key, secret_key, host)
  tracker.start_trace(session_id, ticket_key)
  # ... demo runs ...
  url = tracker.flush_and_get_url()   # called in Beat 6
"""


class LangfuseTracker:
    """Records each demo beat as a Langfuse generation/span and returns a shareable trace URL."""

    def __init__(self, public_key: str = "", secret_key: str = "", host: str = ""):
        self._enabled = False
        self._lf = None
        self._trace = None

        if not public_key or not secret_key:
            return
        try:
            from langfuse import Langfuse  # type: ignore
            self._lf = Langfuse(
                public_key=public_key,
                secret_key=secret_key,
                host=host or "https://cloud.langfuse.com",
            )
            self._enabled = True
        except ImportError:
            pass  # langfuse not installed — silently disabled

    @property
    def enabled(self) -> bool:
        return self._enabled

    def start_trace(self, session_id: str, ticket_key: str) -> None:
        """Start a Langfuse trace for this demo session. Call after Beat 1."""
        if not self._enabled:
            return
        self._trace = self._lf.trace(
            name="sml-demo",
            session_id=session_id,
            tags=["demo", ticket_key],
            metadata={"ticket": ticket_key},
        )

    def record_beat(
        self,
        name: str,
        model: str | None,
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        status: str,
    ) -> None:
        """Record one beat as a Langfuse generation (LLM) or span (non-LLM)."""
        if not self._enabled or self._trace is None:
            return
        try:
            if model:
                self._trace.generation(
                    name=name,
                    model=model,
                    usage={
                        "input": input_tokens,
                        "output": output_tokens,
                        "total": input_tokens + output_tokens,
                    },
                    metadata={"latency_ms": latency_ms, "status": status},
                )
            else:
                self._trace.span(
                    name=name,
                    metadata={"latency_ms": latency_ms, "status": status},
                )
        except Exception:
            pass  # never crash the demo due to Langfuse

    def flush_and_get_url(self) -> str | None:
        """Flush all buffered events and return the shareable trace URL."""
        if not self._enabled or self._trace is None:
            return None
        try:
            self._lf.flush()
            return self._trace.get_trace_url()
        except Exception:
            return None
