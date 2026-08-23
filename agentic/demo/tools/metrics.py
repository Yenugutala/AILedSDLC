from __future__ import annotations
"""
demo/tools/metrics.py
Zero-dependency in-process metrics tracker. No Langfuse, no external service.
All KPIs captured here and streamed to the live HTML dashboard via SSE.
"""

import time
from dataclasses import dataclass, field, asdict
from typing import Optional


# Pricing for claude-sonnet-4-6 (per token)
_INPUT_COST_PER_TOKEN = 3.00 / 1_000_000   # $3 / 1M input tokens
_OUTPUT_COST_PER_TOKEN = 15.00 / 1_000_000  # $15 / 1M output tokens


@dataclass
class BeatRecord:
    beat_id: str
    name: str
    model: Optional[str]        # None for non-LLM steps
    input_tokens: int
    output_tokens: int
    cost_usd: float
    latency_ms: int
    status: str                 # "pass" | "fail" | "fixed" | "skipped"
    jira_artifact: Optional[str] = None  # e.g. "comment:10234"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DemoSummary:
    total_cost_usd: float
    total_input_tokens: int
    total_output_tokens: int
    total_latency_ms: int
    total_beats: int
    jira_artifacts_written: int
    knowledge_queries: int
    chunks_indexed: int

    def to_dict(self) -> dict:
        return asdict(self)


class MetricsTracker:
    """Collects per-beat metrics and computes KPI summaries."""

    def __init__(self) -> None:
        self._records: list[BeatRecord] = []
        self._knowledge_queries: int = 0
        self._chunks_indexed: int = 0
        self._emit_fn = None  # set by dashboard when ready

    def set_emit(self, fn):
        """Wire up the dashboard SSE emitter."""
        self._emit_fn = fn

    def set_chunks_indexed(self, n: int):
        self._chunks_indexed = n

    def increment_knowledge_queries(self):
        self._knowledge_queries += 1

    def record(
        self,
        beat_id: str,
        name: str,
        model: Optional[str],
        input_tokens: int,
        output_tokens: int,
        latency_ms: int,
        status: str,
        jira_artifact: Optional[str] = None,
    ) -> BeatRecord:
        cost = (input_tokens * _INPUT_COST_PER_TOKEN) + (output_tokens * _OUTPUT_COST_PER_TOKEN)
        rec = BeatRecord(
            beat_id=beat_id,
            name=name,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=round(cost, 6),
            latency_ms=latency_ms,
            status=status,
            jira_artifact=jira_artifact,
        )
        self._records.append(rec)
        if self._emit_fn:
            self._emit_fn("beat_end", rec.to_dict())
        return rec

    def emit_log(self, beat_id: str, message: str, level: str = "info"):
        if self._emit_fn:
            self._emit_fn("log_entry", {
                "timestamp": time.strftime("%H:%M:%S"),
                "beat_id": beat_id,
                "message": message,
                "level": level,
            })

    def emit_check(self, beat_id: str, check_name: str, passed: bool, detail: str = ""):
        if self._emit_fn:
            self._emit_fn("check_result", {
                "beat_id": beat_id,
                "check_name": check_name,
                "passed": passed,
                "detail": detail,
            })

    def emit_jira_write(self, beat_id: str, write_type: str, issue_key: str, artifact_id: str = ""):
        if self._emit_fn:
            self._emit_fn("jira_write", {
                "beat_id": beat_id,
                "type": write_type,
                "issue_key": issue_key,
                "artifact_id": artifact_id,
            })

    def get_all(self) -> list[BeatRecord]:
        return list(self._records)

    def get_summary(self) -> DemoSummary:
        jira_count = sum(1 for r in self._records if r.jira_artifact)
        return DemoSummary(
            total_cost_usd=round(sum(r.cost_usd for r in self._records), 4),
            total_input_tokens=sum(r.input_tokens for r in self._records),
            total_output_tokens=sum(r.output_tokens for r in self._records),
            total_latency_ms=sum(r.latency_ms for r in self._records),
            total_beats=len(self._records),
            jira_artifacts_written=jira_count,
            knowledge_queries=self._knowledge_queries,
            chunks_indexed=self._chunks_indexed,
        )

    def emit_summary(self):
        if self._emit_fn:
            self._emit_fn("summary", self.get_summary().to_dict())


class Timer:
    """Context manager to measure elapsed ms."""
    def __init__(self):
        self.elapsed_ms: int = 0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed_ms = int((time.perf_counter() - self._start) * 1000)
