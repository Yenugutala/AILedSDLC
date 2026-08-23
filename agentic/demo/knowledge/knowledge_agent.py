from __future__ import annotations
"""
demo/knowledge/knowledge_agent.py
RAG-powered agent: answers questions and proposes code changes
by retrieving relevant chunks from ChromaDB and calling Claude.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import anthropic
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown

from demo.knowledge.retriever import Retriever, SearchResult
from demo.tools.metrics import MetricsTracker, Timer

console = Console()
_MODEL = "claude-sonnet-4-6"


@dataclass
class KnowledgeResponse:
    question: str
    answer: str
    sources: list[SearchResult]
    input_tokens: int
    output_tokens: int
    latency_ms: int


@dataclass
class FileChange:
    path: str
    original: str
    modified: str
    description: str


@dataclass
class ChangeProposal:
    instruction: str
    changes: list[FileChange]
    summary: str


class KnowledgeAgent:
    def __init__(self, retriever: Retriever, repo_root: Path, metrics: MetricsTracker):
        self._retriever = retriever
        self._repo_root = repo_root
        self._metrics = metrics
        self._claude = anthropic.Anthropic()

    def answer(self, question: str) -> KnowledgeResponse:
        """RAG pipeline: search → context → Claude → grounded answer."""
        console.print(f"\n[dim]  Searching knowledge base...[/]")
        chunks = self._retriever.search(question, n=6)
        context = self._retriever.format_context(chunks)

        console.print(f"[dim]  Retrieved {len(chunks)} relevant chunks[/]")

        system = (
            "You are an expert on the Securities Master Data Lakehouse codebase. "
            "Answer questions using ONLY the provided context. "
            "If the answer is not in the context, say so clearly. "
            "Always cite the source file when referencing specific content. "
            "Be concise and technical — the audience is expert data engineers."
        )
        user = f"Context from codebase:\n\n{context}\n\n---\n\nQuestion: {question}"

        with Timer() as t:
            resp = self._claude.messages.create(
                model=_MODEL,
                max_tokens=1024,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        answer = resp.content[0].text
        self._metrics.increment_knowledge_queries()
        self._metrics.record(
            beat_id="ask",
            name=f"Knowledge: {question[:40]}...",
            model=_MODEL,
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            latency_ms=t.elapsed_ms,
            status="pass",
        )
        return KnowledgeResponse(
            question=question,
            answer=answer,
            sources=chunks,
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            latency_ms=t.elapsed_ms,
        )

    def propose_change(self, instruction: str) -> Optional[ChangeProposal]:
        """Find affected files and generate diffs via Claude."""
        console.print(f"\n[dim]  Searching for affected files...[/]")
        chunks = self._retriever.search(instruction, n=10)

        # Gather unique file paths from results
        file_paths = list(dict.fromkeys(
            c.source for c in chunks
            if not c.source.startswith("jira:")
        ))
        if not file_paths:
            console.print("[yellow]  No relevant files found.[/]")
            return None

        console.print(f"[dim]  Found {len(file_paths)} potentially affected files[/]")

        # Read full content of each file
        file_contents = {}
        for rel_path in file_paths:
            full = self._repo_root / rel_path
            if full.exists():
                file_contents[rel_path] = full.read_text(errors="ignore")

        if not file_contents:
            return None

        # Build prompt for Claude
        files_block = "\n\n".join(
            f"=== FILE: {p} ===\n{content[:3000]}"
            for p, content in file_contents.items()
        )
        system = (
            "You are a senior data engineer. Apply the requested change across all relevant files. "
            "For each file that needs modification, output:\n\n"
            "### CHANGE: <relative/path/to/file>\n"
            "DESCRIPTION: <one line describing what changed>\n"
            "```\n<complete modified file content>\n```\n\n"
            "Only include files that actually need changes. "
            "Preserve all existing logic — only make the minimal change requested."
        )
        user = f"Instruction: {instruction}\n\nFiles:\n\n{files_block}"

        console.print("[dim]  Calling Claude to generate changes...[/]")
        with Timer() as t:
            resp = self._claude.messages.create(
                model=_MODEL,
                max_tokens=4096,
                system=system,
                messages=[{"role": "user", "content": user}],
            )
        output = resp.content[0].text

        self._metrics.record(
            beat_id="change",
            name=f"Change: {instruction[:40]}...",
            model=_MODEL,
            input_tokens=resp.usage.input_tokens,
            output_tokens=resp.usage.output_tokens,
            latency_ms=t.elapsed_ms,
            status="pass",
        )

        changes = _parse_changes(output, file_contents)
        return ChangeProposal(
            instruction=instruction,
            changes=changes,
            summary=f"{len(changes)} file(s) modified",
        )

    def apply_changes(self, proposal: ChangeProposal) -> int:
        """Write all proposed changes to disk. Returns number of files written."""
        count = 0
        for change in proposal.changes:
            full = self._repo_root / change.path
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_text(change.modified)
            count += 1
        return count


def _parse_changes(output: str, originals: dict[str, str]) -> list[FileChange]:
    """Parse ### CHANGE: <path> blocks from Claude output."""
    import re
    changes = []
    pattern = re.compile(
        r"### CHANGE: (.+?)\nDESCRIPTION: (.+?)\n```[^\n]*\n([\s\S]+?)```",
        re.MULTILINE,
    )
    for m in pattern.finditer(output):
        path = m.group(1).strip()
        desc = m.group(2).strip()
        new_content = m.group(3)
        changes.append(FileChange(
            path=path,
            original=originals.get(path, ""),
            modified=new_content,
            description=desc,
        ))
    return changes


def run_repl(agent: KnowledgeAgent):
    """Interactive REPL for audience Q&A and change proposals."""
    console.print(Panel(
        "[bold]AI-DLC Knowledge Agent[/]\n"
        "[dim]Ask anything about the codebase, specs, or Jira tickets.\n"
        "Prefix with [bold]change:[/] to propose a code modification.\n"
        "Type [bold]exit[/] to quit.[/]",
        title="[bold cyan]sml ask[/]",
        border_style="cyan",
    ))

    while True:
        try:
            user_input = console.input("\n[bold cyan]>[/] ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not user_input or user_input.lower() in ("exit", "quit", "q"):
            break

        if user_input.lower().startswith("change:"):
            instruction = user_input[7:].strip()
            _handle_change(agent, instruction)
        else:
            _handle_question(agent, user_input)

    console.print("\n[dim]Knowledge agent closed.[/]")


def _handle_question(agent: KnowledgeAgent, question: str):
    with console.status("[cyan]Searching + calling Claude...[/]"):
        try:
            result = agent.answer(question)
        except Exception as e:
            console.print(f"[red]Error: {e}[/]")
            return

    console.print(Panel(
        Markdown(result.answer),
        title="[bold green]Answer[/]",
        border_style="green",
    ))
    console.print(
        f"[dim]  Sources: {', '.join(s.source for s in result.sources[:3])} "
        f"· {result.input_tokens + result.output_tokens} tokens "
        f"· {result.latency_ms}ms[/]"
    )


def _handle_change(agent: KnowledgeAgent, instruction: str):
    with console.status("[cyan]Analysing affected files...[/]"):
        try:
            proposal = agent.propose_change(instruction)
        except Exception as e:
            console.print(f"[red]Error: {e}[/]")
            return

    if not proposal or not proposal.changes:
        console.print("[yellow]  No changes proposed.[/]")
        return

    console.print(f"\n[bold]Proposed changes ({len(proposal.changes)} file(s)):[/]")
    for c in proposal.changes:
        console.print(f"  [cyan]├──[/] {c.path}  [dim]— {c.description}[/]")

    confirm = console.input("\n[bold]Apply all changes?[/] [y/n]: ").strip().lower()
    if confirm == "y":
        n = agent.apply_changes(proposal)
        console.print(f"[green]  ✓ {n} file(s) updated.[/]")
    else:
        console.print("[dim]  Changes discarded.[/]")
