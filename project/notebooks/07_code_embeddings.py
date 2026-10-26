# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Code Embeddings
# MAGIC GitHub repo → structure-aware chunks (Python via `ast`, SQL via `sqlglot`) → OpenRouter embeddings → Delta.
# MAGIC Only new or changed chunks are embedded. Tables: `<catalog>.<schema>.code_chunks`, `<catalog>.<schema>.code_embeddings`.

# COMMAND ----------
# MAGIC %pip install -q sqlglot

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("github_repo", "Yenugutala/AILedSDLC")
dbutils.widgets.text("git_ref", "securities-master-dev")
dbutils.widgets.text("include", "project/**/*.py,project/**/*.sql")
dbutils.widgets.text("catalog", "statestreet")
dbutils.widgets.text("schema", "m_statestreet")
dbutils.widgets.text("embedding_model", "openai/text-embedding-3-large")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("max_chars", "2000")
dbutils.widgets.text("batch_size", "64")

# Paste the key here only in the workspace copy — never commit it (the repo is public).
# Or leave the placeholder and set secret_scope: keys "openrouter_api_key" (and optional "github_token").
OPENROUTER_API_KEY = "<PASTE_OPENROUTER_API_KEY>"
GITHUB_TOKEN = ""

W = {k: dbutils.widgets.get(k).strip() for k in
     ("github_repo", "git_ref", "include", "catalog", "schema", "embedding_model", "secret_scope", "max_chars", "batch_size")}


def secret(key):
    try:
        return dbutils.secrets.get(W["secret_scope"], key) if W["secret_scope"] else ""
    except Exception:
        return ""


OPENROUTER_API_KEY = secret("openrouter_api_key") or OPENROUTER_API_KEY
GITHUB_TOKEN = secret("github_token") or GITHUB_TOKEN

REPO, REF, MODEL = W["github_repo"], W["git_ref"], W["embedding_model"]
MAX_CHARS, BATCH = int(W["max_chars"]), int(W["batch_size"])
HARD_MAX = MAX_CHARS * 6
T_CHUNKS = f"{W['catalog']}.{W['schema']}.code_chunks"
T_EMB = f"{W['catalog']}.{W['schema']}.code_embeddings"
INCLUDE = [p.strip() for p in W["include"].split(",") if p.strip()]

# COMMAND ----------
# MAGIC %md ## 1. Fetch files from GitHub at one commit

# COMMAND ----------
import fnmatch, io, tarfile, requests

gh = {"Accept": "application/vnd.github+json", **({"Authorization": f"Bearer {GITHUB_TOKEN}"} if GITHUB_TOKEN else {})}
head = requests.get(f"https://api.github.com/repos/{REPO}/commits/{REF}", headers=gh, timeout=60)
head.raise_for_status()
COMMIT = head.json()["sha"]
tar = requests.get(f"https://api.github.com/repos/{REPO}/tarball/{COMMIT}", headers=gh, timeout=300)
tar.raise_for_status()

FILES = {}
with tarfile.open(fileobj=io.BytesIO(tar.content), mode="r:gz") as tf:
    for m in tf.getmembers():
        path = m.name.split("/", 1)[-1]
        if m.isfile() and any(fnmatch.fnmatch(path, p) for p in INCLUDE):
            FILES[path] = tf.extractfile(m).read().decode("utf-8", errors="replace")
print(f"{REPO}@{REF} ({COMMIT[:10]}): {len(FILES)} files")

# COMMAND ----------
# MAGIC %md ## 2. Structure-aware chunking
# MAGIC - Databricks notebooks split on `COMMAND` cells; `%sql` / `%python` / `%md` magic cells are unwrapped.
# MAGIC - Python: top-level functions, classes and statement groups (`ast`), never cut mid-definition.
# MAGIC - SQL: whole statements (`sqlglot` tokenizer, so `;` inside strings is safe); small statements are grouped.
# MAGIC - Markdown headings become the section label of the code that follows.

# COMMAND ----------
import ast, hashlib, logging, re
import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

logging.getLogger("sqlglot").setLevel(logging.ERROR)
SEP = re.compile(r"^(#|--) COMMAND -+\s*$")
MAGIC_LANG = {"%sql": "sql", "%python": "python", "%md": "markdown", "%sh": "shell", "%pip": "shell", "%run": "run"}
ADJACENT = re.compile(r"'((?:[^'\\]|\\.)*)'\s*\n\s*'")
THREE_PART = re.compile(r"\b[A-Za-z_]\w*\.[A-Za-z_]\w*\.[A-Za-z_]\w*\b")


def cells(path, text):
    """[(cell_no, language, first_line, text)] — a plain file is one cell."""
    ext = "sql" if path.endswith(".sql") else "python"
    lines = text.splitlines()
    if not lines or "Databricks notebook source" not in lines[0]:
        return [(0, ext, 1, text)]
    groups, cur = [], []
    for n, line in enumerate(lines[1:], 2):
        if SEP.match(line):
            groups.append(cur)
            cur = []
        else:
            cur.append((n, line))
    groups.append(cur)
    out = []
    for i, g in enumerate(groups):
        lang, prefix = ext, "# MAGIC" if ext == "python" else "-- MAGIC"
        if any(l.strip() for _, l in g) and all(l.lstrip().startswith(prefix) or not l.strip() for _, l in g):
            g = [(n, l.lstrip()[len(prefix):].removeprefix(" ")) for n, l in g]
            first = next(l.split()[0] for _, l in g if l.strip())
            if first in MAGIC_LANG:
                lang, g = MAGIC_LANG[first], [(n, l.removeprefix(first).lstrip() if l.startswith(first) else l) for n, l in g]
        while g and not g[0][1].strip():
            g = g[1:]
        while g and not g[-1][1].strip():
            g = g[:-1]
        if g:
            out.append((i, lang, g[0][0], "\n".join(l for _, l in g)))
    return out


def python_units(text):
    """[(line_offset, end_offset, text, kind, symbols)] — one unit per top-level node, comments kept with it."""
    lines = text.splitlines()
    try:
        nodes = ast.parse(text).body
    except SyntaxError:
        return [(0, len(lines), text, "python", [])]
    units, prev = [], 0
    for n in nodes:
        end = n.end_lineno
        kind = {ast.FunctionDef: "function", ast.AsyncFunctionDef: "function", ast.ClassDef: "class"}.get(type(n), "statements")
        units.append((prev, end, "\n".join(lines[prev:end]), kind, [n.name] if kind != "statements" else []))
        prev = end
    if prev < len(lines) and "\n".join(lines[prev:]).strip():
        units.append((prev, len(lines), "\n".join(lines[prev:]), "statements", []))
    return units


def sql_units(text):
    """[(line_offset, end_offset, text, kind, symbols)] — one unit per statement."""
    try:
        cuts = [t.end + 1 for t in sqlglot.tokenize(text, read="databricks") if t.token_type == TokenType.SEMICOLON]
    except Exception:
        cuts = []
    units, start = [], 0
    for end in cuts + [len(text)]:
        part = text[start:end]
        if part.strip(" \n\t;"):
            off = text[:start + len(part) - len(part.lstrip())].count("\n")
            units.append((off, off + part.strip().count("\n") + 1, part.strip(), "sql", []))
        start = end
    return units


def sql_tables(sql):
    """(writes, reads) for one SQL text; falls back to 3-part names when it can't be parsed."""
    writes, reads = set(), set()
    try:
        trees = sqlglot.parse(ADJACENT.sub("'\\1", sql), read="databricks")
    except Exception:
        return writes, set(THREE_PART.findall(sql))
    for e in filter(None, trees):
        target = e.this.this if isinstance(e.this, exp.Schema) else e.this
        is_write = isinstance(e, (exp.Merge, exp.Insert, exp.Update, exp.Delete)) or (
            isinstance(e, exp.Create) and str(e.args.get("kind", "")).upper() in ("TABLE", "VIEW"))
        if is_write and isinstance(target, exp.Table):
            writes.add(".".join(p.name for p in target.parts).lower())
        ctes = {c.alias.lower() for c in e.find_all(exp.CTE)}
        for t in e.find_all(exp.Table):
            name = ".".join(p.name for p in t.parts).lower()
            if t is not target and name not in ctes and name not in writes:
                reads.add(name)
        if isinstance(e, exp.Command):
            reads |= set(THREE_PART.findall(e.sql()))
    return writes, reads


def python_tables(code):
    """Tables named in literal strings: spark.sql("...") bodies and 3-part names in table()/saveAsTable()."""
    writes, reads = set(), set()
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return writes, reads
    for c in (n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)):
        arg = c.args[0] if c.args and isinstance(c.args[0], ast.Constant) and isinstance(c.args[0].value, str) else None
        if arg is None:
            continue
        if c.func.attr == "sql":
            w, r = sql_tables(arg.value)
            writes, reads = writes | w, reads | r
        elif c.func.attr in ("saveAsTable", "insertInto", "toTable"):
            writes.add(arg.value.lower())
        elif c.func.attr == "table":
            reads.add(arg.value.lower())
    return writes, reads


def group(units, max_chars):
    """Merge consecutive small units up to max_chars; never split a unit unless it exceeds HARD_MAX."""
    out, cur = [], []
    for u in units:
        if len(u[2]) > HARD_MAX:
            out += [cur] if cur else []
            lines = u[2].splitlines()
            step = max(1, len(lines) * MAX_CHARS // len(u[2]))
            out += [[(u[0] + i, u[0] + min(i + step, len(lines)), "\n".join(lines[i:i + step]), u[3], u[4])]
                    for i in range(0, len(lines), step)]
            cur = []
        elif cur and sum(len(x[2]) for x in cur) + len(u[2]) > max_chars:
            out.append(cur)
            cur = [u]
        else:
            cur.append(u)
    return out + ([cur] if cur else [])


def chunk_file(path, text):
    rows, section = [], ""
    for cell_no, lang, first_line, body in cells(path, text):
        if lang == "markdown":
            section = next((l.lstrip("# ").strip() for l in body.splitlines() if l.strip()), section)
            if len(body) < 300:
                continue
            units = [(0, body.count("\n") + 1, body, "markdown", [])]
        elif lang == "python":
            units = python_units(body)
        elif lang == "sql":
            units = sql_units(body)
        else:
            continue
        for part, g in enumerate(group(units, MAX_CHARS)):
            chunk = "\n\n".join(u[2] for u in g).strip()
            extract = {"sql": sql_tables, "python": python_tables}.get(lang)
            writes, reads = extract(chunk) if extract else (set(), set())
            symbols = [s for u in g for s in u[4]]
            kinds = sorted({u[3] for u in g})
            start, end = first_line + g[0][0], first_line + g[-1][1] - 1
            header = "\n".join(x for x in [
                f"File: {path} (lines {start}-{end})",
                f"Section: {section}" if section else "",
                f"Language: {lang} · {', '.join(kinds)}",
                f"Defines: {', '.join(symbols)}" if symbols else "",
                f"Writes: {', '.join(sorted(writes))}" if writes else "",
                f"Reads: {', '.join(sorted(reads))}" if reads else ""] if x)
            rows.append(dict(
                chunk_id=f"{path}#c{cell_no}#p{part}", repo=REPO, ref=REF, commit_sha=COMMIT, path=path,
                language=lang, cell=cell_no, part=part, start_line=start, end_line=end, section=section,
                kinds=kinds, symbols=symbols, tables_written=sorted(writes), tables_read=sorted(reads - writes),
                header=header, text=chunk, content_hash=hashlib.sha256((header + "\n\n" + chunk).encode()).hexdigest()))
    return rows


CHUNKS = [r for path in sorted(FILES) for r in chunk_file(path, FILES[path])]
print(f"{len(CHUNKS)} chunks from {len(FILES)} files; largest {max(len(c['text']) for c in CHUNKS)} chars")

# COMMAND ----------
# MAGIC %md ## 3. Store chunks in Delta (MERGE; chunks removed from the repo are deleted)

# COMMAND ----------
from pyspark.sql import types as T

UNIFORM = "TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')"
STR, INT, ARR = T.StringType(), T.IntegerType(), T.ArrayType(T.StringType())
CHUNK_SCHEMA = T.StructType([T.StructField(n, t) for n, t in [
    ("chunk_id", STR), ("repo", STR), ("ref", STR), ("commit_sha", STR), ("path", STR), ("language", STR),
    ("cell", INT), ("part", INT), ("start_line", INT), ("end_line", INT), ("section", STR), ("kinds", ARR),
    ("symbols", ARR), ("tables_written", ARR), ("tables_read", ARR), ("header", STR), ("text", STR),
    ("content_hash", STR)]])

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {W['catalog']}.{W['schema']}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {T_CHUNKS} ({CHUNK_SCHEMA.toDDL()}, _ingestion_ts TIMESTAMP) {UNIFORM}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_EMB} (chunk_id STRING, repo STRING, content_hash STRING, model STRING,
              dims INT, embedding ARRAY<FLOAT>, embedded_at TIMESTAMP) {UNIFORM}""")

spark.createDataFrame(CHUNKS, CHUNK_SCHEMA).createOrReplaceTempView("incoming_chunks")
merged = spark.sql(f"""
    MERGE INTO {T_CHUNKS} t
    USING (SELECT *, current_timestamp() AS _ingestion_ts FROM incoming_chunks) s ON t.chunk_id = s.chunk_id
    WHEN MATCHED AND (t.content_hash <> s.content_hash OR t.commit_sha <> s.commit_sha) THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND t.repo = :repo THEN DELETE""", args={"repo": REPO}).first().asDict()
print("code_chunks:", merged)

# COMMAND ----------
# MAGIC %md ## 4. Embed new / changed chunks with OpenRouter and store in Delta

# COMMAND ----------
import time
from datetime import datetime, timezone

if OPENROUTER_API_KEY.startswith("<"):
    raise ValueError("Set OPENROUTER_API_KEY in the configuration cell (or a secret_scope) before embedding.")

todo = spark.sql(f"""
    SELECT c.chunk_id, c.content_hash, concat(c.header, '\n\n', c.text) AS input
    FROM {T_CHUNKS} c LEFT ANTI JOIN {T_EMB} e
      ON e.chunk_id = c.chunk_id AND e.content_hash = c.content_hash AND e.model = :model
    WHERE c.repo = :repo""", args={"model": MODEL, "repo": REPO}).collect()
print(f"{len(todo)} chunks need embeddings ({MODEL})")


def embed(texts):
    for attempt in range(6):
        r = requests.post("https://openrouter.ai/api/v1/embeddings", timeout=180,
                          headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
                          json={"model": MODEL, "input": texts})
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        data = sorted(r.json()["data"], key=lambda d: d.get("index", 0))
        if len(data) != len(texts):
            raise RuntimeError(f"expected {len(texts)} embeddings, got {len(data)}")
        return [d["embedding"] for d in data]
    raise RuntimeError(f"OpenRouter kept failing: {r.status_code} {r.text[:300]}")


EMB_SCHEMA = "chunk_id STRING, repo STRING, content_hash STRING, model STRING, dims INT, embedding ARRAY<FLOAT>, embedded_at TIMESTAMP"


def flush(rows):
    if not rows:
        return
    spark.createDataFrame(rows, EMB_SCHEMA).createOrReplaceTempView("incoming_embeddings")
    spark.sql(f"""MERGE INTO {T_EMB} t USING incoming_embeddings s ON t.chunk_id = s.chunk_id AND t.model = s.model
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


pending, done = [], 0
for i in range(0, len(todo), BATCH):
    batch = todo[i:i + BATCH]
    now = datetime.now(timezone.utc)
    for row, vec in zip(batch, embed([r["input"] for r in batch])):
        pending.append((row["chunk_id"], REPO, row["content_hash"], MODEL, len(vec), [float(x) for x in vec], now))
    if len(pending) >= BATCH * 5:
        flush(pending)
        done, pending = done + len(pending), []
flush(pending)
done += len(pending)

spark.sql(f"DELETE FROM {T_EMB} WHERE repo = :repo AND chunk_id NOT IN (SELECT chunk_id FROM {T_CHUNKS} WHERE repo = :repo)",
          args={"repo": REPO})
print(f"embedded {done} chunks")

# COMMAND ----------
# MAGIC %md ## 5. Summary

# COMMAND ----------
import json

summary = spark.sql(f"""
    SELECT count(*) AS chunks, count(e.chunk_id) AS embedded, count(DISTINCT c.path) AS files, max(e.dims) AS dims
    FROM {T_CHUNKS} c LEFT JOIN {T_EMB} e ON e.chunk_id = c.chunk_id AND e.content_hash = c.content_hash AND e.model = :model
    WHERE c.repo = :repo""", args={"model": MODEL, "repo": REPO}).first().asDict()
summary.update(commit=COMMIT, newly_embedded=done)
print(summary)
dbutils.notebook.exit(json.dumps(summary, default=str))
