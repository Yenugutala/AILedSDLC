# Skill: Retrieval-Augmented Generation (RAG)

## Overview
Grounding LLM responses in external knowledge by retrieving relevant documents at query time.

## Key Patterns

### Basic RAG Pipeline
```
User Query → Embed Query → Vector Search → Retrieve Chunks → Augment Prompt → LLM → Response
```

### Chunking Strategies
| Strategy | Best For |
|---|---|
| Fixed-size (512 tokens, 20% overlap) | General text, articles |
| Semantic chunking | Documents with clear topic boundaries |
| Sentence-level | Short precise answers |
| Hierarchical (parent-child) | Long docs needing both detail and context |
| Page-level | PDFs, slide decks |

### Embedding Models
- **OpenAI text-embedding-3-large** — high quality, paid API
- **Cohere embed-v3** — multilingual, reranking support
- **BGE-M3** — open-source, strong multilingual performance
- Match embedding model between indexing and query time

### Retrieval Methods
- **Dense retrieval** — vector similarity (cosine, dot product)
- **Sparse retrieval** — BM25 keyword matching
- **Hybrid** — combine dense + sparse with RRF (Reciprocal Rank Fusion)
- **Reranking** — pass top-k candidates through a cross-encoder reranker for precision

### Advanced Patterns
- **HyDE (Hypothetical Document Embeddings)** — generate a hypothetical answer, embed it, retrieve similar docs
- **Multi-query retrieval** — generate multiple query variants; union results
- **Parent-child chunking** — retrieve small chunks, return surrounding parent for context
- **GraphRAG** — use knowledge graph traversal alongside vector search

## Best Practices
- Evaluate retrieval quality separately from generation quality
- Use metadata filters to scope retrieval (date, source, document type)
- Add source attribution to every generated response
- Cache embeddings for static documents
- Monitor retrieval hit rate and answer faithfulness in production

## Common Pitfalls
- Chunk boundaries splitting important context mid-sentence
- Not cleaning documents before chunking (headers, footers, boilerplate)
- Embedding model mismatch between index and query time
- Hallucination despite retrieval — LLM ignores retrieved context

## Tools
- **LangChain / LlamaIndex** — RAG orchestration frameworks
- **Chroma / Qdrant / Weaviate** — vector databases
- **pgvector** — Postgres extension for vector search
- **Cohere Rerank** — cross-encoder reranking API
- **RAGAS** — RAG evaluation framework
